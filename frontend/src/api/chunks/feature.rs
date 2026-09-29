// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Contract v2.1 polygon parsing and HEALPix-NESTED feature resolution.
//!
//! `cdshealpix::nested::polygon_coverage(..., false)` supplies candidate cells.
//! That API approximates HEALPix cell edges by great-circle arcs; at nside 1024
//! its candidate-boundary uncertainty is below one cell width (about 3.4 km).
//! We then test every candidate cell's centre with
//! `cdshealpix::sph_geom::Polygon::contains`, so inclusion is centre-in-polygon
//! rather than BMOC full-or-partial overlap. Polygon edges are great-circle arcs,
//! including across the longitude seam and near the poles.

use std::f64::consts::PI;

use cdshealpix::nested;
use cdshealpix::sph_geom::{
    coo3d::{Coo3D, LonLat},
    Polygon,
};
use serde::Serialize;
use serde_json::Value;

const MIN_VERTICES: usize = 3;
const MAX_VERTICES: usize = 4096;

/// A validated polygon with canonical longitudes in `[0, 360)` degrees.
#[derive(Debug, Clone, PartialEq)]
pub struct PolygonRequest {
    vertices_deg: Vec<[f64; 2]>,
}

impl PolygonRequest {
    fn vertices_rad(&self) -> Vec<(f64, f64)> {
        self.vertices_deg
            .iter()
            .map(|[lon, lat]| (lon.to_radians(), lat.to_radians()))
            .collect()
    }

    #[cfg(test)]
    fn vertices_deg(&self) -> &[[f64; 2]] {
        &self.vertices_deg
    }
}

/// The additive per-array-set Contract v2.1 feature block.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ResolvedFeature {
    #[serde(rename = "type")]
    pub feature_type: &'static str,
    pub n_points: u64,
    pub ranges: Vec<[u64; 2]>,
    pub coords: FeatureCoords,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct FeatureCoords {
    pub lat: Vec<f64>,
    pub lon: Vec<f64>,
}

/// Parse an optional top-level metadata `feature`.
pub fn parse_feature(value: Option<&Value>) -> Result<Option<PolygonRequest>, String> {
    value.map(parse_polygon).transpose()
}

fn parse_polygon(value: &Value) -> Result<PolygonRequest, String> {
    let object = value.as_object().ok_or("'feature' must be a JSON object")?;
    match object.get("type") {
        Some(Value::String(feature_type)) if feature_type == "polygon" => {}
        Some(Value::String(feature_type)) => {
            return Err(format!(
                "unsupported feature type '{feature_type}'; v2.1 supports only 'polygon'"
            ));
        }
        Some(_) => return Err("feature.type must be the string 'polygon'".to_string()),
        None => return Err("feature must contain type 'polygon'".to_string()),
    }

    let shape = object
        .get("shape")
        .ok_or("polygon feature must contain a 'shape'")?
        .as_array()
        .ok_or("feature.shape must be an array of [lon, lat] vertices")?;
    if !(MIN_VERTICES..=MAX_VERTICES).contains(&shape.len()) {
        return Err(format!(
            "feature.shape must contain {MIN_VERTICES}..={MAX_VERTICES} vertices (got {})",
            shape.len()
        ));
    }

    let mut vertices = Vec::with_capacity(shape.len());
    for (index, vertex) in shape.iter().enumerate() {
        let pair = vertex
            .as_array()
            .filter(|pair| pair.len() == 2)
            .ok_or_else(|| format!("feature.shape[{index}] must be [lon, lat]"))?;
        let lon = pair[0]
            .as_f64()
            .filter(|value| value.is_finite())
            .ok_or_else(|| format!("feature.shape[{index}][0] longitude must be a number"))?;
        let lat = pair[1]
            .as_f64()
            .filter(|value| value.is_finite())
            .ok_or_else(|| format!("feature.shape[{index}][1] latitude must be a number"))?;
        if !(-180.0..=360.0).contains(&lon) {
            return Err(format!(
                "feature.shape[{index}][0] longitude must be in -180..=360 degrees"
            ));
        }
        if !(-90.0..=90.0).contains(&lat) {
            return Err(format!(
                "feature.shape[{index}][1] latitude must be in -90..=90 degrees"
            ));
        }
        vertices.push([normalise_lon(lon), clean_zero(lat)]);
    }

    // The contract accepts open or explicitly closed rings. cdshealpix closes
    // polygons itself, so remove a repeated final vertex before constructing it.
    if vertices.len() > MIN_VERTICES && vertices.first() == vertices.last() {
        vertices.pop();
    }
    if vertices.len() < MIN_VERTICES {
        return Err("feature.shape must contain at least 3 distinct ring vertices".to_string());
    }
    for index in 0..vertices.len() {
        if vertices[index] == vertices[(index + 1) % vertices.len()] {
            return Err(format!(
                "feature.shape has a zero-length edge at vertex {index}"
            ));
        }
    }

    Ok(PolygonRequest {
        vertices_deg: vertices,
    })
}

fn normalise_lon(lon: f64) -> f64 {
    clean_zero(lon.rem_euclid(360.0))
}

fn clean_zero(value: f64) -> f64 {
    if value == 0.0 {
        0.0
    } else {
        value
    }
}

/// Resolve the polygon to sorted, disjoint half-open NESTED ranges and centres.
pub fn resolve_polygon(
    polygon: &PolygonRequest,
    nside: u32,
    max_feature_points: u64,
) -> Result<ResolvedFeature, String> {
    if !cdshealpix::is_nside(nside) {
        return Err(format!(
            "HEALPix nside {nside} is invalid; it must be a non-zero power of two"
        ));
    }
    let depth = cdshealpix::depth(nside);
    let vertices = polygon.vertices_rad();
    let geometry = Polygon::new(
        vertices
            .iter()
            .map(|&(lon, lat)| LonLat { lon, lat })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    );

    // The approximate mode avoids cdshealpix's expensive special-point path
    // (and a known polar-edge panic in 0.9.1). It affects only candidate cell
    // edges; centre filtering below defines the Contract v2.1 inclusion.
    let coverage = nested::polygon_coverage(depth, &vertices, false);
    let layer = nested::get(depth);
    let selected = || {
        coverage.flat_iter().filter(|&hash| {
            let (lon, lat) = layer.center(hash);
            geometry.contains(&Coo3D::from_sph_coo(lon, lat))
        })
    };

    let mut ranges = Vec::new();
    let mut range_start = None;
    let mut previous = 0_u64;
    let mut n_points = 0_u64;
    for hash in selected() {
        n_points = n_points
            .checked_add(1)
            .ok_or("polygon selected too many HEALPix cells")?;
        match range_start {
            None => range_start = Some(hash),
            Some(_) if hash != previous + 1 => {
                ranges.push([range_start.take().unwrap(), previous + 1]);
                range_start = Some(hash);
            }
            Some(_) => {}
        }
        previous = hash;
    }
    if let Some(start) = range_start {
        ranges.push([start, previous + 1]);
    }

    if n_points > max_feature_points {
        return Err(format!(
            "polygon selects {n_points} points, exceeding chunks.max_feature_points={max_feature_points}"
        ));
    }

    let capacity = usize::try_from(n_points)
        .map_err(|_| format!("polygon selects {n_points} points, too many for this server"))?;
    let mut lat = Vec::with_capacity(capacity);
    let mut lon = Vec::with_capacity(capacity);
    for hash in selected() {
        let (cell_lon, cell_lat) = layer.center(hash);
        lat.push(clean_zero(cell_lat * 180.0 / PI));
        lon.push(clean_zero(cell_lon * 180.0 / PI));
    }

    Ok(ResolvedFeature {
        feature_type: "polygon",
        n_points,
        ranges,
        coords: FeatureCoords { lat, lon },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn polygon(shape: Value) -> PolygonRequest {
        parse_feature(Some(&json!({"type": "polygon", "shape": shape})))
            .unwrap()
            .unwrap()
    }

    #[test]
    fn polygon_parsing_validates_and_normalises_open_or_closed_rings() {
        let open = polygon(json!([[-10, 1], [360, 2], [10, 3]]));
        assert_eq!(
            open.vertices_deg(),
            &[[350.0, 1.0], [0.0, 2.0], [10.0, 3.0]]
        );

        let closed = polygon(json!([[-10, 1], [0, 2], [10, 3], [350, 1]]));
        assert_eq!(closed.vertices_deg(), open.vertices_deg());

        for invalid in [
            json!(null),
            json!({"type": "bbox", "shape": [[0, 0], [1, 0], [0, 1]]}),
            json!({"type": "polygon", "shape": [[0, 0], [1, 0]]}),
            json!({"type": "polygon", "shape": [[0, 0], [1, 0], [2]]}),
            json!({"type": "polygon", "shape": [[0, 0], [361, 0], [0, 1]]}),
            json!({"type": "polygon", "shape": [[0, 0], [1, 91], [0, 1]]}),
            json!({"type": "polygon", "shape": [[0, 0], [0, 0], [1, 1]]}),
        ] {
            assert!(parse_feature(Some(&invalid)).is_err(), "accepted {invalid}");
        }
        let too_many = json!({
            "type": "polygon",
            "shape": vec![json!([0, 0]); MAX_VERTICES + 1],
        });
        assert!(parse_feature(Some(&too_many)).is_err());
        assert!(parse_feature(None).unwrap().is_none());
    }

    #[test]
    fn dateline_and_polar_polygons_have_canonical_longitudes() {
        // 179E -> 179W crosses the dateline by the short great-circle arc.
        let dateline = polygon(json!([[179, -5], [-179, -5], [-179, 5], [179, 5]]));
        assert_eq!(
            dateline.vertices_deg(),
            &[[179.0, -5.0], [181.0, -5.0], [181.0, 5.0], [179.0, 5.0]]
        );

        // cdshealpix's spherical polygon interpretation handles this ring near
        // the north pole; longitude is irrelevant at the pole itself.
        let polar = polygon(json!([[0, 85], [120, 85], [240, 85]]));
        assert_eq!(polar.vertices_deg()[2], [240.0, 85.0]);
        let result = resolve_polygon(&polar, 16, 3_072).unwrap();
        assert!(result.n_points > 0);
    }

    #[test]
    fn tiny_nside_one_polygon_selects_the_hand_checkable_equatorial_cell() {
        let request = polygon(json!([[-20, -20], [20, -20], [20, 20], [-20, 20]]));
        let result = resolve_polygon(&request, 1, 12).unwrap();
        assert_eq!(result.n_points, 1);
        assert_eq!(result.ranges, vec![[4, 5]]);
        assert_eq!(result.coords.lat, vec![0.0]);
        assert_eq!(result.coords.lon, vec![0.0]);
    }

    #[test]
    fn nested_indices_match_healpy_reference_centres() {
        // Standard HEALPix NESTED reference values, also returned by
        // healpy.pix2ang(..., nest=True, lonlat=True). At nside=1 the north
        // base cell 0 is (45 deg, asin(2/3)); equatorial cell 4 is (0, 0).
        // At nside=2, nested child 3 of base cell 0 is centred at
        // (45 deg, 66.4435356909 deg). These hard-coded checks guard the most
        // dangerous possible failure here: silently using RING index order.
        let checks = [
            (0_u8, 0_u64, 45.0, 41.810_314_895_778_596),
            (0, 4, 0.0, 0.0),
            (0, 8, 45.0, -41.810_314_895_778_596),
            (1, 3, 45.0, 66.443_535_690_898_76),
        ];
        for (depth, hash, expected_lon, expected_lat) in checks {
            let (lon, lat) = nested::center(depth, hash);
            assert!((lon.to_degrees() - expected_lon).abs() < 1.0e-12);
            assert!((lat.to_degrees() - expected_lat).abs() < 1.0e-12);
            assert_eq!(nested::hash(depth, lon, lat), hash);
        }
    }

    #[test]
    fn nside_1024_polygon_is_deterministic_and_ranges_are_canonical() {
        let request = polygon(json!([[-1, 51], [1, 51], [1, 52], [-1, 52]]));
        let first = resolve_polygon(&request, 1024, 1_000_000).unwrap();
        let second = resolve_polygon(&request, 1024, 1_000_000).unwrap();
        assert_eq!(first, second);
        // Exact count is intentionally pinned: a cdshealpix convention or
        // centre-inclusion regression must not silently change the region.
        assert_eq!(first.n_points, 378);
        assert_eq!(first.ranges.first(), Some(&[716_767, 716_768]));
        assert_eq!(first.ranges.last(), Some(&[3_536_744, 3_536_745]));
        assert_eq!(
            first.ranges.iter().map(|[lo, hi]| hi - lo).sum::<u64>(),
            first.n_points
        );
        assert_eq!(first.coords.lat.len() as u64, first.n_points);
        assert_eq!(first.coords.lon.len() as u64, first.n_points);
        assert!(first.ranges.iter().all(|[lo, hi]| lo < hi));
        assert!(first.ranges.windows(2).all(|pair| pair[0][1] < pair[1][0]));
    }

    #[test]
    fn over_cap_error_reports_the_exact_selected_count() {
        let request = polygon(json!([[-20, -20], [20, -20], [20, 20], [-20, 20]]));
        let error = resolve_polygon(&request, 1, 0).unwrap_err();
        assert!(error.contains("selects 1 points"), "{error}");
        assert!(error.contains("max_feature_points=0"), "{error}");
    }
}

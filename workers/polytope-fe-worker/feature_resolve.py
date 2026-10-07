"""Resolve a polytope-mars spatial feature to field-index ranges without reading values."""

import copy
import itertools
import json
import math
import time


class FeatureResolveError(ValueError):
    pass


def is_feature_resolve_request(request):
    return isinstance(request, dict) and isinstance(request.get("feature_resolve"), dict)


def _positive_int(value, name):
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise FeatureResolveError(f"feature_resolve.{name} must be a positive integer")
    return value


def _setup_feature(polytope_mars, request):
    feature_config = request.pop("feature", None)
    if not isinstance(feature_config, dict):
        raise FeatureResolveError("feature-resolve request must contain a feature object")
    feature_copy = copy.deepcopy(feature_config)
    feature_type = feature_config.get("type")
    if not isinstance(feature_type, str):
        raise FeatureResolveError("feature.type must be a string")
    feature = polytope_mars._feature_factory(feature_type, feature_config, polytope_mars.conf)
    feature.validate(request, feature_copy)
    request = feature.parse(request, feature_copy)
    shapes = polytope_mars._create_base_shapes(request, feature_type)
    shapes.extend(feature.get_shapes())
    return feature_type, shapes


def _slice_without_values(api, polytope_request):
    """Mirror Polytope.retrieve through slicing, stopping before datacube.get()."""
    from polytope_feature.shapes import Point  # type: ignore[import-not-found]

    api.datacube.check_branching_axes(polytope_request)
    api.switch_polytope_dim(polytope_request)
    for polytope in polytope_request.polytopes():
        if polytope.method != "nearest":
            continue
        k = polytope.k
        key = tuple(polytope.axes())
        values = polytope.values if polytope.is_flat else polytope.points
        existing = api.datacube.nearest_search.get(key)
        if existing is None:
            api.datacube.nearest_search[key] = (values, k)
        elif isinstance(polytope, Point):
            existing[0].append(values[0])
    return api.slice(api.datacube, polytope_request.polytopes())


def _node_index_candidates(leaf):
    candidates = []
    node = leaf
    while node is not None:
        for value in getattr(node, "indexes", []):
            if isinstance(value, (list, tuple)):
                candidates.extend(value)
            else:
                candidates.append(value)
        node = getattr(node, "parent", None)
    return candidates


def _point_rows(tree, datacube):
    mapper = datacube.grid_transformation
    mapped_axes = list(mapper._mapped_axes())
    if "latitude" not in mapped_axes or "longitude" not in mapped_axes:
        raise FeatureResolveError("selected grid mapper does not expose latitude/longitude axes")

    rows = []
    for leaf in tree.leaves:
        path = leaf.flatten()
        latitudes = path.get("latitude", ())
        longitudes = path.get("longitude", ())
        if not latitudes or not longitudes:
            continue
        candidates = _node_index_candidates(leaf)
        candidate_offset = 0
        for latitude, longitude in itertools.product(latitudes, longitudes):
            candidate = candidates[candidate_offset:] or None
            raw = mapper.unmap([latitude], [longitude], candidate)
            if isinstance(raw, (list, tuple)):
                indexes = list(raw)
            elif hasattr(raw, "tolist"):
                converted = raw.tolist()
                indexes = converted if isinstance(converted, list) else [converted]
            else:
                indexes = [raw]
            if len(indexes) != 1:
                raise FeatureResolveError("polytope grid mapper returned an ambiguous field index")
            try:
                index = int(indexes[0])
                lat = float(latitude)
                lon = float(longitude)
            except (TypeError, ValueError, OverflowError) as exc:
                raise FeatureResolveError(
                    "polytope grid mapper returned an invalid index or coordinate"
                ) from exc
            rows.append((index, lat, lon))
            candidate_offset += 1
    return rows


def _ranges_from_indices(indices):
    ranges = []
    for index in indices:
        if ranges and index == ranges[-1][1]:
            ranges[-1][1] += 1
        else:
            ranges.append([index, index + 1])
    return ranges


def normalise_result(feature_type, rows, count_values, max_points, computed_ranges=None):
    """Sort/dedupe points by field index and produce the chunks feature block."""
    by_index = {}
    for index, latitude, longitude in rows:
        if index < 0 or index >= count_values:
            raise FeatureResolveError(
                f"polytope selected field index {index} outside grid size {count_values}"
            )
        if not math.isfinite(latitude) or not math.isfinite(longitude):
            raise FeatureResolveError("polytope selected a non-finite coordinate")
        by_index.setdefault(index, (latitude, longitude))
    ordered = sorted(by_index.items())
    if not ordered:
        raise FeatureResolveError(f"{feature_type} feature selects no grid points")
    if len(ordered) > max_points:
        raise FeatureResolveError(
            f"{feature_type} selects {len(ordered)} points, exceeding "
            f"chunks.max_feature_points={max_points}"
        )
    indices = [index for index, _ in ordered]
    ranges = _ranges_from_indices(indices)
    if computed_ranges is not None:
        try:
            computed = sorted(
                {index for lo, hi in computed_ranges for index in range(int(lo), int(hi))}
            )
        except (TypeError, ValueError, OverflowError) as exc:
            raise FeatureResolveError("polytope produced invalid GribJump ranges") from exc
        if computed != indices:
            raise FeatureResolveError(
                "polytope coordinate indices differ from its computed GribJump ranges"
            )
    return {
        "type": feature_type,
        "n_points": len(ordered),
        "ranges": ranges,
        "coords": {
            "lat": [coords[0] for _, coords in ordered],
            "lon": [coords[1] for _, coords in ordered],
        },
    }


def resolve_feature(datasource, request, *, user=None, job_id=None):
    """Use polytope-mars' exact config and polytope_feature slicing path."""
    from polytope_feature.polytope import (  # type: ignore[import-not-found]
        Polytope,
        Request,
    )
    from polytope_mars.api import PolytopeMars  # type: ignore[import-not-found]
    import pygribjump  # type: ignore[import-not-found]

    started = time.monotonic()
    marker = request.coerced_request.get("feature_resolve")
    count_values = _positive_int(marker.get("count_values"), "count_values")
    max_points = _positive_int(marker.get("max_feature_points"), "max_feature_points")
    expected_hash = marker.get("grid_hash")
    if expected_hash is not None and not isinstance(expected_hash, str):
        raise FeatureResolveError("feature_resolve.grid_hash must be a string or null")

    prepared, config = datasource.prepare_request(request)
    prepared.pop("feature_resolve", None)
    polytope_mars = PolytopeMars(
        config,
        log_context={
            "user": f"{getattr(user, 'realm', '')}:{getattr(user, 'username', '')}",
            "id": job_id or "feature-resolve",
        },
    )
    feature_type, feature_shapes = _setup_feature(polytope_mars, prepared)
    api = Polytope(
        datacube=pygribjump.GribJump(),
        options=polytope_mars.conf.options.model_dump(),
        context=polytope_mars.log_context,
    )
    actual_hash = api.datacube.grid_md5_hash
    if expected_hash is not None and str(actual_hash).lower() != expected_hash.lower():
        raise FeatureResolveError(
            f"representative field grid hash mismatch: expected {expected_hash}, "
            f"polytope mapper uses {actual_hash}"
        )

    tree = _slice_without_values(api, Request(*feature_shapes))
    range_tree = copy.deepcopy(tree)
    fdb_requests = []
    decoding = []
    api.datacube.get_fdb_requests(range_tree, fdb_requests, decoding)
    if len(fdb_requests) != 1:
        raise FeatureResolveError(
            f"feature-resolve representative field produced {len(fdb_requests)} GribJump requests"
        )
    computed_ranges = fdb_requests[0][1]
    result = normalise_result(
        feature_type,
        _point_rows(range_tree, api.datacube),
        count_values,
        max_points,
        computed_ranges,
    )
    elapsed = round((time.monotonic() - started) * 1000, 1)
    return json.dumps(result, separators=(",", ":")).encode("utf-8"), elapsed

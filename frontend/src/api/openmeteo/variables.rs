#[derive(Clone)]
pub enum ParamKind {
    Direct {
        param: &'static str,
        levtype: &'static str,
    },
    WindSpeed {
        u_param: &'static str,
        v_param: &'static str,
        levtype: &'static str,
    },
    WindDirection {
        u_param: &'static str,
        v_param: &'static str,
        levtype: &'static str,
    },
    PressureLevel {
        param: &'static str,
    },
    PressureLevelWindSpeed,
    PressureLevelWindDirection,
}

#[derive(Clone)]
pub struct VariableInfo {
    pub kind: ParamKind,
    pub unit: &'static str,
    #[allow(dead_code)]
    pub is_accumulated: bool,
}

const VALID_PRESSURE_LEVELS: [u32; 13] = [
    1000, 925, 850, 700, 600, 500, 400, 300, 250, 200, 150, 100, 50,
];

pub fn lookup(name: &str) -> Option<(VariableInfo, Option<u32>)> {
    let surface = match name {
        "temperature_2m" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "2t",
                levtype: "sfc",
            },
            unit: "°C",
            is_accumulated: false,
        }),
        "dew_point_2m" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "2d",
                levtype: "sfc",
            },
            unit: "°C",
            is_accumulated: false,
        }),
        "pressure_msl" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "msl",
                levtype: "sfc",
            },
            unit: "hPa",
            is_accumulated: false,
        }),
        "surface_pressure" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "sp",
                levtype: "sfc",
            },
            unit: "hPa",
            is_accumulated: false,
        }),
        "cloud_cover" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "tcc",
                levtype: "sfc",
            },
            unit: "%",
            is_accumulated: false,
        }),
        "cloud_cover_low" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "lcc",
                levtype: "sfc",
            },
            unit: "%",
            is_accumulated: false,
        }),
        "cloud_cover_mid" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "mcc",
                levtype: "sfc",
            },
            unit: "%",
            is_accumulated: false,
        }),
        "cloud_cover_high" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "hcc",
                levtype: "sfc",
            },
            unit: "%",
            is_accumulated: false,
        }),
        "wind_speed_10m" => Some(VariableInfo {
            kind: ParamKind::WindSpeed {
                u_param: "10u",
                v_param: "10v",
                levtype: "sfc",
            },
            unit: "m/s",
            is_accumulated: false,
        }),
        "wind_direction_10m" => Some(VariableInfo {
            kind: ParamKind::WindDirection {
                u_param: "10u",
                v_param: "10v",
                levtype: "sfc",
            },
            unit: "°",
            is_accumulated: false,
        }),
        "wind_speed_100m" => Some(VariableInfo {
            kind: ParamKind::WindSpeed {
                u_param: "100u",
                v_param: "100v",
                levtype: "sfc",
            },
            unit: "m/s",
            is_accumulated: false,
        }),
        "wind_direction_100m" => Some(VariableInfo {
            kind: ParamKind::WindDirection {
                u_param: "100u",
                v_param: "100v",
                levtype: "sfc",
            },
            unit: "°",
            is_accumulated: false,
        }),
        "wind_gusts_10m" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "10fg",
                levtype: "sfc",
            },
            unit: "m/s",
            is_accumulated: false,
        }),
        "precipitation" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "tp",
                levtype: "sfc",
            },
            unit: "mm",
            is_accumulated: true,
        }),
        "snowfall_water_equivalent" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "sf",
                levtype: "sfc",
            },
            unit: "mm",
            is_accumulated: true,
        }),
        "snow_depth" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "sd",
                levtype: "sfc",
            },
            unit: "m",
            is_accumulated: false,
        }),
        "shortwave_radiation" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "ssrd",
                levtype: "sfc",
            },
            unit: "W/m²",
            is_accumulated: true,
        }),
        "direct_radiation" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "fdir",
                levtype: "sfc",
            },
            unit: "W/m²",
            is_accumulated: true,
        }),
        "cape" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "cape",
                levtype: "sfc",
            },
            unit: "J/kg",
            is_accumulated: false,
        }),
        "surface_temperature" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "skt",
                levtype: "sfc",
            },
            unit: "°C",
            is_accumulated: false,
        }),
        "soil_temperature_0_to_7cm" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "stl1",
                levtype: "sfc",
            },
            unit: "°C",
            is_accumulated: false,
        }),
        "soil_temperature_7_to_28cm" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "stl2",
                levtype: "sfc",
            },
            unit: "°C",
            is_accumulated: false,
        }),
        "soil_temperature_28_to_100cm" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "stl3",
                levtype: "sfc",
            },
            unit: "°C",
            is_accumulated: false,
        }),
        "soil_temperature_100_to_255cm" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "stl4",
                levtype: "sfc",
            },
            unit: "°C",
            is_accumulated: false,
        }),
        "soil_moisture_0_to_7cm" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "swvl1",
                levtype: "sfc",
            },
            unit: "m³/m³",
            is_accumulated: false,
        }),
        "soil_moisture_7_to_28cm" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "swvl2",
                levtype: "sfc",
            },
            unit: "m³/m³",
            is_accumulated: false,
        }),
        "soil_moisture_28_to_100cm" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "swvl3",
                levtype: "sfc",
            },
            unit: "m³/m³",
            is_accumulated: false,
        }),
        "soil_moisture_100_to_255cm" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "swvl4",
                levtype: "sfc",
            },
            unit: "m³/m³",
            is_accumulated: false,
        }),
        "visibility" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "vis",
                levtype: "sfc",
            },
            unit: "m",
            is_accumulated: false,
        }),
        "total_column_integrated_water_vapour" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "tcwv",
                levtype: "sfc",
            },
            unit: "kg/m²",
            is_accumulated: false,
        }),
        "boundary_layer_height" => Some(VariableInfo {
            kind: ParamKind::Direct {
                param: "blh",
                levtype: "sfc",
            },
            unit: "m",
            is_accumulated: false,
        }),
        _ => None,
    };

    if let Some(info) = surface {
        return Some((info, None));
    }

    parse_pressure_level_variable(name)
}

pub fn required_params(info: &VariableInfo, level: Option<u32>) -> Vec<(&str, &str, Option<u32>)> {
    match &info.kind {
        ParamKind::Direct { param, levtype } => vec![(*param, *levtype, level)],
        ParamKind::WindSpeed {
            u_param,
            v_param,
            levtype,
        }
        | ParamKind::WindDirection {
            u_param,
            v_param,
            levtype,
        } => vec![(*u_param, *levtype, level), (*v_param, *levtype, level)],
        ParamKind::PressureLevel { param } => vec![(*param, "pl", level)],
        ParamKind::PressureLevelWindSpeed | ParamKind::PressureLevelWindDirection => {
            vec![("u", "pl", level), ("v", "pl", level)]
        }
    }
}

fn parse_pressure_level_variable(name: &str) -> Option<(VariableInfo, Option<u32>)> {
    let suffix = "hPa";
    let (base, level_raw) = name.rsplit_once('_')?;
    if !level_raw.ends_with(suffix) {
        return None;
    }

    let level_str = level_raw.trim_end_matches(suffix);
    let level = level_str.parse::<u32>().ok()?;
    if !VALID_PRESSURE_LEVELS.contains(&level) {
        return None;
    }

    let info = match base {
        "temperature" => VariableInfo {
            kind: ParamKind::PressureLevel { param: "t" },
            unit: "°C",
            is_accumulated: false,
        },
        "geopotential_height" => VariableInfo {
            kind: ParamKind::PressureLevel { param: "z" },
            unit: "m",
            is_accumulated: false,
        },
        "relative_humidity" => VariableInfo {
            kind: ParamKind::PressureLevel { param: "r" },
            unit: "%",
            is_accumulated: false,
        },
        "wind_speed" => VariableInfo {
            kind: ParamKind::PressureLevelWindSpeed,
            unit: "m/s",
            is_accumulated: false,
        },
        "wind_direction" => VariableInfo {
            kind: ParamKind::PressureLevelWindDirection,
            unit: "°",
            is_accumulated: false,
        },
        "vertical_velocity" => VariableInfo {
            kind: ParamKind::PressureLevel { param: "w" },
            unit: "m/s",
            is_accumulated: false,
        },
        _ => return None,
    };

    Some((info, Some(level)))
}

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ForecastParams {
    pub latitude: f64,
    pub longitude: f64,
    #[serde(default)]
    pub hourly: Option<String>,
    #[serde(default)]
    pub daily: Option<String>,
    #[serde(default)]
    pub current: Option<String>,
    #[serde(default = "defaults::temperature_unit")]
    pub temperature_unit: String,
    #[serde(default = "defaults::wind_speed_unit")]
    pub wind_speed_unit: String,
    #[serde(default = "defaults::precipitation_unit")]
    pub precipitation_unit: String,
    #[serde(default = "defaults::timeformat")]
    pub timeformat: String,
    #[serde(default = "defaults::timezone")]
    pub timezone: String,
    #[serde(default)]
    pub past_days: Option<u32>,
    #[serde(default = "defaults::forecast_days")]
    pub forecast_days: u32,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub models: Option<String>,
    #[serde(default = "defaults::cell_selection")]
    pub cell_selection: String,
    #[serde(default)]
    pub elevation: Option<f64>,
}

impl ForecastParams {
    pub fn parse_hourly(&self) -> Vec<String> {
        parse_csv(&self.hourly)
    }

    pub fn parse_daily(&self) -> Vec<String> {
        parse_csv(&self.daily)
    }

    pub fn parse_current(&self) -> Vec<String> {
        parse_csv(&self.current)
    }
}

fn parse_csv(value: &Option<String>) -> Vec<String> {
    value
        .as_deref()
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

mod defaults {
    pub fn temperature_unit() -> String {
        "celsius".to_string()
    }

    pub fn wind_speed_unit() -> String {
        "kmh".to_string()
    }

    pub fn precipitation_unit() -> String {
        "mm".to_string()
    }

    pub fn timeformat() -> String {
        "iso8601".to_string()
    }

    pub fn timezone() -> String {
        "GMT".to_string()
    }

    pub fn forecast_days() -> u32 {
        7
    }

    pub fn cell_selection() -> String {
        "land".to_string()
    }
}

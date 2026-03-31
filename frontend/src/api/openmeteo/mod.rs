mod params;
mod response;
mod variables;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    Extension, Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use bits::{Job, JobResult, PollOutcome};
use bytes::BytesMut;
use chrono::Utc;
use futures::TryStreamExt;
use serde_json::{Value, json};

use crate::auth::AuthUser;
use crate::state::AppState;

const POLL_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ParamKey {
    param: String,
    levtype: String,
    level: Option<u32>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct GroupKey {
    levtype: String,
    level: Option<u32>,
}

#[derive(Clone)]
struct RequestedVariable {
    name: String,
    info: variables::VariableInfo,
    level: Option<u32>,
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new().route("/forecast", get(forecast))
}

async fn forecast(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    auth_user: Option<Extension<AuthUser>>,
    Query(params): Query<params::ForecastParams>,
) -> Response {
    let _ = (
        &params.temperature_unit,
        &params.wind_speed_unit,
        &params.precipitation_unit,
        &params.timeformat,
        &params.past_days,
        &params.start_date,
        &params.end_date,
        &params.models,
        &params.cell_selection,
    );

    let daily = params.parse_daily();
    if !daily.is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "daily variables are not supported yet",
        );
    }

    let current = params.parse_current();
    if !current.is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "current variables are not supported yet",
        );
    }

    let hourly_names = params.parse_hourly();
    if hourly_names.is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "hourly parameter must include at least one variable",
        );
    }

    let mut requested = Vec::with_capacity(hourly_names.len());
    for name in hourly_names {
        match variables::lookup(&name) {
            Some((info, level)) => requested.push(RequestedVariable { name, info, level }),
            None => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    &format!("unknown variable: {name}"),
                );
            }
        }
    }

    let forecast_hours = params.forecast_days.saturating_mul(24);
    let today = Utc::now().date_naive().format("%Y%m%d").to_string();

    let mut groups: HashMap<GroupKey, Vec<String>> = HashMap::new();
    let mut seen = HashSet::new();
    for variable in &requested {
        for (param, levtype, level) in variables::required_params(&variable.info, variable.level) {
            let key = (param.to_string(), levtype.to_string(), level);
            if seen.insert(key) {
                let group = GroupKey {
                    levtype: levtype.to_string(),
                    level,
                };
                groups.entry(group).or_default().push(param.to_string());
            }
        }
    }

    let mut submitted: Vec<(GroupKey, Vec<String>, String)> = Vec::with_capacity(groups.len());
    for (group, param_list) in groups {
        let param_str = param_list.join("/");
        let mut request = json!({
            "class": "od",
            "stream": "oper",
            "type": "fc",
            "expver": "0001",
            "domain": "g",
            "levtype": group.levtype,
            "param": param_str,
            "date": today,
            "time": "0000",
            "feature": {
                "type": "timeseries",
                "points": [[params.latitude, params.longitude]],
                "time_axis": "step",
                "axes": ["latitude", "longitude"],
                "range": {"start": 0, "end": forecast_hours}
            }
        });

        if let Some(level) = group.level {
            request["levelist"] = json!(level);
        }

        let mut job = Job::new(request);
        job.metadata = json!({"api": "openmeteo"}).into();
        let mut user_context = serde_json::Map::new();
        if let Some(ref ip) = super::client_ip(&headers) {
            user_context.insert("client_ip".to_string(), json!(ip));
        }
        if let Some(Extension(ref user)) = auth_user {
            if crate::api::check_admin_bypass(user, &state.admin_bypass_roles) {
                user_context.insert("can_bypass_role_check".to_string(), json!(true));
            }
            user_context.insert("auth".to_string(), serde_json::to_value(user).unwrap());
        }
        job.user = Value::Object(user_context).into();
        let id = state.bits.submit(job).id;
        submitted.push((group, param_list, id));
    }

    let started = Instant::now();

    let mut fetched: HashMap<ParamKey, Vec<Option<f64>>> = HashMap::new();

    for (group, param_list, id) in submitted {
        let outcome = state.bits.poll(&id, Some(POLL_TIMEOUT)).await;

        match outcome {
            PollOutcome::Ready(JobResult::Success { stream, .. }) => {
                let mut buf = BytesMut::new();
                tokio::pin!(stream);
                while let Some(chunk) = stream.try_next().await.unwrap_or(None) {
                    buf.extend_from_slice(&chunk);
                }
                let body = String::from_utf8_lossy(&buf);
                let covjson: Value = match serde_json::from_str(&body) {
                    Ok(v) => v,
                    Err(err) => {
                        return error_response(
                            StatusCode::BAD_GATEWAY,
                            &format!("failed to parse worker response: {err}"),
                        );
                    }
                };

                for param in &param_list {
                    let raw = extract_param_values(&covjson, param);
                    let source_unit = extract_source_unit(&covjson, param);
                    let converted = apply_unit_conversion(raw, source_unit.as_deref());
                    fetched.insert(
                        ParamKey {
                            param: param.clone(),
                            levtype: group.levtype.clone(),
                            level: group.level,
                        },
                        converted,
                    );
                }
            }
            PollOutcome::Pending { .. } => {
                return error_response(
                    StatusCode::GATEWAY_TIMEOUT,
                    &format!("timeout waiting for job {id}"),
                );
            }
            PollOutcome::Ready(JobResult::Error { message }) => {
                return error_response(
                    StatusCode::BAD_GATEWAY,
                    &format!("worker error: {message}"),
                );
            }
            PollOutcome::Ready(JobResult::Failed { reason }) => {
                return error_response(
                    StatusCode::BAD_GATEWAY,
                    &format!("worker failed: {reason}"),
                );
            }
            PollOutcome::NotFound => {
                return error_response(StatusCode::BAD_GATEWAY, &format!("job {id} not found"));
            }
            PollOutcome::JobLost => {
                return error_response(StatusCode::BAD_GATEWAY, &format!("job {id} lost"));
            }
            PollOutcome::Ready(JobResult::Redirect { .. }) => {
                return error_response(StatusCode::BAD_GATEWAY, "unexpected redirect from worker");
            }
            PollOutcome::Ready(JobResult::ClientGone) => {
                return error_response(StatusCode::BAD_GATEWAY, "client disconnected");
            }
            PollOutcome::Ready(JobResult::Cancelled) => {
                return error_response(StatusCode::BAD_GATEWAY, &format!("job {id} cancelled"));
            }
        }
    }

    let base_date = Utc::now().date_naive();
    let Some(start) = base_date.and_hms_opt(0, 0, 0) else {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to build base datetime",
        );
    };

    let hourly_times: Vec<String> = (0..=forecast_hours)
        .map(|hour| {
            (start + chrono::Duration::hours(hour as i64))
                .format("%Y-%m-%dT%H:%M")
                .to_string()
        })
        .collect();

    let expected_len = hourly_times.len();
    let mut hourly_results = Vec::with_capacity(requested.len());

    for variable in &requested {
        let values = assemble_variable(variable, &fetched);
        hourly_results.push(response::VariableResult {
            name: variable.name.clone(),
            unit: variable.info.unit.to_string(),
            values: fit_to_len(values, expected_len),
        });
    }

    let payload = response::build_forecast_response(
        params.latitude,
        params.longitude,
        params.elevation,
        &params.timezone,
        0,
        started.elapsed().as_secs_f64() * 1000.0,
        &hourly_times,
        &hourly_results,
    );

    (StatusCode::OK, Json(payload)).into_response()
}

fn error_response(status: StatusCode, reason: &str) -> Response {
    (status, Json(response::build_error_response(reason))).into_response()
}

fn assemble_variable(
    variable: &RequestedVariable,
    fetched: &HashMap<ParamKey, Vec<Option<f64>>>,
) -> Vec<Option<f64>> {
    match &variable.info.kind {
        variables::ParamKind::Direct { param, levtype } => fetched
            .get(&ParamKey {
                param: param.to_string(),
                levtype: levtype.to_string(),
                level: variable.level,
            })
            .cloned()
            .unwrap_or_default(),

        variables::ParamKind::PressureLevel { param } => fetched
            .get(&ParamKey {
                param: param.to_string(),
                levtype: "pl".to_string(),
                level: variable.level,
            })
            .cloned()
            .unwrap_or_default(),

        variables::ParamKind::WindSpeed {
            u_param,
            v_param,
            levtype,
        } => {
            let u = get_values(fetched, u_param, levtype, variable.level);
            let v = get_values(fetched, v_param, levtype, variable.level);
            compute_wind_speed(&u, &v)
        }

        variables::ParamKind::WindDirection {
            u_param,
            v_param,
            levtype,
        } => {
            let u = get_values(fetched, u_param, levtype, variable.level);
            let v = get_values(fetched, v_param, levtype, variable.level);
            compute_wind_direction(&u, &v)
        }

        variables::ParamKind::PressureLevelWindSpeed => {
            let u = get_values(fetched, "u", "pl", variable.level);
            let v = get_values(fetched, "v", "pl", variable.level);
            compute_wind_speed(&u, &v)
        }

        variables::ParamKind::PressureLevelWindDirection => {
            let u = get_values(fetched, "u", "pl", variable.level);
            let v = get_values(fetched, "v", "pl", variable.level);
            compute_wind_direction(&u, &v)
        }
    }
}

fn get_values(
    fetched: &HashMap<ParamKey, Vec<Option<f64>>>,
    param: &str,
    levtype: &str,
    level: Option<u32>,
) -> Vec<Option<f64>> {
    fetched
        .get(&ParamKey {
            param: param.to_string(),
            levtype: levtype.to_string(),
            level,
        })
        .cloned()
        .unwrap_or_default()
}

fn fit_to_len(mut values: Vec<Option<f64>>, len: usize) -> Vec<Option<f64>> {
    values.resize(len, None);
    values.truncate(len);
    values
}

fn compute_wind_speed(u: &[Option<f64>], v: &[Option<f64>]) -> Vec<Option<f64>> {
    u.iter()
        .zip(v.iter())
        .map(|(u, v)| match (u, v) {
            (Some(u), Some(v)) => Some((u * u + v * v).sqrt()),
            _ => None,
        })
        .collect()
}

fn compute_wind_direction(u: &[Option<f64>], v: &[Option<f64>]) -> Vec<Option<f64>> {
    u.iter()
        .zip(v.iter())
        .map(|(u, v)| match (u, v) {
            (Some(u), Some(v)) => {
                let dir = v.atan2(*u).to_degrees();
                Some(((180.0 + dir) % 360.0 + 360.0) % 360.0)
            }
            _ => None,
        })
        .collect()
}

fn extract_source_unit(covjson: &Value, param: &str) -> Option<String> {
    covjson
        .get("parameters")?
        .get(param)?
        .get("unit")?
        .get("symbol")?
        .as_str()
        .map(|s| s.to_string())
}

fn apply_unit_conversion(values: Vec<Option<f64>>, source_unit: Option<&str>) -> Vec<Option<f64>> {
    let converter: fn(f64) -> f64 = match source_unit {
        Some("K") => |v| v - 273.15,
        Some("Pa") => |v| v / 100.0,
        Some("m**2 s**-2") => |v| v / 9.80665,
        _ => |v| v,
    };

    values
        .into_iter()
        .map(|v| v.map(|x| round1(converter(x))))
        .collect()
}

fn round1(v: f64) -> f64 {
    (v * 10.0).round() / 10.0
}

fn extract_param_values(covjson: &Value, param: &str) -> Vec<Option<f64>> {
    extract_from_coverage_collection(covjson, param)
        .or_else(|| extract_from_single_coverage(covjson, param))
        .unwrap_or_default()
}

fn extract_from_coverage_collection(covjson: &Value, param: &str) -> Option<Vec<Option<f64>>> {
    let coverage = covjson.get("coverages")?.as_array()?.first()?;
    let vals = coverage.get("ranges")?.get(param)?.get("values")?;
    Some(parse_numeric_array(vals))
}

fn extract_from_single_coverage(covjson: &Value, param: &str) -> Option<Vec<Option<f64>>> {
    let vals = covjson.get("ranges")?.get(param)?.get("values")?;
    Some(parse_numeric_array(vals))
}

fn parse_numeric_array(values: &Value) -> Vec<Option<f64>> {
    values
        .as_array()
        .map(|arr| {
            arr.iter()
                .map(|v| {
                    if v.is_null() {
                        return None;
                    }
                    if let Some(n) = v.as_f64() {
                        return Some(n);
                    }
                    v.as_str().and_then(|s| s.parse::<f64>().ok())
                })
                .collect()
        })
        .unwrap_or_default()
}

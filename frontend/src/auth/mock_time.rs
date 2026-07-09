// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use axum::http::{HeaderMap, HeaderName};
use chrono::{DateTime, NaiveDate, NaiveTime, SecondsFormat, Utc};

pub const MOCK_TIME_HEADER: &str = "polytope-mock-time";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MockTime {
    pub now: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MockTimeAudit {
    pub real_username: String,
    pub real_realm: String,
    pub mocked_now: String,
    pub path: String,
    pub request_id: Option<String>,
    pub header: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MockTimeError {
    MultipleValues,
    NonUtf8,
    ControlCharacter,
    InvalidFormat,
}

impl MockTimeError {
    pub fn message(&self) -> String {
        match self {
            Self::MultipleValues => "Polytope-Mock-Time must be supplied at most once".to_string(),
            Self::NonUtf8 => "Polytope-Mock-Time must be valid UTF-8".to_string(),
            Self::ControlCharacter => {
                "Polytope-Mock-Time must not contain control characters".to_string()
            }
            Self::InvalidFormat => {
                "Polytope-Mock-Time must be HH:MM[:SS] in UTC or an RFC3339 datetime".to_string()
            }
        }
    }
}

pub fn has_mock_time_header(headers: &HeaderMap) -> bool {
    headers.contains_key(MOCK_TIME_HEADER)
}

pub fn parse_mock_time_header(headers: &HeaderMap) -> Result<Option<MockTime>, MockTimeError> {
    let name = HeaderName::from_static(MOCK_TIME_HEADER);
    let mut values = headers.get_all(&name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(MockTimeError::MultipleValues);
    }
    let value = value.to_str().map_err(|_| MockTimeError::NonUtf8)?;
    parse_mock_time_value(value).map(Some)
}

pub fn parse_mock_time_value(value: &str) -> Result<MockTime, MockTimeError> {
    parse_mock_time_value_for_date(value, Utc::now().date_naive())
}

fn parse_mock_time_value_for_date(
    value: &str,
    time_only_date: NaiveDate,
) -> Result<MockTime, MockTimeError> {
    if value.chars().any(char::is_control) {
        return Err(MockTimeError::ControlCharacter);
    }

    let value = value.trim();
    if value.contains('T') || value.contains('t') {
        let now = DateTime::parse_from_rfc3339(value)
            .map_err(|_| MockTimeError::InvalidFormat)?
            .with_timezone(&Utc);
        return Ok(MockTime { now });
    }

    let time = parse_time_only(value)?;
    let now = DateTime::from_naive_utc_and_offset(time_only_date.and_time(time), Utc);
    Ok(MockTime { now })
}

fn parse_time_only(value: &str) -> Result<NaiveTime, MockTimeError> {
    let mut parts = value.split(':');
    let hour = parse_time_component(parts.next())?;
    let minute = parse_time_component(parts.next())?;
    let second = match parts.next() {
        Some(second) => parse_time_component(Some(second))?,
        None => 0,
    };
    if parts.next().is_some() {
        return Err(MockTimeError::InvalidFormat);
    }

    NaiveTime::from_hms_opt(hour, minute, second).ok_or(MockTimeError::InvalidFormat)
}

fn parse_time_component(component: Option<&str>) -> Result<u32, MockTimeError> {
    let component = component.ok_or(MockTimeError::InvalidFormat)?;
    if component.is_empty()
        || !component
            .chars()
            .all(|character| character.is_ascii_digit())
    {
        return Err(MockTimeError::InvalidFormat);
    }
    component
        .parse::<u32>()
        .map_err(|_| MockTimeError::InvalidFormat)
}

#[cfg(test)]
fn parse_mock_time_value_on_date(
    value: &str,
    time_only_date: NaiveDate,
) -> Result<MockTime, MockTimeError> {
    parse_mock_time_value_for_date(value, time_only_date)
}

pub fn normalise_mocked_now(now: DateTime<Utc>) -> String {
    now.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use chrono::TimeZone;

    fn test_date() -> NaiveDate {
        NaiveDate::from_ymd_opt(2030, 1, 2).unwrap()
    }

    fn utc_datetime(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
    ) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
            .unwrap()
    }

    #[test]
    fn missing_header_returns_none() {
        let headers = HeaderMap::new();
        assert_eq!(parse_mock_time_header(&headers), Ok(None));
        assert!(!has_mock_time_header(&headers));
    }

    #[test]
    fn parses_hh_mm_ss_using_injected_utc_date() {
        let parsed = parse_mock_time_value_on_date("12:34:56", test_date()).unwrap();
        assert_eq!(parsed.now, utc_datetime(2030, 1, 2, 12, 34, 56));
        assert_eq!(normalise_mocked_now(parsed.now), "2030-01-02T12:34:56Z");
    }

    #[test]
    fn parses_hh_mm_with_seconds_defaulting_to_zero() {
        let parsed = parse_mock_time_value_on_date("12:34", test_date()).unwrap();
        assert_eq!(parsed.now, utc_datetime(2030, 1, 2, 12, 34, 0));
        assert_eq!(normalise_mocked_now(parsed.now), "2030-01-02T12:34:00Z");
    }

    #[test]
    fn parses_full_rfc3339_datetime_with_non_today_date() {
        let parsed = parse_mock_time_value_on_date("2040-05-06T07:08:09Z", test_date()).unwrap();
        assert_eq!(parsed.now, utc_datetime(2040, 5, 6, 7, 8, 9));
        assert_eq!(normalise_mocked_now(parsed.now), "2040-05-06T07:08:09Z");
    }

    #[test]
    fn normalises_rfc3339_datetime_with_non_utc_offset() {
        let parsed =
            parse_mock_time_value_on_date("2040-05-06T08:08:09+01:00", test_date()).unwrap();
        assert_eq!(parsed.now, utc_datetime(2040, 5, 6, 7, 8, 9));
        assert_eq!(normalise_mocked_now(parsed.now), "2040-05-06T07:08:09Z");
    }

    #[test]
    fn rejects_multiple_header_values() {
        let mut headers = HeaderMap::new();
        headers.append(MOCK_TIME_HEADER, HeaderValue::from_static("12:34:56"));
        headers.append(MOCK_TIME_HEADER, HeaderValue::from_static("12:35:56"));
        assert_eq!(
            parse_mock_time_header(&headers),
            Err(MockTimeError::MultipleValues)
        );
    }

    #[test]
    fn rejects_non_utf8_header_value() {
        let mut headers = HeaderMap::new();
        headers.insert(
            MOCK_TIME_HEADER,
            HeaderValue::from_bytes(b"12:34:\xff").unwrap(),
        );
        assert_eq!(
            parse_mock_time_header(&headers),
            Err(MockTimeError::NonUtf8)
        );
    }

    #[test]
    fn rejects_control_characters() {
        assert_eq!(
            parse_mock_time_value_on_date("12:34\u{7}", test_date()),
            Err(MockTimeError::ControlCharacter)
        );
    }

    #[test]
    fn rejects_unparseable_values_with_polytope_mock_time_message() {
        let error = parse_mock_time_value_on_date("not a time", test_date()).unwrap_err();
        assert_eq!(error, MockTimeError::InvalidFormat);
        assert!(error.message().contains("Polytope-Mock-Time"));
    }

    #[test]
    fn rejects_time_only_values_with_timezone_suffixes() {
        for value in ["12:34:56Z", "12:34+01:00"] {
            assert_eq!(
                parse_mock_time_value_on_date(value, test_date()),
                Err(MockTimeError::InvalidFormat)
            );
        }
    }

    #[test]
    fn time_only_midnight_rollover_uses_injected_utc_date() {
        let parsed = parse_mock_time_value_on_date("00:05", test_date()).unwrap();
        assert_eq!(parsed.now, utc_datetime(2030, 1, 2, 0, 5, 0));
        assert_eq!(normalise_mocked_now(parsed.now), "2030-01-02T00:05:00Z");
    }
}

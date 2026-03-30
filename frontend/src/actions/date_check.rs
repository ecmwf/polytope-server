use chrono::{Duration, Local, NaiveDate};

use crate::actions::coercion::request_field_as_strings;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DateCheckError(pub String);

impl std::fmt::Display for DateCheckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for DateCheckError {}

pub fn date_check(
    value: &serde_json::Value,
    allowed_values: &[String],
) -> Result<(), DateCheckError> {
    for allowed in allowed_values {
        date_check_single_rule(value, allowed)?;
    }
    Ok(())
}

pub fn date_check_single_rule(
    value: &serde_json::Value,
    allowed_value: &str,
) -> Result<(), DateCheckError> {
    let Some(comp) = allowed_value.chars().next() else {
        return Err(DateCheckError(
            "Invalid date comparison, expected < or >".into(),
        ));
    };
    let after = match comp {
        '<' => false,
        '>' => true,
        _ => {
            return Err(DateCheckError(format!(
                "Invalid date comparison {comp}, expected < or >"
            )));
        }
    };

    let offset = Local::now().date_naive() - parse_relative_delta(&allowed_value[1..])?;
    let formatted = offset.format("%Y%m%d").to_string();

    for part in expand_date_rule_input(value)? {
        check_single_date(&part, offset, &formatted, after)?;
    }

    Ok(())
}

fn expand_date_rule_input(value: &serde_json::Value) -> Result<Vec<String>, DateCheckError> {
    let raw = request_field_as_strings(value).join("/");
    let split: Vec<&str> = raw.split('/').collect();
    if (split.len() == 3 || split.len() == 5)
        && split
            .get(1)
            .is_some_and(|part| part.eq_ignore_ascii_case("to"))
    {
        if split.len() == 5
            && !split
                .get(3)
                .is_some_and(|part| part.eq_ignore_ascii_case("by"))
        {
            return Err(DateCheckError("Invalid date range".into()));
        }
        return Ok(vec![split[0].to_string(), split[2].to_string()]);
    }
    Ok(split.into_iter().map(str::to_string).collect())
}

fn check_single_date(
    value: &str,
    offset: NaiveDate,
    formatted_offset: &str,
    after: bool,
) -> Result<(), DateCheckError> {
    let date = if value.starts_with('0') || value.starts_with('-') {
        let delta = value.parse::<i64>().map_err(|_| {
            DateCheckError("Invalid date, expected real date in YYYYMMDD format".into())
        })?;
        Local::now().date_naive() + Duration::days(delta)
    } else {
        NaiveDate::parse_from_str(value, "%Y%m%d").map_err(|_| {
            DateCheckError("Invalid date, expected real date in YYYYMMDD format".into())
        })?
    };

    if after && date >= offset {
        Err(DateCheckError(format!(
            "Date is too recent, expected < {formatted_offset}"
        )))
    } else if !after && date < offset {
        Err(DateCheckError(format!(
            "Date is too old, expected > {formatted_offset}"
        )))
    } else {
        Ok(())
    }
}

fn parse_relative_delta(value: &str) -> Result<Duration, DateCheckError> {
    let mut chars = value.trim().chars().peekable();
    let mut duration = Duration::zero();
    let mut seen = false;

    while chars.peek().is_some() {
        let mut digits = String::new();
        while chars.peek().is_some_and(|ch| ch.is_ascii_digit()) {
            digits.push(chars.next().unwrap());
        }
        let amount = digits
            .parse::<i64>()
            .map_err(|_| DateCheckError(format!("Invalid relative date offset: {value}")))?;
        let unit = chars
            .next()
            .ok_or_else(|| DateCheckError(format!("Invalid relative date offset: {value}")))?;
        duration += match unit {
            'd' => Duration::days(amount),
            'h' => Duration::hours(amount),
            'm' => Duration::minutes(amount),
            _ => {
                return Err(DateCheckError(format!(
                    "Invalid relative date offset: {value}"
                )));
            }
        };
        seen = true;
    }

    if seen {
        Ok(duration)
    } else {
        Err(DateCheckError(format!(
            "Invalid relative date offset: {value}"
        )))
    }
}

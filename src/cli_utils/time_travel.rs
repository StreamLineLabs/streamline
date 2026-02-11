//! Time travel utilities for consume commands
//!
//! Provides human-friendly time expressions like:
//! - `--at "2024-01-15 14:30:00"` - Specific timestamp
//! - `--last 5m` - Last 5 minutes
//! - `--last 2h` - Last 2 hours
//! - `--last 1d` - Last 1 day

use chrono::{DateTime, Duration, Local, NaiveDateTime, TimeZone, Utc};

/// Parsed time expression result
#[derive(Debug, Clone, PartialEq)]
pub enum TimeExpression {
    /// Absolute timestamp in milliseconds since epoch
    Absolute(i64),
    /// Relative duration from now
    Relative(Duration),
    /// Earliest available offset
    Earliest,
    /// Latest available offset
    Latest,
}

impl TimeExpression {
    /// Convert to milliseconds since epoch
    pub fn to_timestamp_ms(&self) -> i64 {
        match self {
            TimeExpression::Absolute(ts) => *ts,
            TimeExpression::Relative(duration) => {
                let now = Utc::now();
                (now - *duration).timestamp_millis()
            }
            TimeExpression::Earliest => -2, // Kafka special value for earliest
            TimeExpression::Latest => -1,   // Kafka special value for latest
        }
    }

    /// Get a human-readable description
    pub fn describe(&self) -> String {
        match self {
            TimeExpression::Absolute(ts) => {
                let dt = DateTime::from_timestamp_millis(*ts)
                    .map(|dt| dt.with_timezone(&Local))
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                    .unwrap_or_else(|| format!("{}ms", ts));
                format!("at {}", dt)
            }
            TimeExpression::Relative(duration) => {
                let secs = duration.num_seconds();
                if secs < 60 {
                    format!("{} second(s) ago", secs)
                } else if secs < 3600 {
                    format!("{} minute(s) ago", secs / 60)
                } else if secs < 86400 {
                    format!("{} hour(s) ago", secs / 3600)
                } else {
                    format!("{} day(s) ago", secs / 86400)
                }
            }
            TimeExpression::Earliest => "earliest available".to_string(),
            TimeExpression::Latest => "latest available".to_string(),
        }
    }
}

/// Parse a time expression string
///
/// Supported formats:
/// - Relative: `5m`, `2h`, `1d`, `30s`, `1w`
/// - Absolute: `2024-01-15`, `2024-01-15 14:30:00`, `2024-01-15T14:30:00`
/// - Keywords: `now`, `today`, `yesterday`, `earliest`, `latest`
/// - Unix timestamp: `1705312200` (10 digits) or `1705312200000` (13 digits)
pub fn parse_time_expression(input: &str) -> Result<TimeExpression, String> {
    let input = input.trim().to_lowercase();

    // Keywords
    match input.as_str() {
        "now" | "latest" => return Ok(TimeExpression::Latest),
        "earliest" | "beginning" => return Ok(TimeExpression::Earliest),
        "today" => {
            let today = Local::now()
                .date_naive()
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| "Failed to create midnight time for today".to_string())?;
            let ts = Local
                .from_local_datetime(&today)
                .single()
                .ok_or_else(|| "Failed to convert today to local datetime".to_string())?
                .timestamp_millis();
            return Ok(TimeExpression::Absolute(ts));
        }
        "yesterday" => {
            let yesterday = (Local::now() - Duration::days(1))
                .date_naive()
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| "Failed to create midnight time for yesterday".to_string())?;
            let ts = Local
                .from_local_datetime(&yesterday)
                .single()
                .ok_or_else(|| "Failed to convert yesterday to local datetime".to_string())?
                .timestamp_millis();
            return Ok(TimeExpression::Absolute(ts));
        }
        _ => {}
    }

    // Try relative duration (e.g., "5m", "2h", "1d")
    if let Some(duration) = parse_relative_duration(&input) {
        return Ok(TimeExpression::Relative(duration));
    }

    // Try unix timestamp (seconds or milliseconds)
    if let Ok(ts) = input.parse::<i64>() {
        // If it's 13 digits, assume milliseconds
        // If it's 10 digits, assume seconds
        let ts_ms = if ts > 1_000_000_000_000 {
            ts
        } else {
            ts * 1000
        };
        return Ok(TimeExpression::Absolute(ts_ms));
    }

    // Try various datetime formats
    if let Some(ts) = parse_datetime(&input) {
        return Ok(TimeExpression::Absolute(ts));
    }

    Err(format!(
        "Invalid time expression: '{}'. \n\
        Examples:\n\
          - Relative: 5m, 2h, 1d, 30s, 1w\n\
          - Absolute: 2024-01-15, 2024-01-15 14:30:00\n\
          - Keywords: now, today, yesterday, earliest",
        input
    ))
}

/// Parse a relative duration string like "5m", "2h", "1d"
fn parse_relative_duration(input: &str) -> Option<Duration> {
    let input = input.trim();
    if input.is_empty() {
        return None;
    }

    // Find where the number ends and the unit starts
    let (num_str, unit) = input
        .find(|c: char| !c.is_ascii_digit())
        .map(|i| (&input[..i], &input[i..]))
        .unwrap_or((input, ""));

    let num: i64 = num_str.parse().ok()?;

    match unit.trim() {
        "s" | "sec" | "secs" | "second" | "seconds" => Some(Duration::seconds(num)),
        "m" | "min" | "mins" | "minute" | "minutes" => Some(Duration::minutes(num)),
        "h" | "hr" | "hrs" | "hour" | "hours" => Some(Duration::hours(num)),
        "d" | "day" | "days" => Some(Duration::days(num)),
        "w" | "wk" | "wks" | "week" | "weeks" => Some(Duration::weeks(num)),
        "" => {
            // If no unit and the number looks like a unix timestamp (10+ digits),
            // return None so it can be parsed as an absolute timestamp
            if num_str.len() >= 10 {
                return None;
            }
            // Otherwise assume minutes for small numbers, seconds for large
            if num < 60 {
                Some(Duration::minutes(num))
            } else {
                Some(Duration::seconds(num))
            }
        }
        _ => None,
    }
}

/// Parse various datetime formats
fn parse_datetime(input: &str) -> Option<i64> {
    // Try ISO 8601 with T separator
    if let Ok(dt) = DateTime::parse_from_rfc3339(input) {
        return Some(dt.timestamp_millis());
    }

    // Common formats to try
    let formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
        "%d-%m-%Y",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y",
    ];

    for format in &formats {
        if let Ok(naive) = NaiveDateTime::parse_from_str(input, format) {
            if let Some(local) = Local.from_local_datetime(&naive).single() {
                return Some(local.timestamp_millis());
            }
        }
        // Try parsing as date only (with midnight time)
        if let Ok(date) = chrono::NaiveDate::parse_from_str(input, format) {
            let naive = date.and_hms_opt(0, 0, 0)?;
            if let Some(local) = Local.from_local_datetime(&naive).single() {
                return Some(local.timestamp_millis());
            }
        }
    }

    None
}

/// Format a timestamp for display
pub fn format_timestamp(ts_ms: i64) -> String {
    DateTime::from_timestamp_millis(ts_ms)
        .map(|dt| dt.with_timezone(&Local))
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| format!("{}ms", ts_ms))
}

/// Calculate the timestamp for "last N" queries
pub fn last_n_timestamp(duration: &Duration) -> i64 {
    (Utc::now() - *duration).timestamp_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_relative_duration() {
        assert_eq!(parse_relative_duration("5m"), Some(Duration::minutes(5)));
        assert_eq!(parse_relative_duration("2h"), Some(Duration::hours(2)));
        assert_eq!(parse_relative_duration("1d"), Some(Duration::days(1)));
        assert_eq!(parse_relative_duration("30s"), Some(Duration::seconds(30)));
        assert_eq!(parse_relative_duration("1w"), Some(Duration::weeks(1)));
        assert_eq!(
            parse_relative_duration("10 minutes"),
            Some(Duration::minutes(10))
        );
    }

    #[test]
    fn test_parse_time_expression_keywords() {
        assert_eq!(
            parse_time_expression("earliest").unwrap(),
            TimeExpression::Earliest
        );
        assert_eq!(
            parse_time_expression("latest").unwrap(),
            TimeExpression::Latest
        );
        assert_eq!(
            parse_time_expression("now").unwrap(),
            TimeExpression::Latest
        );
    }

    #[test]
    fn test_parse_time_expression_relative() {
        match parse_time_expression("5m").unwrap() {
            TimeExpression::Relative(d) => assert_eq!(d, Duration::minutes(5)),
            _ => panic!("Expected relative duration"),
        }
    }

    #[test]
    fn test_parse_time_expression_unix_timestamp() {
        // Seconds
        match parse_time_expression("1705312200").unwrap() {
            TimeExpression::Absolute(ts) => assert_eq!(ts, 1705312200000),
            _ => panic!("Expected absolute timestamp"),
        }
        // Milliseconds
        match parse_time_expression("1705312200000").unwrap() {
            TimeExpression::Absolute(ts) => assert_eq!(ts, 1705312200000),
            _ => panic!("Expected absolute timestamp"),
        }
    }

    #[test]
    fn test_time_expression_describe() {
        let expr = TimeExpression::Relative(Duration::minutes(5));
        assert!(expr.describe().contains("5 minute"));

        let expr = TimeExpression::Earliest;
        assert!(expr.describe().contains("earliest"));
    }

    #[test]
    fn test_format_timestamp() {
        // Just check it doesn't panic
        let result = format_timestamp(1705312200000);
        assert!(!result.is_empty());
    }
}

//! Mock data generation for the dev server
//!
//! Template-based message generation with variable substitution.
//! Supports: {{uuid}}, {{timestamp}}, {{i}}, {{random:min:max}},
//! {{random_choice:a,b,c}}, {{now:format}}

use rand::Rng;
use uuid::Uuid;

/// Generate a message from a template, substituting variables
pub fn generate_message(template: &str, index: usize) -> String {
    let mut result = template.to_string();
    let mut rng = rand::thread_rng();
    let mut search_from = 0;

    // Process all template variables
    while let Some(pos) = result[search_from..].find("{{") {
        let start = search_from + pos;
        let end = match result[start..].find("}}") {
            Some(pos) => start + pos + 2,
            None => break,
        };

        let var = &result[start + 2..end - 2];
        let (replacement, known) = evaluate_variable(var, index, &mut rng);
        if known {
            result.replace_range(start..end, &replacement);
            // Don't advance search_from — replacement may contain new vars
        } else {
            // Skip past this unknown variable to avoid infinite loop
            search_from = end;
        }
    }

    result
}

/// Evaluate a single template variable.
/// Returns (replacement, known) where known=true if the variable was recognized.
fn evaluate_variable(var: &str, index: usize, rng: &mut impl Rng) -> (String, bool) {
    if var == "uuid" {
        return (Uuid::new_v4().to_string(), true);
    }

    if var == "timestamp" {
        return (chrono::Utc::now().timestamp_millis().to_string(), true);
    }

    if var == "i" || var == "index" {
        return (index.to_string(), true);
    }

    if let Some(range_str) = var.strip_prefix("random:") {
        let parts: Vec<&str> = range_str.split(':').collect();
        if parts.len() == 2 {
            if let (Ok(min), Ok(max)) = (parts[0].parse::<i64>(), parts[1].parse::<i64>()) {
                return (rng.gen_range(min..=max).to_string(), true);
            }
        }
        return ("0".to_string(), true);
    }

    if let Some(choices_str) = var.strip_prefix("random_choice:") {
        let choices: Vec<&str> = choices_str.split(',').collect();
        if !choices.is_empty() {
            let idx = rng.gen_range(0..choices.len());
            return (choices[idx].to_string(), true);
        }
        return (String::new(), true);
    }

    if let Some(format_str) = var.strip_prefix("now:") {
        return (chrono::Utc::now().format(format_str).to_string(), true);
    }

    if var == "date" {
        return (chrono::Utc::now().format("%Y-%m-%d").to_string(), true);
    }

    if var == "time" {
        return (chrono::Utc::now().format("%H:%M:%S").to_string(), true);
    }

    if var == "datetime" {
        return (
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            true,
        );
    }

    if var == "hostname" {
        return ("dev-localhost".to_string(), true);
    }

    if let Some(prefix_str) = var.strip_prefix("seq:") {
        return (format!("{}{}", prefix_str, index), true);
    }

    // Unknown variable - return as-is
    (format!("{{{{{}}}}}", var), false)
}

/// Generate a batch of messages from a template
pub fn generate_batch(template: &str, count: usize) -> Vec<String> {
    (0..count).map(|i| generate_message(template, i)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_uuid() {
        let msg = generate_message("id={{uuid}}", 0);
        assert!(msg.starts_with("id="));
        assert_eq!(msg.len(), 3 + 36); // "id=" + UUID
    }

    #[test]
    fn test_generate_index() {
        let msg = generate_message("item-{{i}}", 42);
        assert_eq!(msg, "item-42");
    }

    #[test]
    fn test_generate_timestamp() {
        let msg = generate_message("ts={{timestamp}}", 0);
        assert!(msg.starts_with("ts="));
        let ts_str = &msg[3..];
        assert!(ts_str.parse::<i64>().is_ok());
    }

    #[test]
    fn test_generate_random_range() {
        for _ in 0..100 {
            let msg = generate_message("val={{random:1:10}}", 0);
            let val_str = &msg[4..];
            let val: i64 = val_str.parse().unwrap();
            assert!((1..=10).contains(&val));
        }
    }

    #[test]
    fn test_generate_random_choice() {
        let choices = ["a", "b", "c"];
        for _ in 0..100 {
            let msg = generate_message("pick={{random_choice:a,b,c}}", 0);
            let pick = &msg[5..];
            assert!(choices.contains(&pick));
        }
    }

    #[test]
    fn test_generate_date() {
        let msg = generate_message("date={{date}}", 0);
        assert!(msg.starts_with("date="));
        assert_eq!(msg.len(), 5 + 10); // "date=" + "YYYY-MM-DD"
    }

    #[test]
    fn test_generate_seq() {
        let msg = generate_message("{{seq:user-}}", 5);
        assert_eq!(msg, "user-5");
    }

    #[test]
    fn test_generate_complex_template() {
        let template = r#"{"id":"{{uuid}}","index":{{i}},"value":{{random:1:100}}}"#;
        let msg = generate_message(template, 7);
        let parsed: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert!(parsed["id"].is_string());
        assert_eq!(parsed["index"], 7);
        assert!(parsed["value"].as_i64().unwrap() >= 1);
        assert!(parsed["value"].as_i64().unwrap() <= 100);
    }

    #[test]
    fn test_generate_batch() {
        let messages = generate_batch("msg-{{i}}", 5);
        assert_eq!(messages.len(), 5);
        assert_eq!(messages[0], "msg-0");
        assert_eq!(messages[4], "msg-4");
    }

    #[test]
    fn test_unknown_variable_passthrough() {
        let msg = generate_message("{{unknown_var}}", 0);
        assert_eq!(msg, "{{unknown_var}}");
    }

    #[test]
    fn test_no_variables() {
        let msg = generate_message("plain text", 0);
        assert_eq!(msg, "plain text");
    }
}

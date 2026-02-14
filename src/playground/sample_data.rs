//! Sample data generators for playground mode.
//!
//! Generates realistic JSON data for demo topics: events, users, metrics, and logs.

/// Generate JSON page/click events.
pub fn generate_events(count: u32) -> Vec<String> {
    let pages = ["/products", "/cart", "/checkout", "/home", "/search", "/profile"];
    let event_types = ["page_view", "click", "scroll", "form_submit", "add_to_cart"];

    (0..count)
        .map(|i| {
            let ts = base_timestamp() + (i as i64 * 1000);
            format!(
                r#"{{"type":"{}","user_id":"u{}","page":"{}","session_id":"sess-{}","ts":{}}}"#,
                event_types[i as usize % event_types.len()],
                (i % 50) + 1,
                pages[i as usize % pages.len()],
                i / 10,
                ts,
            )
        })
        .collect()
}

/// Generate JSON user profiles.
pub fn generate_users(count: u32) -> Vec<String> {
    let first_names = ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Hank"];
    let last_names = ["Smith", "Jones", "Brown", "Wilson", "Taylor", "Clark", "Hall", "Young"];
    let plans = ["free", "pro", "enterprise"];

    (0..count)
        .map(|i| {
            let idx = i as usize;
            let first = first_names[idx % first_names.len()];
            let last = last_names[idx % last_names.len()];
            let ts = base_timestamp() + (i as i64 * 500);
            format!(
                r#"{{"user_id":"u{}","name":"{} {}","email":"{}{}@example.com","plan":"{}","signup_ts":{}}}"#,
                i + 1,
                first,
                last,
                first.to_lowercase(),
                i + 1,
                plans[idx % plans.len()],
                ts,
            )
        })
        .collect()
}

/// Generate JSON metric data points.
pub fn generate_metrics(count: u32) -> Vec<String> {
    let metric_names = ["cpu_usage", "memory_usage", "disk_io", "network_rx", "request_latency"];
    let hosts = ["web-01", "web-02", "api-01", "api-02", "db-01"];

    (0..count)
        .map(|i| {
            let idx = i as usize;
            let value = match metric_names[idx % metric_names.len()] {
                "cpu_usage" => 20.0 + ((i as f64 * 7.3) % 70.0),
                "memory_usage" => 40.0 + ((i as f64 * 3.1) % 50.0),
                "disk_io" => (i as f64 * 11.7) % 100.0,
                "network_rx" => 100.0 + ((i as f64 * 23.9) % 900.0),
                _ => 5.0 + ((i as f64 * 1.7) % 495.0),
            };
            let ts = base_timestamp() + (i as i64 * 1000);
            format!(
                r#"{{"metric":"{}","value":{:.1},"host":"{}","unit":"{}","ts":{}}}"#,
                metric_names[idx % metric_names.len()],
                value,
                hosts[idx % hosts.len()],
                unit_for(metric_names[idx % metric_names.len()]),
                ts,
            )
        })
        .collect()
}

/// Generate JSON log entries with levels.
pub fn generate_logs(count: u32) -> Vec<String> {
    let services = ["api-gateway", "user-service", "order-service", "auth-service", "payment-service"];
    let messages_info = [
        "Request processed successfully",
        "Cache hit for key",
        "Connection pool healthy",
        "Health check passed",
        "Configuration reloaded",
    ];
    let messages_warn = [
        "Slow query detected",
        "Rate limit approaching threshold",
        "Retry attempt",
        "Connection pool nearing capacity",
    ];
    let messages_error = [
        "Failed to connect to database",
        "Request timeout after 30s",
        "Invalid authentication token",
    ];

    (0..count)
        .map(|i| {
            let idx = i as usize;
            let (level, msg) = match i % 10 {
                0 => ("ERROR", messages_error[idx % messages_error.len()]),
                1 | 5 => ("WARN", messages_warn[idx % messages_warn.len()]),
                _ => ("INFO", messages_info[idx % messages_info.len()]),
            };
            let ts = base_timestamp() + (i as i64 * 200);
            format!(
                r#"{{"level":"{}","service":"{}","message":"{}","request_id":"req-{}","ts":{}}}"#,
                level,
                services[idx % services.len()],
                msg,
                i,
                ts,
            )
        })
        .collect()
}

fn base_timestamp() -> i64 {
    // Use a fixed recent-ish epoch offset so data looks realistic
    // but is deterministic for tests
    1_700_000_000_000
}

fn unit_for(metric: &str) -> &'static str {
    match metric {
        "cpu_usage" | "memory_usage" => "percent",
        "disk_io" => "MB/s",
        "network_rx" => "KB/s",
        "request_latency" => "ms",
        _ => "",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_events() {
        let events = generate_events(10);
        assert_eq!(events.len(), 10);
        for e in &events {
            assert!(e.contains("\"type\""));
            assert!(e.contains("\"user_id\""));
        }
    }

    #[test]
    fn test_generate_users() {
        let users = generate_users(5);
        assert_eq!(users.len(), 5);
        for u in &users {
            assert!(u.contains("\"user_id\""));
            assert!(u.contains("\"email\""));
        }
    }

    #[test]
    fn test_generate_metrics() {
        let metrics = generate_metrics(20);
        assert_eq!(metrics.len(), 20);
        for m in &metrics {
            assert!(m.contains("\"metric\""));
            assert!(m.contains("\"value\""));
        }
    }

    #[test]
    fn test_generate_logs() {
        let logs = generate_logs(30);
        assert_eq!(logs.len(), 30);
        let has_error = logs.iter().any(|l| l.contains("\"ERROR\""));
        let has_info = logs.iter().any(|l| l.contains("\"INFO\""));
        assert!(has_error);
        assert!(has_info);
    }
}

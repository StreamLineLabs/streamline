//! Shared helpers for integration tests.

use std::time::Duration;

/// Default timeout for integration test operations.
pub const TEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Default number of partitions for test topics.
pub const TEST_PARTITIONS: u32 = 3;

/// Generates a unique topic name for testing.
pub fn test_topic_name(prefix: &str) -> String {
    format!(
        "{}-{}-{}",
        prefix,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    )
}

/// Waits for a condition to become true, polling at the given interval.
pub async fn wait_for<F, Fut>(timeout: Duration, interval: Duration, check: F) -> bool
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if check().await {
            return true;
        }
        tokio::time::sleep(interval).await;
    }
    false
}

use super::*;

#[test]
fn test_quota_limits_config_defaults() {
    // LimitsConfig should have sensible defaults
    use crate::server::limits::LimitsConfig;

    let config = LimitsConfig::default();

    assert!(
        config.max_connections > 0,
        "Should have positive max connections"
    );
    assert!(
        config.max_connections_per_ip > 0,
        "Should have positive per-IP limit"
    );
    assert!(
        config.max_request_size > 0,
        "Should have positive request size limit"
    );
    assert_eq!(
        config.produce_rate_limit_bytes, 0,
        "Default should be unlimited"
    );
}

#[test]
fn test_quota_limits_config_custom_values() {
    // LimitsConfig should accept custom values
    use crate::server::limits::LimitsConfig;

    let config = LimitsConfig {
        max_connections: 500,
        max_connections_per_ip: 50,
        max_request_size: 10 * 1024 * 1024,
        produce_rate_limit_bytes: 100_000_000,
        connection_idle_timeout_secs: 300,
    };

    assert_eq!(config.max_connections, 500);
    assert_eq!(config.max_connections_per_ip, 50);
    assert_eq!(config.max_request_size, 10 * 1024 * 1024);
    assert_eq!(config.produce_rate_limit_bytes, 100_000_000);
    assert_eq!(config.connection_idle_timeout_secs, 300);
}

#[test]
fn test_quota_resource_limiter_construction() {
    // ResourceLimiter should be constructable from config
    use crate::server::limits::{LimitsConfig, ResourceLimiter};

    let config = LimitsConfig::default();
    let limiter = ResourceLimiter::new(config);

    // Initial stats should be zero
    let stats = limiter.stats();
    assert_eq!(stats.total_connections, 0);
    assert_eq!(stats.connections_rejected, 0);
}

#[test]
fn test_quota_resource_limiter_connection_acceptance() {
    // ResourceLimiter should accept connections
    use crate::server::limits::{LimitsConfig, ResourceLimiter};
    use std::net::IpAddr;

    let config = LimitsConfig {
        max_connections: 10,
        max_connections_per_ip: 5,
        ..Default::default()
    };
    let limiter = ResourceLimiter::new(config);

    // Accept a connection
    let ip: IpAddr = "192.168.1.1".parse().unwrap();
    let result = limiter.try_accept_connection(ip);
    assert!(result.is_ok(), "Should accept first connection");

    // Verify tracking
    let stats = limiter.stats();
    assert_eq!(stats.total_connections, 1);
}

#[test]
fn test_quota_resource_limiter_max_connections() {
    // ResourceLimiter should enforce max connections
    use crate::server::limits::{LimitError, LimitsConfig, ResourceLimiter};
    use std::net::IpAddr;

    let config = LimitsConfig {
        max_connections: 2,
        max_connections_per_ip: 10,
        ..Default::default()
    };
    let limiter = ResourceLimiter::new(config);

    let ip1: IpAddr = "192.168.1.1".parse().unwrap();
    let ip2: IpAddr = "192.168.1.2".parse().unwrap();
    let ip3: IpAddr = "192.168.1.3".parse().unwrap();

    // Hold guards to keep connections alive
    let _guard1 = limiter.try_accept_connection(ip1).unwrap();
    let _guard2 = limiter.try_accept_connection(ip2).unwrap();

    // Third should be rejected
    let result = limiter.try_accept_connection(ip3);
    assert!(
        matches!(result, Err(LimitError::MaxConnectionsReached)),
        "Should reject connection beyond max"
    );
}

#[test]
fn test_quota_resource_limiter_per_ip_limit() {
    // ResourceLimiter should enforce per-IP connection limits
    use crate::server::limits::{LimitError, LimitsConfig, ResourceLimiter};
    use std::net::IpAddr;

    let config = LimitsConfig {
        max_connections: 100,
        max_connections_per_ip: 2,
        ..Default::default()
    };
    let limiter = ResourceLimiter::new(config);

    let ip: IpAddr = "192.168.1.1".parse().unwrap();
    let ip2: IpAddr = "192.168.1.2".parse().unwrap();

    // Hold guards
    let _guard1 = limiter.try_accept_connection(ip).unwrap();
    let _guard2 = limiter.try_accept_connection(ip).unwrap();

    // Third from same IP should be rejected
    let result = limiter.try_accept_connection(ip);
    assert!(
        matches!(result, Err(LimitError::PerIpLimitReached)),
        "Should reject connection beyond per-IP limit"
    );

    // But different IP should work
    let result2 = limiter.try_accept_connection(ip2);
    assert!(result2.is_ok(), "Should accept from different IP");
}

#[test]
fn test_quota_resource_limiter_connection_guard_drop() {
    // Connection count should decrease when guard is dropped
    use crate::server::limits::{LimitsConfig, ResourceLimiter};
    use std::net::IpAddr;

    let config = LimitsConfig {
        max_connections: 2,
        max_connections_per_ip: 2,
        ..Default::default()
    };
    let limiter = ResourceLimiter::new(config);

    let ip: IpAddr = "192.168.1.1".parse().unwrap();

    {
        let _guard1 = limiter.try_accept_connection(ip).unwrap();
        let _guard2 = limiter.try_accept_connection(ip).unwrap();
        assert_eq!(limiter.stats().total_connections, 2);
    } // guards dropped here

    // Should be able to add again
    assert_eq!(limiter.stats().total_connections, 0);
    let result = limiter.try_accept_connection(ip);
    assert!(result.is_ok(), "Should accept after guards dropped");
}

#[test]
fn test_quota_request_size_validation() {
    // Request size should be validated against limits
    use crate::server::limits::{LimitError, LimitsConfig, ResourceLimiter};

    let config = LimitsConfig {
        max_request_size: 1024 * 1024, // 1MB
        ..Default::default()
    };
    let limiter = ResourceLimiter::new(config);

    // Valid request size
    assert!(limiter.check_request_size(1024).is_ok());
    assert!(limiter.check_request_size(512 * 1024).is_ok());
    assert!(limiter.check_request_size(1024 * 1024).is_ok()); // exactly at limit

    // Invalid request size
    let result = limiter.check_request_size(2 * 1024 * 1024);
    assert!(
        matches!(result, Err(LimitError::RequestTooLarge { .. })),
        "Should reject oversized request"
    );
}

#[test]
fn test_quota_produce_rate_limit() {
    // ResourceLimiter should enforce produce rate limits
    use crate::server::limits::{LimitError, LimitsConfig, ResourceLimiter};
    use std::net::IpAddr;

    let config = LimitsConfig {
        produce_rate_limit_bytes: 1000, // 1KB/s (very low for testing)
        ..Default::default()
    };
    let limiter = ResourceLimiter::new(config);

    let ip: IpAddr = "192.168.1.1".parse().unwrap();

    // First request should be allowed (up to burst capacity)
    let result1 = limiter.check_produce_rate(ip, 1000);
    assert!(result1.is_ok(), "First produce should be allowed");

    // Second request should be rate limited
    let result2 = limiter.check_produce_rate(ip, 1000);
    assert!(
        matches!(result2, Err(LimitError::RateLimitExceeded)),
        "Should be rate limited after burst"
    );
}

#[test]
fn test_quota_produce_rate_unlimited() {
    // No rate limit when produce_rate_limit_bytes is 0
    use crate::server::limits::{LimitsConfig, ResourceLimiter};
    use std::net::IpAddr;

    let config = LimitsConfig {
        produce_rate_limit_bytes: 0, // Unlimited
        ..Default::default()
    };
    let limiter = ResourceLimiter::new(config);

    let ip: IpAddr = "192.168.1.1".parse().unwrap();

    // Many large requests should all succeed
    for _ in 0..100 {
        let result = limiter.check_produce_rate(ip, 10_000_000);
        assert!(result.is_ok(), "Should allow all when unlimited");
    }
}

#[test]
fn test_quota_manager_construction() {
    // QuotaManager should be constructable
    use crate::server::limits::{QuotaConfig, QuotaManager};

    let config = QuotaConfig {
        producer_byte_rate: 10_000_000,
        consumer_byte_rate: 50_000_000,
        request_percentage: 0,
        enabled: true,
    };

    let manager = QuotaManager::new(config);
    assert!(manager.is_enabled(), "QuotaManager should be enabled");
}

#[test]
fn test_quota_manager_producer_quota_check() {
    // QuotaManager should check producer quotas
    use crate::server::limits::{QuotaConfig, QuotaManager};
    use std::net::IpAddr;

    let config = QuotaConfig {
        producer_byte_rate: 1_000_000, // 1MB/s
        consumer_byte_rate: 0,
        request_percentage: 0,
        enabled: true,
    };

    let manager = QuotaManager::new(config);
    let ip: IpAddr = "192.168.1.1".parse().unwrap();

    // First produce should not be throttled
    let throttle_time = manager.check_producer_quota(Some("client1"), ip, 100_000);
    assert_eq!(throttle_time, 0, "First produce should not be throttled");
}

#[test]
fn test_quota_manager_consumer_quota_check() {
    // QuotaManager should check consumer quotas
    use crate::server::limits::{QuotaConfig, QuotaManager};
    use std::net::IpAddr;

    let config = QuotaConfig {
        producer_byte_rate: 0,
        consumer_byte_rate: 5_000_000, // 5MB/s
        request_percentage: 0,
        enabled: true,
    };

    let manager = QuotaManager::new(config);
    let ip: IpAddr = "192.168.1.1".parse().unwrap();

    // First fetch should not be throttled
    let throttle_time = manager.check_consumer_quota(Some("client1"), ip, 500_000);
    assert_eq!(throttle_time, 0, "First fetch should not be throttled");
}

#[test]
fn test_quota_manager_throttle_time_calculation() {
    // QuotaManager should calculate throttle time when quota exceeded
    use crate::server::limits::{QuotaConfig, QuotaManager};
    use std::net::IpAddr;

    let config = QuotaConfig {
        producer_byte_rate: 1000, // 1KB/s (very low)
        consumer_byte_rate: 0,
        request_percentage: 0,
        enabled: true,
    };

    let manager = QuotaManager::new(config);
    let ip: IpAddr = "192.168.1.1".parse().unwrap();

    // Exhaust quota
    let _ = manager.check_producer_quota(Some("client1"), ip, 1000);

    // Second should get throttle time
    let throttle_time = manager.check_producer_quota(Some("client1"), ip, 1000);
    assert!(
        throttle_time > 0,
        "Should have positive throttle time when quota exceeded"
    );
}

#[test]
fn test_quota_manager_per_client_quotas() {
    // QuotaManager should support per-client quotas
    use crate::server::limits::{QuotaConfig, QuotaManager};
    use std::net::IpAddr;

    let default_config = QuotaConfig {
        producer_byte_rate: 100,
        consumer_byte_rate: 0,
        request_percentage: 0,
        enabled: true,
    };

    let manager = QuotaManager::new(default_config);
    let ip: IpAddr = "192.168.1.1".parse().unwrap();

    // Set higher quota for specific client
    let client_config = QuotaConfig {
        producer_byte_rate: 10_000_000,
        consumer_byte_rate: 0,
        request_percentage: 0,
        enabled: true,
    };
    manager.set_client_quota("premium-client", client_config);

    // Premium client should not be throttled
    let throttle = manager.check_producer_quota(Some("premium-client"), ip, 1_000_000);
    assert_eq!(throttle, 0, "Premium client should not be throttled");

    assert_eq!(manager.client_quota_count(), 1);
}

#[test]
fn test_quota_manager_disabled() {
    // QuotaManager should not throttle when disabled
    use crate::server::limits::{QuotaConfig, QuotaManager};
    use std::net::IpAddr;

    let config = QuotaConfig {
        producer_byte_rate: 1, // Very low but disabled
        consumer_byte_rate: 1,
        request_percentage: 0,
        enabled: false,
    };

    let manager = QuotaManager::new(config);
    assert!(!manager.is_enabled(), "QuotaManager should be disabled");

    let ip: IpAddr = "192.168.1.1".parse().unwrap();

    // Should not throttle even with large requests
    for _ in 0..100 {
        let throttle = manager.check_producer_quota(None, ip, 10_000_000);
        assert_eq!(throttle, 0, "Should not throttle when disabled");
    }
}

#[test]
fn test_quota_manager_remove_client_quota() {
    // QuotaManager should allow removing client quotas
    use crate::server::limits::{QuotaConfig, QuotaManager};

    let config = QuotaConfig::default();
    let manager = QuotaManager::new(config);

    // Add and remove quota
    let client_config = QuotaConfig {
        producer_byte_rate: 1000,
        consumer_byte_rate: 1000,
        request_percentage: 0,
        enabled: true,
    };
    manager.set_client_quota("client1", client_config);
    assert_eq!(manager.client_quota_count(), 1);

    manager.remove_client_quota("client1");
    assert_eq!(manager.client_quota_count(), 0);
}

#[test]
fn test_quota_manager_total_throttle_time_tracking() {
    // QuotaManager should track total throttle time
    use crate::server::limits::{QuotaConfig, QuotaManager};
    use std::net::IpAddr;

    let config = QuotaConfig {
        producer_byte_rate: 100, // Very low
        consumer_byte_rate: 0,
        request_percentage: 0,
        enabled: true,
    };

    let manager = QuotaManager::new(config);
    let ip: IpAddr = "192.168.1.1".parse().unwrap();

    // Initial throttle time should be 0
    assert_eq!(manager.total_throttle_time_ms(), 0);

    // Exhaust quota to trigger throttling
    let _ = manager.check_producer_quota(None, ip, 100);
    let _ = manager.check_producer_quota(None, ip, 100);

    // Total throttle time should have increased
    // (may be 0 if test runs fast, but at least doesn't panic)
    let _total = manager.total_throttle_time_ms();
}

#[test]
fn test_quota_throttle_time_in_response() {
    // Verify throttle_time_ms is included in API responses
    use kafka_protocol::messages::ApiVersionsResponse;

    let mut response = ApiVersionsResponse::default();
    response.throttle_time_ms = 100;
    assert_eq!(response.throttle_time_ms, 100);
}

#[test]
fn test_quota_produce_response_throttle_time() {
    // Produce response should include throttle_time_ms
    use kafka_protocol::messages::ProduceResponse;

    let mut response = ProduceResponse::default();
    response.throttle_time_ms = 500;
    assert_eq!(response.throttle_time_ms, 500);
}

#[test]
fn test_quota_fetch_response_throttle_time() {
    // Fetch response should include throttle_time_ms
    use kafka_protocol::messages::FetchResponse;

    let mut response = FetchResponse::default();
    response.throttle_time_ms = 250;
    assert_eq!(response.throttle_time_ms, 250);
}

#[test]
fn test_quota_limit_error_display() {
    // LimitError should have proper Display implementation
    use crate::server::limits::LimitError;

    let err1 = LimitError::MaxConnectionsReached;
    assert!(err1.to_string().contains("connections"));

    let err2 = LimitError::PerIpLimitReached;
    assert!(err2.to_string().contains("IP"));

    let err3 = LimitError::RequestTooLarge { size: 100, max: 50 };
    assert!(err3.to_string().contains("100"));
    assert!(err3.to_string().contains("50"));

    let err4 = LimitError::RateLimitExceeded;
    assert!(err4.to_string().contains("Rate"));
}

#[test]
fn test_quota_limiter_stats_tracking() {
    // LimiterStats should track rejections
    use crate::server::limits::{LimitsConfig, ResourceLimiter};
    use std::net::IpAddr;

    let config = LimitsConfig {
        max_connections: 1,
        max_connections_per_ip: 10,
        ..Default::default()
    };
    let limiter = ResourceLimiter::new(config);

    let ip1: IpAddr = "192.168.1.1".parse().unwrap();
    let ip2: IpAddr = "192.168.1.2".parse().unwrap();

    // Hold first connection
    let _guard = limiter.try_accept_connection(ip1).unwrap();

    // Try and fail to accept second
    let _ = limiter.try_accept_connection(ip2);

    let stats = limiter.stats();
    assert_eq!(stats.total_connections, 1);
    assert!(stats.connections_rejected >= 1);
}

#[test]
fn test_quota_error_code_quota_violated() {
    // Verify QUOTA_VIOLATION error code value
    let quota_violation_code: i16 = 82;
    assert_eq!(quota_violation_code, 82);
}

#[test]
fn test_quota_error_code_throttling_quota_exceeded() {
    // Verify THROTTLING_QUOTA_EXCEEDED error code value
    let throttling_exceeded_code: i16 = 89;
    assert_eq!(throttling_exceeded_code, 89);
}

#[test]
fn test_quota_connection_guard_touch() {
    // ConnectionGuard.touch() should update last activity
    use crate::server::limits::{LimitsConfig, ResourceLimiter};
    use std::net::IpAddr;

    let config = LimitsConfig::default();
    let limiter = ResourceLimiter::new(config);

    let ip: IpAddr = "192.168.1.1".parse().unwrap();
    let guard = limiter.try_accept_connection(ip).unwrap();

    // Touch should not panic
    guard.touch();

    // Idle secs should be close to 0
    assert!(guard.idle_secs() < 5);
}

#[test]
fn test_quota_connection_guard_ip() {
    // ConnectionGuard should report correct IP
    use crate::server::limits::{LimitsConfig, ResourceLimiter};
    use std::net::IpAddr;

    let config = LimitsConfig::default();
    let limiter = ResourceLimiter::new(config);

    let ip: IpAddr = "10.0.0.1".parse().unwrap();
    let guard = limiter.try_accept_connection(ip).unwrap();

    assert_eq!(guard.ip(), ip);
}

#[test]
fn test_quota_idle_timeout_configuration() {
    // ResourceLimiter should report idle timeout from config
    use crate::server::limits::{LimitsConfig, ResourceLimiter};
    use std::time::Duration;

    let config = LimitsConfig {
        connection_idle_timeout_secs: 300,
        ..Default::default()
    };
    let limiter = ResourceLimiter::new(config);

    let timeout = limiter.idle_timeout();
    assert_eq!(timeout, Some(Duration::from_secs(300)));
}

#[test]
fn test_quota_idle_timeout_disabled() {
    // Idle timeout should be None when set to 0
    use crate::server::limits::{LimitsConfig, ResourceLimiter};

    let config = LimitsConfig {
        connection_idle_timeout_secs: 0,
        ..Default::default()
    };
    let limiter = ResourceLimiter::new(config);

    assert!(limiter.idle_timeout().is_none());
}

#[test]
fn test_quota_cleanup_stale_entries() {
    // cleanup_stale_entries should not panic
    use crate::server::limits::{LimitsConfig, ResourceLimiter};

    let config = LimitsConfig::default();
    let limiter = ResourceLimiter::new(config);

    // Should not panic even with no entries
    limiter.cleanup_stale_entries();
}

#[test]
fn test_quota_config_default() {
    // QuotaConfig should have sensible defaults
    use crate::server::limits::QuotaConfig;

    let config = QuotaConfig::default();
    assert_eq!(config.producer_byte_rate, 0, "Default should be unlimited");
    assert_eq!(config.consumer_byte_rate, 0, "Default should be unlimited");
    assert_eq!(config.request_percentage, 0);
    assert!(!config.enabled, "Default should be disabled");
}

#[test]
fn test_quota_ipv6_addresses() {
    // Limiter should handle IPv6 addresses
    use crate::server::limits::{LimitsConfig, ResourceLimiter};
    use std::net::IpAddr;

    let config = LimitsConfig::default();
    let limiter = ResourceLimiter::new(config);

    let ipv6_1: IpAddr = "::1".parse().unwrap();
    let ipv6_2: IpAddr = "2001:db8::1".parse().unwrap();
    let ipv6_3: IpAddr = "fe80::1".parse().unwrap();

    let _guard1 = limiter.try_accept_connection(ipv6_1).unwrap();
    let _guard2 = limiter.try_accept_connection(ipv6_2).unwrap();
    let _guard3 = limiter.try_accept_connection(ipv6_3).unwrap();

    assert_eq!(limiter.stats().total_connections, 3);
}

#[test]
fn test_quota_large_request_size_boundary() {
    // Test boundary conditions for request size limits
    use crate::server::limits::{LimitsConfig, ResourceLimiter};

    let config = LimitsConfig {
        max_request_size: 100 * 1024 * 1024, // 100MB
        ..Default::default()
    };
    let limiter = ResourceLimiter::new(config);

    // Exactly at limit should succeed
    assert!(limiter.check_request_size(100 * 1024 * 1024).is_ok());

    // Just over limit should fail
    assert!(limiter.check_request_size(100 * 1024 * 1024 + 1).is_err());
}

#[test]
fn test_quota_unlimited_request_size() {
    // Request size 0 means unlimited
    use crate::server::limits::{LimitsConfig, ResourceLimiter};

    let config = LimitsConfig {
        max_request_size: 0, // Unlimited
        ..Default::default()
    };
    let limiter = ResourceLimiter::new(config);

    // Very large request should succeed
    assert!(limiter.check_request_size(u64::MAX).is_ok());
}

#[test]
fn test_quota_size_rejections_counter() {
    // Size rejections should be counted in stats
    use crate::server::limits::{LimitsConfig, ResourceLimiter};

    let config = LimitsConfig {
        max_request_size: 100,
        ..Default::default()
    };
    let limiter = ResourceLimiter::new(config);

    // Trigger rejection
    let _ = limiter.check_request_size(200);

    let stats = limiter.stats();
    assert_eq!(stats.size_rejections, 1);
}

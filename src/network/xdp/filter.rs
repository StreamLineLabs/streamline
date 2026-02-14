//! XDP packet filtering for Kafka protocol

use super::{XdpAction, XdpFilterConfig};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, trace, warn};

/// XDP filter for Kafka protocol packets
///
/// This filter determines which packets should be:
/// - Redirected to AF_XDP socket (XDP_REDIRECT)
/// - Passed to kernel network stack (XDP_PASS)
/// - Dropped (XDP_DROP)
pub struct XdpFilter {
    /// Configuration
    config: XdpFilterConfig,

    /// Target port (Kafka)
    target_port: u16,

    /// Allowed source IP addresses (empty = allow all)
    allowed_sources: RwLock<Vec<IpPrefix>>,

    /// Blocked source IP addresses
    blocked_sources: RwLock<Vec<IpPrefix>>,

    /// Per-source rate limiting state
    rate_limits: RwLock<HashMap<IpAddr, RateLimitState>>,

    /// SYN flood protection state
    syn_tracker: RwLock<HashMap<IpAddr, SynTracker>>,

    /// Filter statistics
    stats: FilterStats,
}

/// IP prefix for allow/block lists
#[derive(Debug, Clone)]
pub struct IpPrefix {
    /// Base address
    pub addr: IpAddr,
    /// Prefix length (CIDR notation)
    pub prefix_len: u8,
}

impl IpPrefix {
    /// Parse from string like "192.168.1.0/24"
    pub fn parse(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.split('/').collect();
        let addr: IpAddr = parts.get(0)?.parse().ok()?;
        let prefix_len: u8 = parts
            .get(1)
            .and_then(|p| p.parse().ok())
            .unwrap_or(if addr.is_ipv4() { 32 } else { 128 });
        Some(Self { addr, prefix_len })
    }

    /// Check if an IP address matches this prefix
    pub fn matches(&self, ip: &IpAddr) -> bool {
        match (self.addr, ip) {
            (IpAddr::V4(prefix), IpAddr::V4(addr)) => {
                let prefix_bits = u32::from(*prefix);
                let addr_bits = u32::from(*addr);
                let mask = if self.prefix_len >= 32 {
                    u32::MAX
                } else {
                    u32::MAX << (32 - self.prefix_len)
                };
                (prefix_bits & mask) == (addr_bits & mask)
            }
            (IpAddr::V6(prefix), IpAddr::V6(addr)) => {
                let prefix_bits = u128::from(*prefix);
                let addr_bits = u128::from(*addr);
                let mask = if self.prefix_len >= 128 {
                    u128::MAX
                } else {
                    u128::MAX << (128 - self.prefix_len)
                };
                (prefix_bits & mask) == (addr_bits & mask)
            }
            _ => false,
        }
    }
}

/// Per-source rate limiting state
#[derive(Debug)]
struct RateLimitState {
    /// Packets in current window
    count: u64,
    /// Window start time (seconds since epoch)
    window_start: u64,
}

/// SYN flood protection state per source
#[derive(Debug)]
struct SynTracker {
    /// SYN packets in current window
    syn_count: u64,
    /// Window start time
    window_start: u64,
    /// Whether source is currently blocked
    blocked: bool,
    /// Block expiry time
    block_until: u64,
}

/// Filter statistics
#[derive(Debug, Default)]
pub struct FilterStats {
    /// Total packets processed
    pub total_packets: AtomicU64,
    /// Packets redirected to AF_XDP
    pub redirected: AtomicU64,
    /// Packets passed to kernel
    pub passed: AtomicU64,
    /// Packets dropped
    pub dropped: AtomicU64,
    /// Packets dropped due to rate limiting
    pub rate_limited: AtomicU64,
    /// Packets dropped due to SYN flood protection
    pub syn_blocked: AtomicU64,
    /// Packets from blocked sources
    pub source_blocked: AtomicU64,
    /// Non-TCP packets skipped
    pub non_tcp_skipped: AtomicU64,
    /// Non-target-port packets skipped
    pub port_mismatch: AtomicU64,
}

/// Packet metadata extracted from headers
#[derive(Debug, Clone)]
pub struct PacketMeta {
    /// Source IP address
    pub src_ip: IpAddr,
    /// Destination IP address
    pub dst_ip: IpAddr,
    /// Source port
    pub src_port: u16,
    /// Destination port
    pub dst_port: u16,
    /// IP protocol (6 = TCP, 17 = UDP)
    pub protocol: u8,
    /// TCP flags (if TCP)
    pub tcp_flags: u8,
    /// Payload offset from start of packet
    pub payload_offset: usize,
    /// Total packet length
    pub packet_len: usize,
}

impl PacketMeta {
    /// Check if this is a TCP SYN packet
    pub fn is_syn(&self) -> bool {
        self.protocol == 6 && (self.tcp_flags & 0x02) != 0 && (self.tcp_flags & 0x10) == 0
    }

    /// Check if this is a TCP packet
    pub fn is_tcp(&self) -> bool {
        self.protocol == 6
    }
}

/// Filter decision
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FilterDecision {
    /// Action to take
    pub action: XdpAction,
    /// Reason for decision
    pub reason: FilterReason,
}

/// Reason for filter decision
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterReason {
    /// Packet matches target port, redirect to AF_XDP
    TargetPort,
    /// Packet is for different port, pass to kernel
    PortMismatch,
    /// Non-TCP packet, pass to kernel
    NonTcp,
    /// Source IP is in block list
    SourceBlocked,
    /// Source IP not in allow list
    SourceNotAllowed,
    /// Rate limit exceeded
    RateLimited,
    /// SYN flood detected
    SynFlood,
    /// Invalid packet
    InvalidPacket,
}

impl XdpFilter {
    /// Create a new XDP filter
    pub fn new(config: XdpFilterConfig, target_port: u16) -> Self {
        let allowed_sources: Vec<IpPrefix> = config
            .allowed_source_prefixes
            .iter()
            .filter_map(|s| IpPrefix::parse(s))
            .collect();

        let blocked_sources: Vec<IpPrefix> = config
            .blocked_source_prefixes
            .iter()
            .filter_map(|s| IpPrefix::parse(s))
            .collect();

        Self {
            config,
            target_port,
            allowed_sources: RwLock::new(allowed_sources),
            blocked_sources: RwLock::new(blocked_sources),
            rate_limits: RwLock::new(HashMap::new()),
            syn_tracker: RwLock::new(HashMap::new()),
            stats: FilterStats::default(),
        }
    }

    /// Process a packet and return filter decision
    pub fn filter(&self, meta: &PacketMeta) -> FilterDecision {
        self.stats.total_packets.fetch_add(1, Ordering::Relaxed);

        // Check if TCP (if configured)
        if self.config.tcp_only && !meta.is_tcp() {
            self.stats.non_tcp_skipped.fetch_add(1, Ordering::Relaxed);
            return FilterDecision {
                action: XdpAction::Pass,
                reason: FilterReason::NonTcp,
            };
        }

        // Check destination port
        if meta.dst_port != self.target_port {
            self.stats.port_mismatch.fetch_add(1, Ordering::Relaxed);
            return FilterDecision {
                action: XdpAction::Pass,
                reason: FilterReason::PortMismatch,
            };
        }

        // Check blocked sources
        let blocked = self.blocked_sources.read();
        if blocked.iter().any(|p| p.matches(&meta.src_ip)) {
            self.stats.source_blocked.fetch_add(1, Ordering::Relaxed);
            return FilterDecision {
                action: XdpAction::Drop,
                reason: FilterReason::SourceBlocked,
            };
        }
        drop(blocked);

        // Check allowed sources (if configured)
        let allowed = self.allowed_sources.read();
        if !allowed.is_empty() && !allowed.iter().any(|p| p.matches(&meta.src_ip)) {
            self.stats.source_blocked.fetch_add(1, Ordering::Relaxed);
            return FilterDecision {
                action: XdpAction::Drop,
                reason: FilterReason::SourceNotAllowed,
            };
        }
        drop(allowed);

        // Check SYN flood protection
        if self.config.syn_flood_protection && meta.is_syn() {
            if self.check_syn_flood(&meta.src_ip) {
                self.stats.syn_blocked.fetch_add(1, Ordering::Relaxed);
                return FilterDecision {
                    action: XdpAction::Drop,
                    reason: FilterReason::SynFlood,
                };
            }
        }

        // Check rate limiting
        if self.config.rate_limit_enabled {
            if self.check_rate_limit(&meta.src_ip) {
                self.stats.rate_limited.fetch_add(1, Ordering::Relaxed);
                return FilterDecision {
                    action: XdpAction::Drop,
                    reason: FilterReason::RateLimited,
                };
            }
        }

        // Packet should be redirected to AF_XDP
        self.stats.redirected.fetch_add(1, Ordering::Relaxed);
        FilterDecision {
            action: XdpAction::Redirect,
            reason: FilterReason::TargetPort,
        }
    }

    /// Check rate limit for a source IP
    fn check_rate_limit(&self, src_ip: &IpAddr) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let mut limits = self.rate_limits.write();
        let state = limits.entry(*src_ip).or_insert(RateLimitState {
            count: 0,
            window_start: now,
        });

        // Reset window if needed
        if now > state.window_start {
            state.window_start = now;
            state.count = 0;
        }

        state.count += 1;

        if state.count > self.config.rate_limit_pps as u64 {
            trace!(src_ip = %src_ip, count = state.count, "Rate limit exceeded");
            return true;
        }

        false
    }

    /// Check SYN flood protection
    fn check_syn_flood(&self, src_ip: &IpAddr) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let mut tracker = self.syn_tracker.write();
        let state = tracker.entry(*src_ip).or_insert(SynTracker {
            syn_count: 0,
            window_start: now,
            blocked: false,
            block_until: 0,
        });

        // Check if blocked
        if state.blocked {
            if now >= state.block_until {
                state.blocked = false;
                state.syn_count = 0;
                state.window_start = now;
            } else {
                return true;
            }
        }

        // Reset window if needed
        if now > state.window_start {
            state.window_start = now;
            state.syn_count = 0;
        }

        state.syn_count += 1;

        if state.syn_count > self.config.syn_flood_threshold as u64 {
            warn!(src_ip = %src_ip, syn_count = state.syn_count, "SYN flood detected");
            state.blocked = true;
            state.block_until = now + 60; // Block for 60 seconds
            return true;
        }

        false
    }

    /// Add an IP prefix to the allow list
    pub fn add_allowed_source(&self, prefix: &str) -> bool {
        if let Some(p) = IpPrefix::parse(prefix) {
            self.allowed_sources.write().push(p);
            debug!(prefix = prefix, "Added allowed source prefix");
            true
        } else {
            warn!(prefix = prefix, "Invalid prefix format");
            false
        }
    }

    /// Add an IP prefix to the block list
    pub fn add_blocked_source(&self, prefix: &str) -> bool {
        if let Some(p) = IpPrefix::parse(prefix) {
            self.blocked_sources.write().push(p);
            debug!(prefix = prefix, "Added blocked source prefix");
            true
        } else {
            warn!(prefix = prefix, "Invalid prefix format");
            false
        }
    }

    /// Remove an IP from the block list
    pub fn remove_blocked_source(&self, prefix: &str) -> bool {
        if let Some(target) = IpPrefix::parse(prefix) {
            let mut blocked = self.blocked_sources.write();
            let initial_len = blocked.len();
            blocked.retain(|p| !(p.addr == target.addr && p.prefix_len == target.prefix_len));
            initial_len != blocked.len()
        } else {
            false
        }
    }

    /// Get filter statistics
    pub fn stats(&self) -> FilterStatsSnapshot {
        FilterStatsSnapshot {
            total_packets: self.stats.total_packets.load(Ordering::Relaxed),
            redirected: self.stats.redirected.load(Ordering::Relaxed),
            passed: self.stats.passed.load(Ordering::Relaxed),
            dropped: self.stats.dropped.load(Ordering::Relaxed),
            rate_limited: self.stats.rate_limited.load(Ordering::Relaxed),
            syn_blocked: self.stats.syn_blocked.load(Ordering::Relaxed),
            source_blocked: self.stats.source_blocked.load(Ordering::Relaxed),
            non_tcp_skipped: self.stats.non_tcp_skipped.load(Ordering::Relaxed),
            port_mismatch: self.stats.port_mismatch.load(Ordering::Relaxed),
        }
    }

    /// Get target port
    pub fn target_port(&self) -> u16 {
        self.target_port
    }

    /// Cleanup expired rate limit entries
    pub fn cleanup(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // Clean rate limits older than 10 seconds
        self.rate_limits
            .write()
            .retain(|_, state| now.saturating_sub(state.window_start) < 10);

        // Clean syn tracker entries that are no longer blocked
        self.syn_tracker
            .write()
            .retain(|_, state| !state.blocked || now < state.block_until);
    }
}

/// Snapshot of filter statistics
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct FilterStatsSnapshot {
    pub total_packets: u64,
    pub redirected: u64,
    pub passed: u64,
    pub dropped: u64,
    pub rate_limited: u64,
    pub syn_blocked: u64,
    pub source_blocked: u64,
    pub non_tcp_skipped: u64,
    pub port_mismatch: u64,
}

/// Parse packet headers to extract metadata
///
/// This is a simplified parser - in production XDP, this would
/// be done in the BPF program in the kernel.
pub fn parse_packet(data: &[u8]) -> Option<PacketMeta> {
    if data.len() < 14 {
        return None; // Too small for Ethernet header
    }

    // Check EtherType
    let ethertype = u16::from_be_bytes([data[12], data[13]]);

    let (ip_start, is_ipv6) = match ethertype {
        0x0800 => (14, false), // IPv4
        0x86DD => (14, true),  // IPv6
        _ => return None,      // Not IP
    };

    if is_ipv6 {
        parse_ipv6_packet(data, ip_start)
    } else {
        parse_ipv4_packet(data, ip_start)
    }
}

fn parse_ipv4_packet(data: &[u8], ip_start: usize) -> Option<PacketMeta> {
    if data.len() < ip_start + 20 {
        return None; // Too small for IPv4 header
    }

    let ihl = (data[ip_start] & 0x0F) as usize * 4;
    let protocol = data[ip_start + 9];
    let src_ip = IpAddr::V4(Ipv4Addr::new(
        data[ip_start + 12],
        data[ip_start + 13],
        data[ip_start + 14],
        data[ip_start + 15],
    ));
    let dst_ip = IpAddr::V4(Ipv4Addr::new(
        data[ip_start + 16],
        data[ip_start + 17],
        data[ip_start + 18],
        data[ip_start + 19],
    ));

    let tcp_start = ip_start + ihl;
    if protocol == 6 && data.len() >= tcp_start + 14 {
        // TCP
        let src_port = u16::from_be_bytes([data[tcp_start], data[tcp_start + 1]]);
        let dst_port = u16::from_be_bytes([data[tcp_start + 2], data[tcp_start + 3]]);
        let tcp_flags = data[tcp_start + 13];
        let tcp_header_len = ((data[tcp_start + 12] >> 4) as usize) * 4;
        let payload_offset = tcp_start + tcp_header_len;

        Some(PacketMeta {
            src_ip,
            dst_ip,
            src_port,
            dst_port,
            protocol,
            tcp_flags,
            payload_offset,
            packet_len: data.len(),
        })
    } else {
        Some(PacketMeta {
            src_ip,
            dst_ip,
            src_port: 0,
            dst_port: 0,
            protocol,
            tcp_flags: 0,
            payload_offset: ip_start + ihl,
            packet_len: data.len(),
        })
    }
}

fn parse_ipv6_packet(data: &[u8], ip_start: usize) -> Option<PacketMeta> {
    if data.len() < ip_start + 40 {
        return None; // Too small for IPv6 header
    }

    let protocol = data[ip_start + 6]; // Next Header
    let src_bytes: [u8; 16] = data[ip_start + 8..ip_start + 24].try_into().ok()?;
    let dst_bytes: [u8; 16] = data[ip_start + 24..ip_start + 40].try_into().ok()?;
    let src_ip = IpAddr::V6(Ipv6Addr::from(src_bytes));
    let dst_ip = IpAddr::V6(Ipv6Addr::from(dst_bytes));

    let tcp_start = ip_start + 40;
    if protocol == 6 && data.len() >= tcp_start + 14 {
        // TCP
        let src_port = u16::from_be_bytes([data[tcp_start], data[tcp_start + 1]]);
        let dst_port = u16::from_be_bytes([data[tcp_start + 2], data[tcp_start + 3]]);
        let tcp_flags = data[tcp_start + 13];
        let tcp_header_len = ((data[tcp_start + 12] >> 4) as usize) * 4;
        let payload_offset = tcp_start + tcp_header_len;

        Some(PacketMeta {
            src_ip,
            dst_ip,
            src_port,
            dst_port,
            protocol,
            tcp_flags,
            payload_offset,
            packet_len: data.len(),
        })
    } else {
        Some(PacketMeta {
            src_ip,
            dst_ip,
            src_port: 0,
            dst_port: 0,
            protocol,
            tcp_flags: 0,
            payload_offset: ip_start + 40,
            packet_len: data.len(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ip_prefix_parse() {
        let prefix = IpPrefix::parse("192.168.1.0/24").unwrap();
        assert!(matches!(prefix.addr, IpAddr::V4(_)));
        assert_eq!(prefix.prefix_len, 24);

        let prefix = IpPrefix::parse("10.0.0.1").unwrap();
        assert_eq!(prefix.prefix_len, 32);

        assert!(IpPrefix::parse("invalid").is_none());
    }

    #[test]
    fn test_ip_prefix_matches() {
        let prefix = IpPrefix::parse("192.168.1.0/24").unwrap();

        assert!(prefix.matches(&IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100))));
        assert!(prefix.matches(&IpAddr::V4(Ipv4Addr::new(192, 168, 1, 0))));
        assert!(prefix.matches(&IpAddr::V4(Ipv4Addr::new(192, 168, 1, 255))));
        assert!(!prefix.matches(&IpAddr::V4(Ipv4Addr::new(192, 168, 2, 1))));
        assert!(!prefix.matches(&IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))));
    }

    #[test]
    fn test_filter_target_port() {
        let config = XdpFilterConfig::default();
        let filter = XdpFilter::new(config, 9092);

        let meta = PacketMeta {
            src_ip: IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)),
            dst_ip: IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
            src_port: 50000,
            dst_port: 9092,
            protocol: 6,
            tcp_flags: 0,
            payload_offset: 54,
            packet_len: 100,
        };

        let decision = filter.filter(&meta);
        assert_eq!(decision.action, XdpAction::Redirect);
        assert_eq!(decision.reason, FilterReason::TargetPort);
    }

    #[test]
    fn test_filter_port_mismatch() {
        let config = XdpFilterConfig::default();
        let filter = XdpFilter::new(config, 9092);

        let meta = PacketMeta {
            src_ip: IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)),
            dst_ip: IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
            src_port: 50000,
            dst_port: 80, // Wrong port
            protocol: 6,
            tcp_flags: 0,
            payload_offset: 54,
            packet_len: 100,
        };

        let decision = filter.filter(&meta);
        assert_eq!(decision.action, XdpAction::Pass);
        assert_eq!(decision.reason, FilterReason::PortMismatch);
    }

    #[test]
    fn test_filter_blocked_source() {
        let config = XdpFilterConfig {
            blocked_source_prefixes: vec!["10.0.0.0/8".to_string()],
            ..Default::default()
        };
        let filter = XdpFilter::new(config, 9092);

        let meta = PacketMeta {
            src_ip: IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3)),
            dst_ip: IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
            src_port: 50000,
            dst_port: 9092,
            protocol: 6,
            tcp_flags: 0,
            payload_offset: 54,
            packet_len: 100,
        };

        let decision = filter.filter(&meta);
        assert_eq!(decision.action, XdpAction::Drop);
        assert_eq!(decision.reason, FilterReason::SourceBlocked);
    }

    #[test]
    fn test_filter_rate_limiting() {
        let config = XdpFilterConfig {
            rate_limit_enabled: true,
            rate_limit_pps: 2, // Very low for testing
            ..Default::default()
        };
        let filter = XdpFilter::new(config, 9092);

        let meta = PacketMeta {
            src_ip: IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)),
            dst_ip: IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
            src_port: 50000,
            dst_port: 9092,
            protocol: 6,
            tcp_flags: 0,
            payload_offset: 54,
            packet_len: 100,
        };

        // First two should pass
        assert_eq!(filter.filter(&meta).action, XdpAction::Redirect);
        assert_eq!(filter.filter(&meta).action, XdpAction::Redirect);

        // Third should be rate limited
        let decision = filter.filter(&meta);
        assert_eq!(decision.action, XdpAction::Drop);
        assert_eq!(decision.reason, FilterReason::RateLimited);
    }

    #[test]
    fn test_packet_meta_is_syn() {
        let syn_meta = PacketMeta {
            src_ip: IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)),
            dst_ip: IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
            src_port: 50000,
            dst_port: 9092,
            protocol: 6,
            tcp_flags: 0x02, // SYN
            payload_offset: 54,
            packet_len: 100,
        };
        assert!(syn_meta.is_syn());

        let synack_meta = PacketMeta {
            tcp_flags: 0x12, // SYN+ACK
            ..syn_meta.clone()
        };
        assert!(!synack_meta.is_syn());

        let ack_meta = PacketMeta {
            tcp_flags: 0x10, // ACK only
            ..syn_meta.clone()
        };
        assert!(!ack_meta.is_syn());
    }

    #[test]
    fn test_filter_stats() {
        let config = XdpFilterConfig::default();
        let filter = XdpFilter::new(config, 9092);

        let meta = PacketMeta {
            src_ip: IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)),
            dst_ip: IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
            src_port: 50000,
            dst_port: 9092,
            protocol: 6,
            tcp_flags: 0,
            payload_offset: 54,
            packet_len: 100,
        };

        filter.filter(&meta);
        filter.filter(&meta);

        let stats = filter.stats();
        assert_eq!(stats.total_packets, 2);
        assert_eq!(stats.redirected, 2);
    }
}

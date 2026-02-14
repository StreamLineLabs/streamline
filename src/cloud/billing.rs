//! Billing and usage metering for Streamline Cloud

use crate::error::{Result, StreamlineError};
use crate::multitenancy::TenantTier;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Billing event types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BillingEvent {
    /// Messages produced
    MessagesProduced {
        tenant_id: String,
        cluster_id: String,
        count: u64,
        bytes: u64,
    },
    /// Messages consumed
    MessagesConsumed {
        tenant_id: String,
        cluster_id: String,
        count: u64,
        bytes: u64,
    },
    /// Compute time (in seconds)
    ComputeTime {
        tenant_id: String,
        cluster_id: String,
        vcpu_seconds: u64,
    },
    /// Storage used (in bytes)
    StorageUsed {
        tenant_id: String,
        cluster_id: String,
        bytes: u64,
    },
    /// Network egress (in bytes)
    NetworkEgress { tenant_id: String, bytes: u64 },
}

/// Billing plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BillingPlan {
    /// Plan ID
    pub id: String,
    /// Plan name
    pub name: String,
    /// Monthly base price in cents
    pub base_price_cents: u64,
    /// Price per million messages (produce)
    pub per_million_produce_cents: u64,
    /// Price per million messages (consume)
    pub per_million_consume_cents: u64,
    /// Price per GB storage per month
    pub per_gb_storage_cents: u64,
    /// Price per GB network egress
    pub per_gb_egress_cents: u64,
    /// Free tier allowance (messages)
    pub free_messages: u64,
    /// Free tier allowance (storage GB)
    pub free_storage_gb: u64,
}

impl BillingPlan {
    /// Free tier plan
    pub fn free() -> Self {
        Self {
            id: "free".to_string(),
            name: "Free".to_string(),
            base_price_cents: 0,
            per_million_produce_cents: 0,
            per_million_consume_cents: 0,
            per_gb_storage_cents: 0,
            per_gb_egress_cents: 0,
            free_messages: 1_000_000, // 1M messages/month
            free_storage_gb: 1,       // 1 GB
        }
    }

    /// Basic tier plan
    pub fn basic() -> Self {
        Self {
            id: "basic".to_string(),
            name: "Basic".to_string(),
            base_price_cents: 2500,        // $25/month
            per_million_produce_cents: 10, // $0.10 per million
            per_million_consume_cents: 5,  // $0.05 per million
            per_gb_storage_cents: 25,      // $0.25/GB/month
            per_gb_egress_cents: 10,       // $0.10/GB
            free_messages: 10_000_000,     // 10M messages
            free_storage_gb: 10,
        }
    }

    /// Professional tier plan
    pub fn professional() -> Self {
        Self {
            id: "professional".to_string(),
            name: "Professional".to_string(),
            base_price_cents: 9900, // $99/month
            per_million_produce_cents: 5,
            per_million_consume_cents: 2,
            per_gb_storage_cents: 15,
            per_gb_egress_cents: 8,
            free_messages: 100_000_000,
            free_storage_gb: 100,
        }
    }

    /// Enterprise tier plan
    pub fn enterprise() -> Self {
        Self {
            id: "enterprise".to_string(),
            name: "Enterprise".to_string(),
            base_price_cents: 49900, // $499/month
            per_million_produce_cents: 2,
            per_million_consume_cents: 1,
            per_gb_storage_cents: 10,
            per_gb_egress_cents: 5,
            free_messages: 1_000_000_000,
            free_storage_gb: 1000,
        }
    }

    /// Get plan for tenant tier
    pub fn for_tier(tier: &TenantTier) -> Self {
        match tier {
            TenantTier::Free => Self::free(),
            TenantTier::Basic => Self::basic(),
            TenantTier::Professional => Self::professional(),
            TenantTier::Enterprise => Self::enterprise(),
            TenantTier::Custom => Self::enterprise(),
        }
    }
}

/// Usage summary for a billing period
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UsageSummary {
    /// Tenant ID
    pub tenant_id: String,
    /// Billing period start
    pub period_start: i64,
    /// Billing period end
    pub period_end: i64,
    /// Total messages produced
    pub messages_produced: u64,
    /// Total messages consumed
    pub messages_consumed: u64,
    /// Total bytes produced
    pub bytes_produced: u64,
    /// Total bytes consumed
    pub bytes_consumed: u64,
    /// Peak storage bytes
    pub peak_storage_bytes: u64,
    /// Network egress bytes
    pub network_egress_bytes: u64,
    /// Compute seconds
    pub compute_seconds: u64,
    /// Active clusters
    pub active_clusters: Vec<String>,
}

impl UsageSummary {
    /// Calculate total cost in cents based on plan
    pub fn calculate_cost(&self, plan: &BillingPlan) -> u64 {
        let mut total = plan.base_price_cents;

        // Messages (after free tier)
        let billable_produce = self.messages_produced.saturating_sub(plan.free_messages);
        total += (billable_produce / 1_000_000) * plan.per_million_produce_cents;

        let billable_consume = self.messages_consumed.saturating_sub(plan.free_messages);
        total += (billable_consume / 1_000_000) * plan.per_million_consume_cents;

        // Storage (after free tier)
        let storage_gb = self.peak_storage_bytes / (1024 * 1024 * 1024);
        let billable_storage = storage_gb.saturating_sub(plan.free_storage_gb);
        total += billable_storage * plan.per_gb_storage_cents;

        // Network egress
        let egress_gb = self.network_egress_bytes / (1024 * 1024 * 1024);
        total += egress_gb * plan.per_gb_egress_cents;

        total
    }
}

/// Invoice for a billing period
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Invoice {
    /// Invoice ID
    pub id: String,
    /// Tenant ID
    pub tenant_id: String,
    /// Billing period start
    pub period_start: i64,
    /// Billing period end
    pub period_end: i64,
    /// Plan used
    pub plan: BillingPlan,
    /// Usage summary
    pub usage: UsageSummary,
    /// Line items
    pub line_items: Vec<LineItem>,
    /// Subtotal in cents
    pub subtotal_cents: u64,
    /// Tax in cents
    pub tax_cents: u64,
    /// Total in cents
    pub total_cents: u64,
    /// Status
    pub status: InvoiceStatus,
    /// Created timestamp
    pub created_at: i64,
}

/// Invoice line item
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineItem {
    /// Description
    pub description: String,
    /// Quantity
    pub quantity: u64,
    /// Unit
    pub unit: String,
    /// Unit price in cents
    pub unit_price_cents: u64,
    /// Total in cents
    pub total_cents: u64,
}

/// Invoice status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum InvoiceStatus {
    /// Draft (current period)
    #[default]
    Draft,
    /// Pending payment
    Pending,
    /// Paid
    Paid,
    /// Overdue
    Overdue,
    /// Void
    Void,
}

/// Billing account
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BillingAccount {
    /// Tenant ID
    pub tenant_id: String,
    /// Current plan
    pub plan: BillingPlan,
    /// Current period usage
    pub current_usage: UsageSummary,
    /// Active metering sessions
    pub metering_sessions: HashMap<String, MeteringSession>,
    /// Created timestamp
    pub created_at: i64,
}

/// Active metering session for a cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeteringSession {
    /// Cluster ID
    pub cluster_id: String,
    /// Started timestamp
    pub started_at: i64,
    /// Messages produced in session
    pub messages_produced: u64,
    /// Messages consumed in session
    pub messages_consumed: u64,
    /// Bytes produced
    pub bytes_produced: u64,
    /// Bytes consumed
    pub bytes_consumed: u64,
}

/// Billing manager
pub struct BillingManager {
    accounts: Arc<RwLock<HashMap<String, BillingAccount>>>,
    #[allow(dead_code)]
    invoices: Arc<RwLock<Vec<Invoice>>>,
}

impl BillingManager {
    /// Create a new billing manager
    pub fn new() -> Result<Self> {
        Ok(Self {
            accounts: Arc::new(RwLock::new(HashMap::new())),
            invoices: Arc::new(RwLock::new(Vec::new())),
        })
    }

    /// Create a billing account for a tenant
    pub async fn create_account(&self, tenant_id: &str, tier: &TenantTier) -> Result<()> {
        let mut accounts = self.accounts.write().await;

        if accounts.contains_key(tenant_id) {
            return Err(StreamlineError::Config(format!(
                "Billing account already exists for tenant: {}",
                tenant_id
            )));
        }

        let now = chrono::Utc::now().timestamp_millis();
        let plan = BillingPlan::for_tier(tier);

        let account = BillingAccount {
            tenant_id: tenant_id.to_string(),
            plan,
            current_usage: UsageSummary {
                tenant_id: tenant_id.to_string(),
                period_start: now,
                period_end: now + 30 * 24 * 60 * 60 * 1000, // 30 days
                ..Default::default()
            },
            metering_sessions: HashMap::new(),
            created_at: now,
        };

        accounts.insert(tenant_id.to_string(), account);
        Ok(())
    }

    /// Start metering for a cluster
    pub async fn start_metering(&self, tenant_id: &str, cluster_id: &str) -> Result<()> {
        let mut accounts = self.accounts.write().await;

        let account = accounts.get_mut(tenant_id).ok_or_else(|| {
            StreamlineError::Config(format!("No billing account for tenant: {}", tenant_id))
        })?;

        let session = MeteringSession {
            cluster_id: cluster_id.to_string(),
            started_at: chrono::Utc::now().timestamp_millis(),
            messages_produced: 0,
            messages_consumed: 0,
            bytes_produced: 0,
            bytes_consumed: 0,
        };

        account
            .metering_sessions
            .insert(cluster_id.to_string(), session);
        account
            .current_usage
            .active_clusters
            .push(cluster_id.to_string());

        Ok(())
    }

    /// Stop metering for a cluster
    pub async fn stop_metering(&self, tenant_id: &str, cluster_id: &str) -> Result<()> {
        let mut accounts = self.accounts.write().await;

        let account = accounts.get_mut(tenant_id).ok_or_else(|| {
            StreamlineError::Config(format!("No billing account for tenant: {}", tenant_id))
        })?;

        // Roll up session usage to period usage
        if let Some(session) = account.metering_sessions.remove(cluster_id) {
            account.current_usage.messages_produced += session.messages_produced;
            account.current_usage.messages_consumed += session.messages_consumed;
            account.current_usage.bytes_produced += session.bytes_produced;
            account.current_usage.bytes_consumed += session.bytes_consumed;
        }

        account
            .current_usage
            .active_clusters
            .retain(|c| c != cluster_id);

        Ok(())
    }

    /// Record a billing event
    pub async fn record_event(&self, event: BillingEvent) -> Result<()> {
        let tenant_id = match &event {
            BillingEvent::MessagesProduced { tenant_id, .. } => tenant_id,
            BillingEvent::MessagesConsumed { tenant_id, .. } => tenant_id,
            BillingEvent::ComputeTime { tenant_id, .. } => tenant_id,
            BillingEvent::StorageUsed { tenant_id, .. } => tenant_id,
            BillingEvent::NetworkEgress { tenant_id, .. } => tenant_id,
        };

        let mut accounts = self.accounts.write().await;

        let account = accounts.get_mut(tenant_id).ok_or_else(|| {
            StreamlineError::Config(format!("No billing account for tenant: {}", tenant_id))
        })?;

        match event {
            BillingEvent::MessagesProduced {
                cluster_id,
                count,
                bytes,
                ..
            } => {
                if let Some(session) = account.metering_sessions.get_mut(&cluster_id) {
                    session.messages_produced += count;
                    session.bytes_produced += bytes;
                }
            }
            BillingEvent::MessagesConsumed {
                cluster_id,
                count,
                bytes,
                ..
            } => {
                if let Some(session) = account.metering_sessions.get_mut(&cluster_id) {
                    session.messages_consumed += count;
                    session.bytes_consumed += bytes;
                }
            }
            BillingEvent::ComputeTime { vcpu_seconds, .. } => {
                account.current_usage.compute_seconds += vcpu_seconds;
            }
            BillingEvent::StorageUsed { bytes, .. } => {
                account.current_usage.peak_storage_bytes =
                    account.current_usage.peak_storage_bytes.max(bytes);
            }
            BillingEvent::NetworkEgress { bytes, .. } => {
                account.current_usage.network_egress_bytes += bytes;
            }
        }

        Ok(())
    }

    /// Get usage summary for a tenant
    pub async fn get_usage(&self, tenant_id: &str) -> Result<UsageSummary> {
        let accounts = self.accounts.read().await;

        let account = accounts.get(tenant_id).ok_or_else(|| {
            StreamlineError::Config(format!("No billing account for tenant: {}", tenant_id))
        })?;

        let mut usage = account.current_usage.clone();

        // Add active session usage
        for session in account.metering_sessions.values() {
            usage.messages_produced += session.messages_produced;
            usage.messages_consumed += session.messages_consumed;
            usage.bytes_produced += session.bytes_produced;
            usage.bytes_consumed += session.bytes_consumed;
        }

        Ok(usage)
    }

    /// Get current invoice for a tenant
    pub async fn get_current_invoice(&self, tenant_id: &str) -> Result<Invoice> {
        let accounts = self.accounts.read().await;

        let account = accounts.get(tenant_id).ok_or_else(|| {
            StreamlineError::Config(format!("No billing account for tenant: {}", tenant_id))
        })?;

        let usage = self.get_usage(tenant_id).await?;
        let cost = usage.calculate_cost(&account.plan);

        // Build line items
        let mut line_items = Vec::new();

        // Base plan
        line_items.push(LineItem {
            description: format!("{} Plan", account.plan.name),
            quantity: 1,
            unit: "month".to_string(),
            unit_price_cents: account.plan.base_price_cents,
            total_cents: account.plan.base_price_cents,
        });

        // Messages produced
        if usage.messages_produced > account.plan.free_messages {
            let billable = usage.messages_produced - account.plan.free_messages;
            let millions = billable / 1_000_000;
            line_items.push(LineItem {
                description: "Messages Produced".to_string(),
                quantity: millions,
                unit: "million".to_string(),
                unit_price_cents: account.plan.per_million_produce_cents,
                total_cents: millions * account.plan.per_million_produce_cents,
            });
        }

        let invoice = Invoice {
            id: format!("inv_{}", uuid::Uuid::new_v4()),
            tenant_id: tenant_id.to_string(),
            period_start: account.current_usage.period_start,
            period_end: account.current_usage.period_end,
            plan: account.plan.clone(),
            usage,
            line_items,
            subtotal_cents: cost,
            tax_cents: 0, // Tax calculation would be more complex
            total_cents: cost,
            status: InvoiceStatus::Draft,
            created_at: chrono::Utc::now().timestamp_millis(),
        };

        Ok(invoice)
    }
}

impl Default for BillingManager {
    fn default() -> Self {
        Self {
            accounts: Arc::new(RwLock::new(HashMap::new())),
            invoices: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_billing_plan_for_tier() {
        let free = BillingPlan::for_tier(&TenantTier::Free);
        assert_eq!(free.base_price_cents, 0);
        assert_eq!(free.free_messages, 1_000_000);

        let pro = BillingPlan::for_tier(&TenantTier::Professional);
        assert_eq!(pro.base_price_cents, 9900);
    }

    #[test]
    fn test_usage_cost_calculation() {
        let plan = BillingPlan::basic();
        let usage = UsageSummary {
            tenant_id: "test".to_string(),
            messages_produced: 50_000_000,  // 50M (40M billable)
            messages_consumed: 100_000_000, // 100M (90M billable)
            peak_storage_bytes: 50 * 1024 * 1024 * 1024, // 50GB (40GB billable)
            network_egress_bytes: 10 * 1024 * 1024 * 1024, // 10GB
            ..Default::default()
        };

        let cost = usage.calculate_cost(&plan);
        // Base: 2500 + Produce: 40*10=400 + Consume: 90*5=450 + Storage: 40*25=1000 + Egress: 10*10=100
        assert_eq!(cost, 2500 + 400 + 450 + 1000 + 100);
    }

    #[tokio::test]
    async fn test_billing_manager() {
        let manager = BillingManager::new().unwrap();

        // Create account
        manager
            .create_account("tenant1", &TenantTier::Basic)
            .await
            .unwrap();

        // Start metering
        manager.start_metering("tenant1", "cluster1").await.unwrap();

        // Record events
        manager
            .record_event(BillingEvent::MessagesProduced {
                tenant_id: "tenant1".to_string(),
                cluster_id: "cluster1".to_string(),
                count: 1000,
                bytes: 100000,
            })
            .await
            .unwrap();

        // Get usage
        let usage = manager.get_usage("tenant1").await.unwrap();
        assert_eq!(usage.messages_produced, 1000);
    }
}

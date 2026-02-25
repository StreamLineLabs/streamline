//! PostgreSQL-backed CDC offset storage
//!
//! Provides durable, crash-safe offset tracking for CDC pipelines using
//! PostgreSQL as the backend store. This replaces the file-backed approach
//! for production deployments where PostgreSQL is already available.
//!
//! ## Schema
//!
//! The store auto-creates its table on first use:
//! ```sql
//! CREATE TABLE IF NOT EXISTS cdc_offsets (
//!     source_name TEXT NOT NULL,
//!     table_name  TEXT NOT NULL,
//!     position    TEXT NOT NULL,
//!     sequence    BIGINT NOT NULL,
//!     committed_at TIMESTAMPTZ NOT NULL,
//!     transaction_id TEXT,
//!     PRIMARY KEY (source_name, table_name)
//! );
//! ```
//!
//! ## Usage
//!
//! ```rust,ignore
//! let store = PostgresOffsetStore::new("postgres://user:pass@localhost/streamline").await?;
//! store.ensure_schema().await?;
//!
//! // Save offset
//! store.save_position("my-source", "users", &pos).await?;
//!
//! // Load all offsets for a source
//! let positions = store.load_positions("my-source").await?;
//! ```

use crate::cdc::delivery::CommittedPosition;
use crate::error::{Result, StreamlineError};
use chrono::Utc;
use std::collections::HashMap;
use tokio_postgres::{Client, NoTls};
use tracing::{debug, info, warn};

const CREATE_TABLE_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS cdc_offsets (
        source_name TEXT NOT NULL,
        table_name  TEXT NOT NULL,
        position    TEXT NOT NULL,
        sequence    BIGINT NOT NULL,
        committed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        transaction_id TEXT,
        PRIMARY KEY (source_name, table_name)
    )
"#;

/// PostgreSQL-backed offset store for CDC delivery tracking
pub struct PostgresOffsetStore {
    client: Client,
}

impl PostgresOffsetStore {
    /// Connect to PostgreSQL and create the offset store
    pub async fn new(connection_string: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(connection_string, NoTls)
            .await
            .map_err(|e| {
                StreamlineError::storage_msg(format!(
                    "Failed to connect to PostgreSQL for CDC offsets: {}",
                    e
                ))
            })?;

        // Spawn the connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                warn!("CDC offset store PostgreSQL connection error: {}", e);
            }
        });

        info!("CDC offset store connected to PostgreSQL");
        Ok(Self { client })
    }

    /// Create the offsets table if it doesn't exist
    pub async fn ensure_schema(&self) -> Result<()> {
        self.client
            .execute(CREATE_TABLE_SQL, &[])
            .await
            .map_err(|e| {
                StreamlineError::storage_msg(format!(
                    "Failed to create CDC offsets table: {}",
                    e
                ))
            })?;
        debug!("CDC offsets table ensured");
        Ok(())
    }

    /// Save or update a committed position for a source/table pair
    pub async fn save_position(
        &self,
        source: &str,
        table: &str,
        position: &CommittedPosition,
    ) -> Result<()> {
        self.client
            .execute(
                r#"
                INSERT INTO cdc_offsets (source_name, table_name, position, sequence, committed_at, transaction_id)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (source_name, table_name)
                DO UPDATE SET
                    position = EXCLUDED.position,
                    sequence = EXCLUDED.sequence,
                    committed_at = EXCLUDED.committed_at,
                    transaction_id = EXCLUDED.transaction_id
                "#,
                &[
                    &source,
                    &table,
                    &position.position,
                    &(position.sequence as i64),
                    &position.committed_at,
                    &position.transaction_id,
                ],
            )
            .await
            .map_err(|e| {
                StreamlineError::storage_msg(format!(
                    "Failed to save CDC offset for {}/{}: {}",
                    source, table, e
                ))
            })?;

        debug!(source, table, position = %position.position, "CDC offset saved to PostgreSQL");
        Ok(())
    }

    /// Load all committed positions for a source
    pub async fn load_positions(
        &self,
        source: &str,
    ) -> Result<HashMap<String, CommittedPosition>> {
        let rows = self
            .client
            .query(
                "SELECT table_name, position, sequence, committed_at, transaction_id FROM cdc_offsets WHERE source_name = $1",
                &[&source],
            )
            .await
            .map_err(|e| {
                StreamlineError::storage_msg(format!(
                    "Failed to load CDC offsets for source '{}': {}",
                    source, e
                ))
            })?;

        let mut positions = HashMap::new();
        for row in rows {
            let table_name: String = row.get(0);
            let position: String = row.get(1);
            let sequence: i64 = row.get(2);
            let committed_at: chrono::DateTime<Utc> = row.get(3);
            let transaction_id: Option<String> = row.get(4);

            positions.insert(
                table_name,
                CommittedPosition {
                    position,
                    sequence: sequence as u64,
                    committed_at,
                    transaction_id,
                },
            );
        }

        debug!(source, count = positions.len(), "CDC offsets loaded from PostgreSQL");
        Ok(positions)
    }

    /// Load all positions for all sources (full checkpoint restore)
    pub async fn load_all(
        &self,
    ) -> Result<HashMap<String, HashMap<String, CommittedPosition>>> {
        let rows = self
            .client
            .query(
                "SELECT source_name, table_name, position, sequence, committed_at, transaction_id FROM cdc_offsets ORDER BY source_name, table_name",
                &[],
            )
            .await
            .map_err(|e| {
                StreamlineError::storage_msg(format!(
                    "Failed to load all CDC offsets: {}",
                    e
                ))
            })?;

        let mut all_positions: HashMap<String, HashMap<String, CommittedPosition>> = HashMap::new();
        for row in rows {
            let source_name: String = row.get(0);
            let table_name: String = row.get(1);
            let position: String = row.get(2);
            let sequence: i64 = row.get(3);
            let committed_at: chrono::DateTime<Utc> = row.get(4);
            let transaction_id: Option<String> = row.get(5);

            all_positions
                .entry(source_name)
                .or_default()
                .insert(
                    table_name,
                    CommittedPosition {
                        position,
                        sequence: sequence as u64,
                        committed_at,
                        transaction_id,
                    },
                );
        }

        info!(
            sources = all_positions.len(),
            total = all_positions.values().map(|m| m.len()).sum::<usize>(),
            "All CDC offsets loaded from PostgreSQL"
        );
        Ok(all_positions)
    }

    /// Delete all offsets for a source (used when removing a CDC source)
    pub async fn delete_source(&self, source: &str) -> Result<u64> {
        let count = self
            .client
            .execute(
                "DELETE FROM cdc_offsets WHERE source_name = $1",
                &[&source],
            )
            .await
            .map_err(|e| {
                StreamlineError::storage_msg(format!(
                    "Failed to delete CDC offsets for source '{}': {}",
                    source, e
                ))
            })?;

        info!(source, deleted = count, "CDC offsets deleted from PostgreSQL");
        Ok(count)
    }

    /// Delete a specific offset for a source/table pair
    pub async fn delete_position(&self, source: &str, table: &str) -> Result<bool> {
        let count = self
            .client
            .execute(
                "DELETE FROM cdc_offsets WHERE source_name = $1 AND table_name = $2",
                &[&source, &table],
            )
            .await
            .map_err(|e| {
                StreamlineError::storage_msg(format!(
                    "Failed to delete CDC offset for {}/{}: {}",
                    source, table, e
                ))
            })?;
        Ok(count > 0)
    }

    /// Get count of tracked offsets (for monitoring)
    pub async fn count(&self) -> Result<i64> {
        let row = self
            .client
            .query_one("SELECT COUNT(*) FROM cdc_offsets", &[])
            .await
            .map_err(|e| {
                StreamlineError::storage_msg(format!(
                    "Failed to count CDC offsets: {}",
                    e
                ))
            })?;
        Ok(row.get(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_table_sql_is_valid() {
        // Verify the SQL string is not empty and contains expected keywords
        assert!(CREATE_TABLE_SQL.contains("CREATE TABLE IF NOT EXISTS"));
        assert!(CREATE_TABLE_SQL.contains("source_name"));
        assert!(CREATE_TABLE_SQL.contains("PRIMARY KEY"));
    }

    #[test]
    fn test_committed_position_serialization() {
        let pos = CommittedPosition {
            position: "0/1234ABCD".to_string(),
            sequence: 42,
            committed_at: Utc::now(),
            transaction_id: Some("txn-1".to_string()),
        };
        let json = serde_json::to_string(&pos).unwrap();
        let restored: CommittedPosition = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.position, "0/1234ABCD");
        assert_eq!(restored.sequence, 42);
        assert_eq!(restored.transaction_id, Some("txn-1".to_string()));
    }
}

//! Kafka Connect Connector Conformance Matrix
//!
//! Tracks compatibility status of popular Kafka Connect connectors
//! with the Streamline Connect runtime.

use serde::{Deserialize, Serialize};

/// Compatibility status for a connector
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompatibilityStatus {
    /// Fully verified and tested
    Verified,
    /// Known to work but not fully tested
    Compatible,
    /// Partially working with known limitations
    Partial,
    /// Not yet tested
    Untested,
    /// Known incompatible
    Incompatible,
}

/// A connector entry in the compatibility matrix
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorEntry {
    pub name: String,
    pub class_name: String,
    pub connector_type: String,
    pub vendor: String,
    pub status: CompatibilityStatus,
    pub version_tested: Option<String>,
    pub notes: Option<String>,
}

/// Get the official Kafka Connect compatibility matrix
pub fn compatibility_matrix() -> Vec<ConnectorEntry> {
    vec![
        // Source Connectors
        ConnectorEntry {
            name: "Debezium PostgreSQL".into(),
            class_name: "io.debezium.connector.postgresql.PostgresConnector".into(),
            connector_type: "source".into(),
            vendor: "Debezium".into(),
            status: CompatibilityStatus::Compatible,
            version_tested: Some("2.5.0".into()),
            notes: Some("Logical replication via pgoutput. WAL2JSON also supported.".into()),
        },
        ConnectorEntry {
            name: "Debezium MySQL".into(),
            class_name: "io.debezium.connector.mysql.MySqlConnector".into(),
            connector_type: "source".into(),
            vendor: "Debezium".into(),
            status: CompatibilityStatus::Compatible,
            version_tested: Some("2.5.0".into()),
            notes: Some("Binlog replication with GTID support.".into()),
        },
        ConnectorEntry {
            name: "Debezium MongoDB".into(),
            class_name: "io.debezium.connector.mongodb.MongoDbConnector".into(),
            connector_type: "source".into(),
            vendor: "Debezium".into(),
            status: CompatibilityStatus::Untested,
            version_tested: None,
            notes: None,
        },
        ConnectorEntry {
            name: "JDBC Source".into(),
            class_name: "io.confluent.connect.jdbc.JdbcSourceConnector".into(),
            connector_type: "source".into(),
            vendor: "Confluent".into(),
            status: CompatibilityStatus::Untested,
            version_tested: None,
            notes: Some("Requires Confluent Hub license.".into()),
        },
        ConnectorEntry {
            name: "FileStream Source".into(),
            class_name: "org.apache.kafka.connect.file.FileStreamSourceConnector".into(),
            connector_type: "source".into(),
            vendor: "Apache Kafka".into(),
            status: CompatibilityStatus::Verified,
            version_tested: Some("3.7.0".into()),
            notes: Some("Built-in connector, verified with Streamline Connect runtime.".into()),
        },
        // Sink Connectors
        ConnectorEntry {
            name: "S3 Sink".into(),
            class_name: "io.confluent.connect.s3.S3SinkConnector".into(),
            connector_type: "sink".into(),
            vendor: "Confluent".into(),
            status: CompatibilityStatus::Untested,
            version_tested: None,
            notes: Some("Use Streamline's native S3 tiering for simpler alternative.".into()),
        },
        ConnectorEntry {
            name: "Elasticsearch Sink".into(),
            class_name: "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector".into(),
            connector_type: "sink".into(),
            vendor: "Confluent".into(),
            status: CompatibilityStatus::Untested,
            version_tested: None,
            notes: None,
        },
        ConnectorEntry {
            name: "JDBC Sink".into(),
            class_name: "io.confluent.connect.jdbc.JdbcSinkConnector".into(),
            connector_type: "sink".into(),
            vendor: "Confluent".into(),
            status: CompatibilityStatus::Untested,
            version_tested: None,
            notes: None,
        },
        ConnectorEntry {
            name: "FileStream Sink".into(),
            class_name: "org.apache.kafka.connect.file.FileStreamSinkConnector".into(),
            connector_type: "sink".into(),
            vendor: "Apache Kafka".into(),
            status: CompatibilityStatus::Verified,
            version_tested: Some("3.7.0".into()),
            notes: Some("Built-in connector, verified with Streamline Connect runtime.".into()),
        },
        ConnectorEntry {
            name: "BigQuery Sink".into(),
            class_name: "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector".into(),
            connector_type: "sink".into(),
            vendor: "WePay".into(),
            status: CompatibilityStatus::Untested,
            version_tested: None,
            notes: None,
        },
        ConnectorEntry {
            name: "HTTP Sink".into(),
            class_name: "io.confluent.connect.http.HttpSinkConnector".into(),
            connector_type: "sink".into(),
            vendor: "Confluent".into(),
            status: CompatibilityStatus::Compatible,
            version_tested: Some("0.2.0".into()),
            notes: Some("Tested with webhook endpoints.".into()),
        },
        ConnectorEntry {
            name: "Snowflake Sink".into(),
            class_name: "com.snowflake.kafka.connector.SnowflakeSinkConnector".into(),
            connector_type: "sink".into(),
            vendor: "Snowflake".into(),
            status: CompatibilityStatus::Untested,
            version_tested: None,
            notes: None,
        },
    ]
}

/// Get summary statistics for the compatibility matrix
pub fn matrix_summary() -> MatrixSummary {
    let matrix = compatibility_matrix();
    MatrixSummary {
        total: matrix.len(),
        verified: matrix.iter().filter(|c| c.status == CompatibilityStatus::Verified).count(),
        compatible: matrix.iter().filter(|c| c.status == CompatibilityStatus::Compatible).count(),
        partial: matrix.iter().filter(|c| c.status == CompatibilityStatus::Partial).count(),
        untested: matrix.iter().filter(|c| c.status == CompatibilityStatus::Untested).count(),
        incompatible: matrix.iter().filter(|c| c.status == CompatibilityStatus::Incompatible).count(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatrixSummary {
    pub total: usize,
    pub verified: usize,
    pub compatible: usize,
    pub partial: usize,
    pub untested: usize,
    pub incompatible: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compatibility_matrix_populated() {
        let matrix = compatibility_matrix();
        assert!(matrix.len() >= 10, "Matrix must have at least 10 connectors");
    }

    #[test]
    fn test_matrix_summary() {
        let summary = matrix_summary();
        assert!(summary.verified >= 2, "At least 2 connectors must be verified");
        assert_eq!(summary.total, summary.verified + summary.compatible + summary.partial + summary.untested + summary.incompatible);
    }

    #[test]
    fn test_no_incompatible_connectors() {
        let matrix = compatibility_matrix();
        let incompatible: Vec<_> = matrix.iter().filter(|c| c.status == CompatibilityStatus::Incompatible).collect();
        assert!(incompatible.is_empty(), "No connectors should be marked incompatible: {:?}", incompatible);
    }
}

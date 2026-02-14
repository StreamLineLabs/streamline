//! Zero-ETL Export Manager
//!
//! Direct streaming export to data warehouse formats (Iceberg, Delta Lake, etc.)

use super::config::ParquetConfig;
use super::parquet_segment::ParquetSegment;
use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Export manager for zero-ETL streaming
pub struct ExportManager {
    /// Configuration
    #[allow(dead_code)]
    config: ParquetConfig,
    /// Active export jobs
    jobs: Arc<RwLock<HashMap<String, ExportJob>>>,
    /// Export destinations
    destinations: Arc<RwLock<HashMap<String, ExportDestination>>>,
}

impl ExportManager {
    /// Create a new export manager
    pub fn new(config: ParquetConfig) -> Result<Self> {
        Ok(Self {
            config,
            jobs: Arc::new(RwLock::new(HashMap::new())),
            destinations: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Register an export destination
    pub async fn register_destination(
        &self,
        name: &str,
        destination: ExportDestination,
    ) -> Result<()> {
        let mut destinations = self.destinations.write().await;
        destinations.insert(name.to_string(), destination);
        tracing::info!(name = name, "Export destination registered");
        Ok(())
    }

    /// Remove an export destination
    pub async fn remove_destination(&self, name: &str) -> Result<()> {
        let mut destinations = self.destinations.write().await;
        destinations.remove(name);
        Ok(())
    }

    /// Create a new export job
    pub async fn create_job(&self, config: ExportJobConfig) -> Result<ExportJob> {
        // Validate destination exists
        {
            let destinations = self.destinations.read().await;
            if !destinations.contains_key(&config.destination) {
                return Err(StreamlineError::Config(format!(
                    "Export destination '{}' not found",
                    config.destination
                )));
            }
        }

        let job = ExportJob {
            id: uuid::Uuid::new_v4().to_string(),
            config: config.clone(),
            status: ExportStatus::Pending,
            progress: ExportProgress::default(),
            created_at: chrono::Utc::now().timestamp(),
            started_at: None,
            completed_at: None,
            error: None,
        };

        let mut jobs = self.jobs.write().await;
        jobs.insert(job.id.clone(), job.clone());

        tracing::info!(
            job_id = %job.id,
            topic = %config.topic,
            destination = %config.destination,
            "Export job created"
        );

        Ok(job)
    }

    /// Start an export job
    pub async fn start_job(&self, job_id: &str) -> Result<()> {
        let mut jobs = self.jobs.write().await;

        let job = jobs
            .get_mut(job_id)
            .ok_or_else(|| StreamlineError::Config(format!("Export job '{}' not found", job_id)))?;

        if !matches!(job.status, ExportStatus::Pending | ExportStatus::Failed) {
            return Err(StreamlineError::Config(format!(
                "Job '{}' cannot be started in current state: {:?}",
                job_id, job.status
            )));
        }

        job.status = ExportStatus::Running;
        job.started_at = Some(chrono::Utc::now().timestamp());

        // Spawn export task
        let job_id = job_id.to_string();
        let jobs = self.jobs.clone();
        let destinations = self.destinations.clone();

        tokio::spawn(async move {
            if let Err(e) = Self::run_export_job(&job_id, &jobs, &destinations).await {
                tracing::error!(job_id = %job_id, error = %e, "Export job failed");

                let mut jobs = jobs.write().await;
                if let Some(job) = jobs.get_mut(&job_id) {
                    job.status = ExportStatus::Failed;
                    job.error = Some(e.to_string());
                    job.completed_at = Some(chrono::Utc::now().timestamp());
                }
            }
        });

        Ok(())
    }

    /// Cancel an export job
    pub async fn cancel_job(&self, job_id: &str) -> Result<()> {
        let mut jobs = self.jobs.write().await;

        let job = jobs
            .get_mut(job_id)
            .ok_or_else(|| StreamlineError::Config(format!("Export job '{}' not found", job_id)))?;

        if !matches!(job.status, ExportStatus::Running | ExportStatus::Pending) {
            return Err(StreamlineError::Config(format!(
                "Job '{}' cannot be cancelled in current state: {:?}",
                job_id, job.status
            )));
        }

        job.status = ExportStatus::Cancelled;
        job.completed_at = Some(chrono::Utc::now().timestamp());

        Ok(())
    }

    /// Get job status
    pub async fn get_job(&self, job_id: &str) -> Option<ExportJob> {
        let jobs = self.jobs.read().await;
        jobs.get(job_id).cloned()
    }

    /// List all jobs
    pub async fn list_jobs(&self) -> Vec<ExportJob> {
        let jobs = self.jobs.read().await;
        jobs.values().cloned().collect()
    }

    /// Run an export job
    async fn run_export_job(
        job_id: &str,
        jobs: &Arc<RwLock<HashMap<String, ExportJob>>>,
        destinations: &Arc<RwLock<HashMap<String, ExportDestination>>>,
    ) -> Result<()> {
        // Get job config
        let (topic, destination_name, format) = {
            let jobs = jobs.read().await;
            let job = jobs
                .get(job_id)
                .ok_or_else(|| StreamlineError::Config(format!("Job '{}' not found", job_id)))?;
            (
                job.config.topic.clone(),
                job.config.destination.clone(),
                job.config.format,
            )
        };

        // Get destination
        let destination = {
            let destinations = destinations.read().await;
            destinations
                .get(&destination_name)
                .cloned()
                .ok_or_else(|| {
                    StreamlineError::Config(format!("Destination '{}' not found", destination_name))
                })?
        };

        tracing::info!(
            job_id = %job_id,
            topic = %topic,
            format = ?format,
            "Starting export"
        );

        // Export based on destination type
        match destination {
            ExportDestination::Iceberg(iceberg) => {
                Self::export_to_iceberg(job_id, &topic, &iceberg, jobs).await?;
            }
            ExportDestination::DeltaLake(delta) => {
                Self::export_to_delta(job_id, &topic, &delta, jobs).await?;
            }
            ExportDestination::S3(s3) => {
                Self::export_to_s3(job_id, &topic, &s3, jobs).await?;
            }
            ExportDestination::LocalPath(path) => {
                Self::export_to_local(job_id, &topic, &path, jobs).await?;
            }
        }

        // Mark job as completed
        {
            let mut jobs = jobs.write().await;
            if let Some(job) = jobs.get_mut(job_id) {
                job.status = ExportStatus::Completed;
                job.completed_at = Some(chrono::Utc::now().timestamp());
            }
        }

        tracing::info!(job_id = %job_id, "Export completed");

        Ok(())
    }

    /// Export to Iceberg table
    async fn export_to_iceberg(
        job_id: &str,
        topic: &str,
        config: &IcebergConfig,
        jobs: &Arc<RwLock<HashMap<String, ExportJob>>>,
    ) -> Result<()> {
        tracing::info!(
            job_id = %job_id,
            topic = topic,
            catalog = %config.catalog_uri,
            table = %config.table_name,
            "Exporting to Iceberg"
        );

        // Update progress
        {
            let mut jobs = jobs.write().await;
            if let Some(job) = jobs.get_mut(job_id) {
                job.progress.phase = "scanning".to_string();
            }
        }

        // Placeholder: would integrate with Iceberg catalog
        // 1. Connect to catalog
        // 2. Get or create table
        // 3. Stream Parquet files to table location
        // 4. Commit to catalog

        Ok(())
    }

    /// Export to Delta Lake
    async fn export_to_delta(
        job_id: &str,
        topic: &str,
        config: &DeltaLakeConfig,
        jobs: &Arc<RwLock<HashMap<String, ExportJob>>>,
    ) -> Result<()> {
        tracing::info!(
            job_id = %job_id,
            topic = topic,
            path = %config.table_path.display(),
            "Exporting to Delta Lake"
        );

        // Update progress
        {
            let mut jobs = jobs.write().await;
            if let Some(job) = jobs.get_mut(job_id) {
                job.progress.phase = "scanning".to_string();
            }
        }

        // Placeholder: would integrate with Delta Lake
        // 1. Create or open Delta table
        // 2. Write Parquet files with delta transaction log
        // 3. Commit transaction

        Ok(())
    }

    /// Export to S3
    async fn export_to_s3(
        job_id: &str,
        topic: &str,
        config: &S3Config,
        jobs: &Arc<RwLock<HashMap<String, ExportJob>>>,
    ) -> Result<()> {
        tracing::info!(
            job_id = %job_id,
            topic = topic,
            bucket = %config.bucket,
            prefix = %config.prefix,
            "Exporting to S3"
        );

        // Update progress
        {
            let mut jobs = jobs.write().await;
            if let Some(job) = jobs.get_mut(job_id) {
                job.progress.phase = "uploading".to_string();
            }
        }

        // Placeholder: would use object_store crate
        // 1. List segments to export
        // 2. Upload each Parquet file
        // 3. Update manifest if needed

        Ok(())
    }

    /// Export to local path
    async fn export_to_local(
        job_id: &str,
        topic: &str,
        path: &PathBuf,
        jobs: &Arc<RwLock<HashMap<String, ExportJob>>>,
    ) -> Result<()> {
        tracing::info!(
            job_id = %job_id,
            topic = topic,
            path = %path.display(),
            "Exporting to local path"
        );

        // Create directory
        std::fs::create_dir_all(path).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to create export directory: {}", e))
        })?;

        // Update progress
        {
            let mut jobs = jobs.write().await;
            if let Some(job) = jobs.get_mut(job_id) {
                job.progress.phase = "copying".to_string();
            }
        }

        // Placeholder: would copy segment files
        // 1. List segments for topic
        // 2. Copy each Parquet file to destination
        // 3. Write manifest

        Ok(())
    }

    /// Export a single segment
    pub async fn export_segment(
        &self,
        segment: &ParquetSegment,
        destination: &str,
    ) -> Result<ExportedFile> {
        let destinations = self.destinations.read().await;
        let dest = destinations.get(destination).ok_or_else(|| {
            StreamlineError::Config(format!("Destination '{}' not found", destination))
        })?;

        let file = match dest {
            ExportDestination::LocalPath(path) => {
                let dest_path = path.join(format!("{}_{}.parquet", segment.topic, segment.id));

                std::fs::copy(&segment.path, &dest_path).map_err(|e| {
                    StreamlineError::storage_msg(format!("Failed to copy segment: {}", e))
                })?;

                ExportedFile {
                    path: dest_path.to_string_lossy().to_string(),
                    size_bytes: segment.size_bytes,
                    row_count: segment.record_count,
                    format: ExportFormat::Parquet,
                }
            }
            _ => {
                return Err(StreamlineError::Config(
                    "Segment export not supported for this destination type".into(),
                ));
            }
        };

        Ok(file)
    }
}

/// Export destination configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ExportDestination {
    /// Apache Iceberg table
    Iceberg(IcebergConfig),
    /// Delta Lake table
    DeltaLake(DeltaLakeConfig),
    /// S3 bucket
    S3(S3Config),
    /// Local file path
    LocalPath(PathBuf),
}

/// Iceberg configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergConfig {
    /// Catalog URI
    pub catalog_uri: String,
    /// Catalog type (rest, hive, glue, etc.)
    pub catalog_type: String,
    /// Namespace
    pub namespace: String,
    /// Table name
    pub table_name: String,
    /// Additional properties
    pub properties: HashMap<String, String>,
}

/// Delta Lake configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaLakeConfig {
    /// Table path
    pub table_path: PathBuf,
    /// Enable time travel
    pub time_travel_enabled: bool,
    /// Retention period in days
    pub retention_days: u32,
}

/// S3 configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    /// Bucket name
    pub bucket: String,
    /// Key prefix
    pub prefix: String,
    /// Region
    pub region: String,
    /// Endpoint (for S3-compatible storage)
    pub endpoint: Option<String>,
    /// Credentials
    pub credentials: Option<S3Credentials>,
}

/// S3 credentials
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Credentials {
    /// Access key ID
    pub access_key_id: String,
    /// Secret access key
    pub secret_access_key: String,
    /// Session token (optional)
    pub session_token: Option<String>,
}

/// Export job
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportJob {
    /// Job ID
    pub id: String,
    /// Job configuration
    pub config: ExportJobConfig,
    /// Current status
    pub status: ExportStatus,
    /// Progress information
    pub progress: ExportProgress,
    /// Created timestamp
    pub created_at: i64,
    /// Started timestamp
    pub started_at: Option<i64>,
    /// Completed timestamp
    pub completed_at: Option<i64>,
    /// Error message if failed
    pub error: Option<String>,
}

/// Export job configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportJobConfig {
    /// Source topic
    pub topic: String,
    /// Destination name
    pub destination: String,
    /// Export format
    pub format: ExportFormat,
    /// Partition filter
    pub partitions: Option<Vec<i32>>,
    /// Time range filter
    pub time_range: Option<TimeRange>,
    /// Batch size
    pub batch_size: usize,
    /// Parallelism
    pub parallelism: usize,
}

impl Default for ExportJobConfig {
    fn default() -> Self {
        Self {
            topic: String::new(),
            destination: String::new(),
            format: ExportFormat::Parquet,
            partitions: None,
            time_range: None,
            batch_size: 10000,
            parallelism: 4,
        }
    }
}

/// Export format
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExportFormat {
    /// Apache Parquet
    Parquet,
    /// Apache Avro
    Avro,
    /// JSON Lines
    JsonLines,
    /// CSV
    Csv,
}

/// Time range for export
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRange {
    /// Start timestamp (inclusive)
    pub start: i64,
    /// End timestamp (exclusive)
    pub end: i64,
}

/// Export status
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExportStatus {
    /// Job is pending
    Pending,
    /// Job is running
    Running,
    /// Job completed successfully
    Completed,
    /// Job failed
    Failed,
    /// Job was cancelled
    Cancelled,
}

/// Export progress
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExportProgress {
    /// Current phase
    pub phase: String,
    /// Segments processed
    pub segments_processed: u64,
    /// Total segments
    pub segments_total: u64,
    /// Rows exported
    pub rows_exported: u64,
    /// Bytes exported
    pub bytes_exported: u64,
    /// Files created
    pub files_created: u64,
}

/// Exported file info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportedFile {
    /// File path or URI
    pub path: String,
    /// Size in bytes
    pub size_bytes: u64,
    /// Row count
    pub row_count: u64,
    /// Format
    pub format: ExportFormat,
}

/// Export manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportManifest {
    /// Export job ID
    pub job_id: String,
    /// Source topic
    pub topic: String,
    /// Exported files
    pub files: Vec<ExportedFile>,
    /// Total rows
    pub total_rows: u64,
    /// Total bytes
    pub total_bytes: u64,
    /// Export timestamp
    pub exported_at: i64,
    /// Time range covered
    pub time_range: Option<TimeRange>,
    /// Schema version
    pub schema_version: u32,
}

impl ExportManifest {
    /// Create a new export manifest
    pub fn new(job_id: String, topic: String) -> Self {
        Self {
            job_id,
            topic,
            files: Vec::new(),
            total_rows: 0,
            total_bytes: 0,
            exported_at: chrono::Utc::now().timestamp(),
            time_range: None,
            schema_version: 1,
        }
    }

    /// Add a file to the manifest
    pub fn add_file(&mut self, file: ExportedFile) {
        self.total_rows += file.row_count;
        self.total_bytes += file.size_bytes;
        self.files.push(file);
    }

    /// Save manifest to file
    pub fn save(&self, path: &PathBuf) -> Result<()> {
        let json = serde_json::to_string_pretty(self).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to serialize manifest: {}", e))
        })?;

        std::fs::write(path, json).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to write manifest: {}", e))
        })?;

        Ok(())
    }

    /// Load manifest from file
    pub fn load(path: &PathBuf) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to read manifest: {}", e)))?;

        serde_json::from_str(&content)
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to parse manifest: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_export_manager_creation() {
        let config = ParquetConfig::default();
        let manager = ExportManager::new(config).unwrap();

        // Register a local destination
        manager
            .register_destination(
                "local-test",
                ExportDestination::LocalPath(PathBuf::from("/tmp/export")),
            )
            .await
            .unwrap();

        let destinations = manager.destinations.read().await;
        assert!(destinations.contains_key("local-test"));
    }

    #[test]
    fn test_export_manifest() {
        let mut manifest = ExportManifest::new("job-1".to_string(), "test-topic".to_string());

        manifest.add_file(ExportedFile {
            path: "file1.parquet".to_string(),
            size_bytes: 1000,
            row_count: 100,
            format: ExportFormat::Parquet,
        });

        manifest.add_file(ExportedFile {
            path: "file2.parquet".to_string(),
            size_bytes: 2000,
            row_count: 200,
            format: ExportFormat::Parquet,
        });

        assert_eq!(manifest.files.len(), 2);
        assert_eq!(manifest.total_rows, 300);
        assert_eq!(manifest.total_bytes, 3000);
    }

    #[tokio::test]
    async fn test_create_job() {
        let config = ParquetConfig::default();
        let manager = ExportManager::new(config).unwrap();

        // Register destination first
        manager
            .register_destination(
                "test-dest",
                ExportDestination::LocalPath(PathBuf::from("/tmp/test")),
            )
            .await
            .unwrap();

        // Create job
        let job = manager
            .create_job(ExportJobConfig {
                topic: "test-topic".to_string(),
                destination: "test-dest".to_string(),
                format: ExportFormat::Parquet,
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(job.status, ExportStatus::Pending);
        assert_eq!(job.config.topic, "test-topic");
    }
}

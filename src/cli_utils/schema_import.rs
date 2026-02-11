//! Schema Import - Import schemas from Confluent Schema Registry to Streamline
//!
//! This module handles importing Avro, Protobuf, and JSON schemas from
//! Confluent Schema Registry into Streamline's schema registry.

use colored::Colorize;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// URL-encode a subject name for Schema Registry API calls
fn encode_subject(subject: &str) -> String {
    subject
        .chars()
        .map(|c| match c {
            ' ' => "%20".to_string(),
            '%' => "%25".to_string(),
            '/' => "%2F".to_string(),
            '?' => "%3F".to_string(),
            '#' => "%23".to_string(),
            '&' => "%26".to_string(),
            '=' => "%3D".to_string(),
            '+' => "%2B".to_string(),
            _ if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' => c.to_string(),
            _ => format!("%{:02X}", c as u8),
        })
        .collect()
}

/// Configuration for schema import
#[derive(Debug, Clone)]
pub struct SchemaImportConfig {
    /// Confluent Schema Registry URL
    pub schema_registry_url: String,
    /// Streamline Schema Registry URL
    pub streamline_registry_url: String,
    /// Subjects to import (empty = all)
    pub subjects: Vec<String>,
    /// Whether to include all versions or just latest
    pub all_versions: bool,
    /// Dry run mode
    pub dry_run: bool,
    /// Output file for schema dump
    pub output_file: Option<std::path::PathBuf>,
    /// Basic auth credentials (user:password)
    pub auth: Option<String>,
}

impl Default for SchemaImportConfig {
    fn default() -> Self {
        Self {
            schema_registry_url: "http://localhost:8081".into(),
            streamline_registry_url: "http://localhost:8082".into(),
            subjects: vec![],
            all_versions: false,
            dry_run: true,
            output_file: None,
            auth: None,
        }
    }
}

/// Schema information from registry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaInfo {
    pub subject: String,
    pub version: i32,
    pub id: i32,
    pub schema_type: String,
    pub schema: String,
    pub references: Vec<SchemaReference>,
    pub compatibility: Option<String>,
}

/// Schema reference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaReference {
    pub name: String,
    pub subject: String,
    pub version: i32,
}

/// Schema import result
#[derive(Debug)]
pub struct SchemaImportResult {
    pub subjects_imported: usize,
    pub schemas_imported: usize,
    pub errors: Vec<String>,
    pub schemas: Vec<SchemaInfo>,
}

/// Schema Registry client
struct SchemaRegistryClient {
    base_url: String,
    auth: Option<String>,
    client: reqwest::blocking::Client,
}

impl SchemaRegistryClient {
    fn new(base_url: &str, auth: Option<String>) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            auth,
            client: reqwest::blocking::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
        }
    }

    fn get(&self, path: &str) -> Result<reqwest::blocking::Response, String> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.client.get(&url);

        if let Some(ref auth) = self.auth {
            if let Some((user, pass)) = auth.split_once(':') {
                req = req.basic_auth(user, Some(pass));
            }
        }

        req.send()
            .map_err(|e| format!("HTTP request failed: {}", e))
    }

    fn list_subjects(&self) -> Result<Vec<String>, String> {
        let response = self.get("/subjects")?;

        if !response.status().is_success() {
            return Err(format!("Failed to list subjects: {}", response.status()));
        }

        response
            .json::<Vec<String>>()
            .map_err(|e| format!("Failed to parse subjects: {}", e))
    }

    fn get_versions(&self, subject: &str) -> Result<Vec<i32>, String> {
        let encoded = encode_subject(subject);
        let response = self.get(&format!("/subjects/{}/versions", encoded))?;

        if !response.status().is_success() {
            return Err(format!(
                "Failed to get versions for {}: {}",
                subject,
                response.status()
            ));
        }

        response
            .json::<Vec<i32>>()
            .map_err(|e| format!("Failed to parse versions: {}", e))
    }

    fn get_schema(&self, subject: &str, version: i32) -> Result<SchemaRegistryResponse, String> {
        let encoded = encode_subject(subject);
        let response = self.get(&format!("/subjects/{}/versions/{}", encoded, version))?;

        if !response.status().is_success() {
            return Err(format!(
                "Failed to get schema for {}:{}: {}",
                subject,
                version,
                response.status()
            ));
        }

        response
            .json::<SchemaRegistryResponse>()
            .map_err(|e| format!("Failed to parse schema: {}", e))
    }

    fn get_compatibility(&self, subject: &str) -> Result<Option<String>, String> {
        let encoded = encode_subject(subject);
        let response = self.get(&format!("/config/{}", encoded));

        match response {
            Ok(resp) if resp.status().is_success() => {
                #[derive(Deserialize)]
                struct CompatResponse {
                    #[serde(rename = "compatibilityLevel")]
                    compatibility_level: Option<String>,
                }
                resp.json::<CompatResponse>()
                    .map(|c| c.compatibility_level)
                    .map_err(|e| format!("Failed to parse compatibility: {}", e))
            }
            _ => Ok(None), // Subject-level config not set
        }
    }
}

#[derive(Debug, Deserialize)]
struct SchemaRegistryResponse {
    subject: String,
    version: i32,
    id: i32,
    #[serde(rename = "schemaType", default)]
    schema_type: Option<String>,
    schema: String,
    #[serde(default)]
    references: Vec<SchemaReferenceResponse>,
}

#[derive(Debug, Deserialize)]
struct SchemaReferenceResponse {
    name: String,
    subject: String,
    version: i32,
}

/// Run the schema import process
pub fn run_schema_import(config: &SchemaImportConfig) -> crate::Result<SchemaImportResult> {
    print_banner();

    let mut result = SchemaImportResult {
        subjects_imported: 0,
        schemas_imported: 0,
        errors: vec![],
        schemas: vec![],
    };

    // Step 1: Connect to Schema Registry
    println!();
    print_step(1, "Connecting to Schema Registry...");

    let client = SchemaRegistryClient::new(&config.schema_registry_url, config.auth.clone());

    // Test connection by listing subjects
    let all_subjects = match client.list_subjects() {
        Ok(subjects) => {
            println!(
                "  {} Connected to Schema Registry at {}",
                "✓".green(),
                config.schema_registry_url.cyan()
            );
            subjects
        }
        Err(e) => {
            println!("  {} Failed to connect: {}", "✗".red(), e);
            result.errors.push(e);
            return Ok(result);
        }
    };

    // Step 2: Discover subjects
    println!();
    print_step(2, "Discovering subjects...");

    let subjects_to_import = if config.subjects.is_empty() {
        all_subjects
    } else {
        all_subjects
            .into_iter()
            .filter(|s| config.subjects.contains(s))
            .collect()
    };

    if subjects_to_import.is_empty() {
        println!("  {} No subjects found to import", "ℹ".blue());
        return Ok(result);
    }

    println!(
        "  {} Found {} subjects",
        "✓".green(),
        subjects_to_import.len()
    );
    for subject in &subjects_to_import {
        println!("  {} {}", "•".dimmed(), subject.cyan());
    }

    // Step 3: Fetch schemas
    println!();
    print_step(3, "Fetching schemas...");

    for subject in &subjects_to_import {
        let versions = match client.get_versions(subject) {
            Ok(v) => v,
            Err(e) => {
                println!(
                    "  {} Failed to get versions for {}: {}",
                    "!".yellow(),
                    subject,
                    e
                );
                result.errors.push(format!("{}: {}", subject, e));
                continue;
            }
        };

        let versions_to_fetch = if config.all_versions {
            versions
        } else {
            // Just the latest version
            versions.into_iter().max().into_iter().collect()
        };

        for version in versions_to_fetch {
            match client.get_schema(subject, version) {
                Ok(schema_resp) => {
                    let compatibility = client.get_compatibility(subject).ok().flatten();

                    let schema_info = SchemaInfo {
                        subject: schema_resp.subject,
                        version: schema_resp.version,
                        id: schema_resp.id,
                        schema_type: schema_resp
                            .schema_type
                            .unwrap_or_else(|| "AVRO".to_string()),
                        schema: schema_resp.schema,
                        references: schema_resp
                            .references
                            .into_iter()
                            .map(|r| SchemaReference {
                                name: r.name,
                                subject: r.subject,
                                version: r.version,
                            })
                            .collect(),
                        compatibility,
                    };

                    result.schemas.push(schema_info);
                }
                Err(e) => {
                    println!(
                        "  {} Failed to fetch {}:{}: {}",
                        "!".yellow(),
                        subject,
                        version,
                        e
                    );
                    result
                        .errors
                        .push(format!("{}:{}: {}", subject, version, e));
                }
            }
        }

        result.subjects_imported += 1;
    }

    result.schemas_imported = result.schemas.len();
    println!(
        "  {} Fetched {} schemas from {} subjects",
        "✓".green(),
        result.schemas_imported,
        result.subjects_imported
    );

    // Step 4: Import to Streamline
    println!();
    print_step(4, "Importing to Streamline...");

    if config.dry_run {
        println!("  {} Dry run mode - showing schema summary", "ℹ".blue());
        println!();

        // Group by subject for display
        let mut by_subject: HashMap<String, Vec<&SchemaInfo>> = HashMap::new();
        for schema in &result.schemas {
            by_subject
                .entry(schema.subject.clone())
                .or_default()
                .push(schema);
        }

        for (subject, schemas) in &by_subject {
            println!("  {}", format!("Subject: {}", subject).cyan().bold());
            for schema in schemas {
                let schema_preview = if schema.schema.len() > 60 {
                    format!("{}...", &schema.schema[..60])
                } else {
                    schema.schema.clone()
                };
                println!(
                    "    {} v{} ({}) ID:{} - {}",
                    "•".dimmed(),
                    schema.version,
                    schema.schema_type,
                    schema.id,
                    schema_preview.dimmed()
                );
                if !schema.references.is_empty() {
                    println!(
                        "      References: {}",
                        schema
                            .references
                            .iter()
                            .map(|r| format!("{} v{}", r.subject, r.version))
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                }
                if let Some(ref compat) = schema.compatibility {
                    println!("      Compatibility: {}", compat);
                }
            }
            println!();
        }

        println!(
            "  {} Run with --execute to import these schemas",
            "→".cyan()
        );
    } else {
        // Register schemas in the Streamline Schema Registry
        let streamline_client =
            SchemaRegistryClient::new(&config.streamline_registry_url, config.auth.clone());

        let mut registered = 0usize;
        for schema in &result.schemas {
            let body = serde_json::json!({
                "schema": schema.schema,
                "schemaType": schema.schema_type,
                "references": schema.references.iter().map(|r| serde_json::json!({
                    "name": r.name,
                    "subject": r.subject,
                    "version": r.version,
                })).collect::<Vec<_>>(),
            });

            let url = format!(
                "{}/subjects/{}/versions",
                streamline_client.base_url,
                encode_subject(&schema.subject)
            );
            let mut req = streamline_client.client.post(&url);
            req = req.header("Content-Type", "application/vnd.schemaregistry.v1+json");
            if let Some(ref auth) = streamline_client.auth {
                if let Some((user, pass)) = auth.split_once(':') {
                    req = req.basic_auth(user, Some(pass));
                }
            }
            match req.json(&body).send() {
                Ok(resp) if resp.status().is_success() => {
                    registered += 1;
                }
                Ok(resp) => {
                    result.errors.push(format!(
                        "Failed to register schema {}: {}",
                        schema.subject,
                        resp.status()
                    ));
                }
                Err(e) => {
                    result.errors.push(format!(
                        "Failed to register schema {}: {}",
                        schema.subject, e
                    ));
                }
            }
        }

        println!(
            "  {} Registered {} of {} schemas to Streamline",
            if registered == result.schemas_imported {
                "✓".green()
            } else {
                "!".yellow()
            },
            registered,
            result.schemas_imported
        );
    }

    // Step 5: Output to file if requested
    println!();
    print_step(5, "Generating output...");

    if let Some(ref output_path) = config.output_file {
        let json = serde_json::to_string_pretty(&result.schemas)?;
        std::fs::write(output_path, json)?;
        println!(
            "  {} Schema dump written to {}",
            "✓".green(),
            output_path.display()
        );
    } else {
        println!("  {} No output file specified", "ℹ".blue());
    }

    // Summary
    println!();
    println!("{}", "─".repeat(60).dimmed());
    println!();
    println!("  {}", "Schema Import Summary".bold().cyan());
    println!();
    println!(
        "  Subjects processed: {}",
        result.subjects_imported.to_string().green()
    );
    println!(
        "  Schemas fetched:    {}",
        result.schemas_imported.to_string().green()
    );
    println!(
        "  Errors:             {}",
        if result.errors.is_empty() {
            "0".green()
        } else {
            result.errors.len().to_string().red()
        }
    );

    // Schema type breakdown
    let mut by_type: HashMap<String, usize> = HashMap::new();
    for schema in &result.schemas {
        *by_type.entry(schema.schema_type.clone()).or_default() += 1;
    }
    if !by_type.is_empty() {
        println!();
        println!("  Schema types:");
        for (schema_type, count) in &by_type {
            println!("    {} {}: {}", "•".dimmed(), schema_type, count);
        }
    }

    Ok(result)
}

fn print_banner() {
    println!();
    println!("  {}", "Streamline Schema Import".bold().cyan());
    println!("{}", "═".repeat(60).dimmed());
    println!();
    println!(
        "  {}",
        "Import schemas from Confluent Schema Registry".dimmed()
    );
}

fn print_step(num: u32, message: &str) {
    println!(
        "  {} {}",
        format!("[{}/5]", num).cyan().bold(),
        message.bold()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_import_config_default() {
        let config = SchemaImportConfig::default();
        assert_eq!(config.schema_registry_url, "http://localhost:8081");
        assert!(config.dry_run);
    }

    #[test]
    fn test_schema_info() {
        let info = SchemaInfo {
            subject: "test-value".to_string(),
            version: 1,
            id: 1,
            schema_type: "AVRO".to_string(),
            schema: r#"{"type":"record","name":"Test"}"#.to_string(),
            references: vec![],
            compatibility: Some("BACKWARD".to_string()),
        };
        assert_eq!(info.schema_type, "AVRO");
    }
}

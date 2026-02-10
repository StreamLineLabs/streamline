//! Policy loading and parsing for Streamline

use crate::error::{Result, StreamlineError};
use crate::policy::types::PolicyDocument;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

/// Source of policy definitions
#[derive(Debug, Clone)]
pub enum PolicySource {
    /// Load from a single YAML/JSON file
    File(PathBuf),
    /// Load from a directory (all .yaml, .yml, .json files)
    Directory(PathBuf),
    /// Load from multiple files
    Files(Vec<PathBuf>),
    /// Load from inline YAML/JSON string
    Inline(String),
    /// Load from environment variable
    EnvVar(String),
}

/// Policy loader for reading and parsing policy documents
pub struct PolicyLoader {
    /// Base directory for resolving relative paths
    base_dir: PathBuf,
    /// Variables for template substitution
    variables: HashMap<String, String>,
}

impl PolicyLoader {
    /// Create a new policy loader
    pub fn new() -> Self {
        Self {
            base_dir: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            variables: HashMap::new(),
        }
    }

    /// Set the base directory for relative paths
    pub fn with_base_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.base_dir = dir.as_ref().to_path_buf();
        self
    }

    /// Add a variable for template substitution
    pub fn with_variable(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.variables.insert(key.into(), value.into());
        self
    }

    /// Add multiple variables
    pub fn with_variables(mut self, vars: HashMap<String, String>) -> Self {
        self.variables.extend(vars);
        self
    }

    /// Load environment variables as template variables
    pub fn with_env_vars(mut self, prefix: Option<&str>) -> Self {
        for (key, value) in std::env::vars() {
            let include = match prefix {
                Some(p) => key.starts_with(p),
                None => true,
            };
            if include {
                self.variables.insert(key, value);
            }
        }
        self
    }

    /// Load policies from a source
    pub fn load(&self, source: PolicySource) -> Result<PolicyDocument> {
        match source {
            PolicySource::File(path) => self.load_file(&path),
            PolicySource::Directory(path) => self.load_directory(&path),
            PolicySource::Files(paths) => self.load_files(&paths),
            PolicySource::Inline(content) => self.parse_content(&content),
            PolicySource::EnvVar(var) => self.load_from_env(&var),
        }
    }

    /// Load policies from a file
    pub fn load_file(&self, path: &Path) -> Result<PolicyDocument> {
        let path = self.resolve_path(path);
        debug!(path = %path.display(), "Loading policy file");

        if !path.exists() {
            return Err(StreamlineError::Config(format!(
                "Policy file not found: {}",
                path.display()
            )));
        }

        let content = std::fs::read_to_string(&path).map_err(|e| {
            StreamlineError::Config(format!(
                "Failed to read policy file {}: {}",
                path.display(),
                e
            ))
        })?;

        let content = self.substitute_variables(&content);
        self.parse_content(&content)
    }

    /// Load policies from a directory (non-recursive for simplicity)
    pub fn load_directory(&self, dir: &Path) -> Result<PolicyDocument> {
        let dir = self.resolve_path(dir);
        debug!(dir = %dir.display(), "Loading policies from directory");

        if !dir.is_dir() {
            return Err(StreamlineError::Config(format!(
                "Policy directory not found: {}",
                dir.display()
            )));
        }

        let mut combined = PolicyDocument::default();
        let mut files_loaded = 0;

        let entries = std::fs::read_dir(&dir).map_err(|e| {
            StreamlineError::Config(format!("Failed to read directory {}: {}", dir.display(), e))
        })?;

        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && self.is_policy_file(&path) {
                match self.load_file(&path) {
                    Ok(doc) => {
                        combined.merge(doc);
                        files_loaded += 1;
                    }
                    Err(e) => {
                        warn!(path = %path.display(), error = %e, "Failed to load policy file, skipping");
                    }
                }
            }
        }

        info!(dir = %dir.display(), files = files_loaded, "Loaded policies from directory");
        Ok(combined)
    }

    /// Load policies from multiple files
    pub fn load_files(&self, paths: &[PathBuf]) -> Result<PolicyDocument> {
        let mut combined = PolicyDocument::default();

        for path in paths {
            let doc = self.load_file(path)?;
            combined.merge(doc);
        }

        Ok(combined)
    }

    /// Load policies from an environment variable
    pub fn load_from_env(&self, var: &str) -> Result<PolicyDocument> {
        let content = std::env::var(var).map_err(|_| {
            StreamlineError::Config(format!("Environment variable '{}' not set", var))
        })?;

        let content = self.substitute_variables(&content);
        self.parse_content(&content)
    }

    /// Parse policy content (YAML or JSON)
    fn parse_content(&self, content: &str) -> Result<PolicyDocument> {
        // Try YAML first (YAML is a superset of JSON)
        serde_yaml::from_str(content)
            .map_err(|e| StreamlineError::Config(format!("Failed to parse policy document: {}", e)))
    }

    /// Resolve a path relative to the base directory
    fn resolve_path(&self, path: &Path) -> PathBuf {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.base_dir.join(path)
        }
    }

    /// Check if a path looks like a policy file (has policy extension)
    /// Note: This checks the extension only, not whether the file exists.
    /// For directory scanning, use in conjunction with path.is_file().
    pub fn is_policy_file(&self, path: &Path) -> bool {
        path.extension()
            .map(|ext| {
                let ext = ext.to_string_lossy().to_lowercase();
                ext == "yaml" || ext == "yml" || ext == "json"
            })
            .unwrap_or(false)
    }

    /// Substitute variables in content using ${VAR} or $VAR syntax
    fn substitute_variables(&self, content: &str) -> String {
        let mut result = content.to_string();

        // Replace ${VAR} syntax
        for (key, value) in &self.variables {
            result = result.replace(&format!("${{{}}}", key), value);
        }

        // Replace $VAR syntax (only if not followed by {)
        let Ok(re) = regex::Regex::new(r"\$([A-Za-z_][A-Za-z0-9_]*)") else {
            return result;
        };
        result = re
            .replace_all(&result, |caps: &regex::Captures| {
                let var = &caps[1];
                self.variables
                    .get(var)
                    .cloned()
                    .unwrap_or_else(|| format!("${}", var))
            })
            .to_string();

        result
    }
}

impl Default for PolicyLoader {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for creating policy loaders with common configurations
pub struct PolicyLoaderBuilder {
    loader: PolicyLoader,
}

impl PolicyLoaderBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            loader: PolicyLoader::new(),
        }
    }

    /// Set base directory
    pub fn base_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.loader = self.loader.with_base_dir(dir);
        self
    }

    /// Add a variable
    pub fn var(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.loader = self.loader.with_variable(key, value);
        self
    }

    /// Load environment variables with optional prefix
    pub fn env_vars(mut self, prefix: Option<&str>) -> Self {
        self.loader = self.loader.with_env_vars(prefix);
        self
    }

    /// Build the loader
    pub fn build(self) -> PolicyLoader {
        self.loader
    }
}

impl Default for PolicyLoaderBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_loader_creation() {
        let loader = PolicyLoader::new();
        assert!(loader.variables.is_empty());
    }

    #[test]
    fn test_loader_with_variables() {
        let loader = PolicyLoader::new()
            .with_variable("ENV", "production")
            .with_variable("REGION", "us-west-2");

        assert_eq!(loader.variables.get("ENV"), Some(&"production".to_string()));
        assert_eq!(
            loader.variables.get("REGION"),
            Some(&"us-west-2".to_string())
        );
    }

    #[test]
    fn test_variable_substitution() {
        let loader = PolicyLoader::new()
            .with_variable("ENV", "prod")
            .with_variable("PARTITIONS", "12");

        let content = r#"
environment: ${ENV}
partitions: $PARTITIONS
"#;
        let result = loader.substitute_variables(content);

        assert!(result.contains("environment: prod"));
        assert!(result.contains("partitions: 12"));
    }

    #[test]
    fn test_parse_yaml_content() {
        let loader = PolicyLoader::new();
        let content = r#"
version: "1.0"
metadata:
  name: test-policy
topics:
  - name: events
    partitions: 3
"#;

        let doc = loader.parse_content(content).unwrap();
        assert_eq!(doc.metadata.name, "test-policy");
        assert_eq!(doc.topics.len(), 1);
        assert_eq!(doc.topics[0].name, "events");
        assert_eq!(doc.topics[0].partitions, 3);
    }

    #[test]
    fn test_parse_json_content() {
        let loader = PolicyLoader::new();
        let content = r#"{
            "version": "1.0",
            "metadata": {"name": "test"},
            "topics": [{"name": "events", "partitions": 6}]
        }"#;

        let doc = loader.parse_content(content).unwrap();
        assert_eq!(doc.topics.len(), 1);
        assert_eq!(doc.topics[0].partitions, 6);
    }

    #[test]
    fn test_load_file() {
        let temp_dir = TempDir::new().unwrap();
        let policy_path = temp_dir.path().join("policy.yaml");

        std::fs::write(
            &policy_path,
            r#"
version: "1.0"
metadata:
  name: file-test
topics:
  - name: test-topic
    partitions: 4
"#,
        )
        .unwrap();

        let loader = PolicyLoader::new().with_base_dir(temp_dir.path());
        let doc = loader.load_file(Path::new("policy.yaml")).unwrap();

        assert_eq!(doc.metadata.name, "file-test");
        assert_eq!(doc.topics[0].partitions, 4);
    }

    #[test]
    fn test_load_directory() {
        let temp_dir = TempDir::new().unwrap();

        // Create multiple policy files
        std::fs::write(
            temp_dir.path().join("topics.yaml"),
            r#"
topics:
  - name: events
    partitions: 3
"#,
        )
        .unwrap();

        std::fs::write(
            temp_dir.path().join("acls.yaml"),
            r#"
acls:
  - principal: "User:alice"
    resource_type: topic
    resource_name: events
    operations: [read]
"#,
        )
        .unwrap();

        let loader = PolicyLoader::new();
        let doc = loader.load_directory(temp_dir.path()).unwrap();

        assert_eq!(doc.topics.len(), 1);
        assert_eq!(doc.acls.len(), 1);
    }

    #[test]
    fn test_load_inline() {
        let content = r#"
topics:
  - name: inline-topic
    partitions: 1
"#;

        let loader = PolicyLoader::new();
        let doc = loader
            .load(PolicySource::Inline(content.to_string()))
            .unwrap();

        assert_eq!(doc.topics.len(), 1);
        assert_eq!(doc.topics[0].name, "inline-topic");
    }

    #[test]
    fn test_is_policy_file() {
        let loader = PolicyLoader::new();

        assert!(loader.is_policy_file(Path::new("policy.yaml")));
        assert!(loader.is_policy_file(Path::new("policy.yml")));
        assert!(loader.is_policy_file(Path::new("policy.json")));
        assert!(!loader.is_policy_file(Path::new("policy.txt")));
        assert!(!loader.is_policy_file(Path::new("policy")));
    }

    #[test]
    fn test_builder() {
        let loader = PolicyLoaderBuilder::new()
            .base_dir("/tmp")
            .var("ENV", "test")
            .build();

        assert_eq!(loader.base_dir, PathBuf::from("/tmp"));
        assert_eq!(loader.variables.get("ENV"), Some(&"test".to_string()));
    }
}

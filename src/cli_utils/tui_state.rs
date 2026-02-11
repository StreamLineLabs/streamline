//! TUI State Persistence
//!
//! Saves and loads TUI dashboard state (bookmarks, theme, sort preferences)
//! to `~/.streamline/tui-state.json`.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;

/// Current version of the state file format
const STATE_VERSION: u32 = 1;

/// Persistent state saved between TUI sessions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TuiPersistentState {
    /// Schema version for future migrations
    pub version: u32,
    /// Bookmarked topic names
    pub bookmarked_topics: HashSet<String>,
    /// Bookmarked consumer group IDs
    pub bookmarked_groups: HashSet<String>,
    /// Theme preference ("dark" or "light")
    pub theme: String,
    /// Last active tab index
    pub last_tab: usize,
    /// Sort preferences per view
    pub sort_preferences: SortPreferences,
}

impl Default for TuiPersistentState {
    fn default() -> Self {
        Self {
            version: STATE_VERSION,
            bookmarked_topics: HashSet::new(),
            bookmarked_groups: HashSet::new(),
            theme: "dark".to_string(),
            last_tab: 0,
            sort_preferences: SortPreferences::default(),
        }
    }
}

/// Sort preferences for different views
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SortPreferences {
    /// Topics view sort config
    pub topics: Option<SortConfig>,
    /// Groups view sort config
    pub groups: Option<SortConfig>,
}

/// Sort configuration for a single view
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortConfig {
    /// Column name to sort by
    pub column: String,
    /// Sort direction (true = ascending)
    pub ascending: bool,
}

impl TuiPersistentState {
    /// Returns the path to the state file: `~/.streamline/tui-state.json`
    pub fn state_file_path() -> Option<PathBuf> {
        dirs::home_dir().map(|home| home.join(".streamline").join("tui-state.json"))
    }

    /// Load state from disk, returning defaults if file doesn't exist or is invalid
    pub fn load() -> Self {
        let Some(path) = Self::state_file_path() else {
            return Self::default();
        };

        fs::read_to_string(&path)
            .ok()
            .and_then(|contents| serde_json::from_str(&contents).ok())
            .unwrap_or_default()
    }

    /// Save state to disk, creating parent directories if needed
    pub fn save(&self) -> Result<(), std::io::Error> {
        let Some(path) = Self::state_file_path() else {
            return Ok(()); // No home directory, silently skip
        };

        // Create parent directory if it doesn't exist
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let contents = serde_json::to_string_pretty(self)?;
        fs::write(&path, contents)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_default_state() {
        let state = TuiPersistentState::default();
        assert_eq!(state.version, STATE_VERSION);
        assert!(state.bookmarked_topics.is_empty());
        assert!(state.bookmarked_groups.is_empty());
        assert_eq!(state.theme, "dark");
        assert_eq!(state.last_tab, 0);
    }

    #[test]
    fn test_serialize_deserialize() {
        let mut state = TuiPersistentState::default();
        state.bookmarked_topics.insert("orders".to_string());
        state.bookmarked_topics.insert("events".to_string());
        state.bookmarked_groups.insert("my-group".to_string());
        state.theme = "light".to_string();
        state.last_tab = 2;
        state.sort_preferences.topics = Some(SortConfig {
            column: "messages".to_string(),
            ascending: false,
        });

        let json = serde_json::to_string(&state).unwrap();
        let loaded: TuiPersistentState = serde_json::from_str(&json).unwrap();

        assert_eq!(loaded.bookmarked_topics.len(), 2);
        assert!(loaded.bookmarked_topics.contains("orders"));
        assert!(loaded.bookmarked_groups.contains("my-group"));
        assert_eq!(loaded.theme, "light");
        assert_eq!(loaded.last_tab, 2);
        assert!(loaded.sort_preferences.topics.is_some());
    }

    #[test]
    fn test_save_load_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let state_path = temp_dir.path().join("tui-state.json");

        let mut state = TuiPersistentState::default();
        state.bookmarked_topics.insert("test-topic".to_string());
        state.theme = "light".to_string();

        // Save to temp path
        let contents = serde_json::to_string_pretty(&state).unwrap();
        std::fs::write(&state_path, &contents).unwrap();

        // Load back
        let loaded_contents = std::fs::read_to_string(&state_path).unwrap();
        let loaded: TuiPersistentState = serde_json::from_str(&loaded_contents).unwrap();

        assert!(loaded.bookmarked_topics.contains("test-topic"));
        assert_eq!(loaded.theme, "light");
    }
}

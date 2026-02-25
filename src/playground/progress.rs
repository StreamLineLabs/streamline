//! Playground Progress Tracking & Achievement System
//!
//! Tracks user progress through tutorials and awards completion badges:
//!
//! - **Progress Persistence**: Saves tutorial progress to a local file so
//!   users can resume across sessions.
//! - **Achievement Badges**: Awards badges for completing milestones
//!   (first message, first consumer group, SQL query, etc.)
//! - **Guided Curriculum**: Structured learning path from basics to advanced.
//! - **Shareable State**: Export/import playground state for sharing.

use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

// ---------------------------------------------------------------------------
// Progress Tracker
// ---------------------------------------------------------------------------

/// Persistent progress state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressState {
    /// User-chosen display name.
    pub username: String,
    /// Completed tutorial IDs.
    pub completed_tutorials: HashSet<String>,
    /// Per-tutorial step progress.
    pub tutorial_progress: HashMap<String, Vec<bool>>,
    /// Earned badges.
    pub badges: Vec<Badge>,
    /// Total messages produced.
    pub messages_produced: u64,
    /// Total messages consumed.
    pub messages_consumed: u64,
    /// Total SQL queries executed.
    pub queries_executed: u64,
    /// Total topics created.
    pub topics_created: u64,
    /// When the user started.
    pub started_at: DateTime<Utc>,
    /// Last activity.
    pub last_activity: DateTime<Utc>,
    /// Current curriculum level.
    pub curriculum_level: CurriculumLevel,
}

impl Default for ProgressState {
    fn default() -> Self {
        Self {
            username: "explorer".to_string(),
            completed_tutorials: HashSet::new(),
            tutorial_progress: HashMap::new(),
            badges: Vec::new(),
            messages_produced: 0,
            messages_consumed: 0,
            queries_executed: 0,
            topics_created: 0,
            started_at: Utc::now(),
            last_activity: Utc::now(),
            curriculum_level: CurriculumLevel::Beginner,
        }
    }
}

/// Curriculum levels.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurriculumLevel {
    Beginner,
    Intermediate,
    Advanced,
    Expert,
}

/// An achievement badge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Badge {
    /// Badge identifier.
    pub id: String,
    /// Display name.
    pub name: String,
    /// Description.
    pub description: String,
    /// Emoji icon.
    pub icon: String,
    /// When earned.
    pub earned_at: DateTime<Utc>,
}

/// Badge definitions.
pub const BADGE_DEFINITIONS: &[(&str, &str, &str, &str)] = &[
    (
        "first-message",
        "First Message",
        "Produced your first message to a topic",
        "📨",
    ),
    (
        "topic-creator",
        "Topic Creator",
        "Created your first custom topic",
        "📋",
    ),
    (
        "consumer",
        "Consumer",
        "Consumed messages from a topic",
        "📥",
    ),
    (
        "sql-explorer",
        "SQL Explorer",
        "Executed your first SQL query on streaming data",
        "🔍",
    ),
    (
        "prolific-producer",
        "Prolific Producer",
        "Produced 100+ messages",
        "🚀",
    ),
    (
        "tutorial-graduate",
        "Tutorial Graduate",
        "Completed all beginner tutorials",
        "🎓",
    ),
    (
        "power-user",
        "Power User",
        "Completed all intermediate tutorials",
        "⚡",
    ),
    (
        "streamline-master",
        "Streamline Master",
        "Completed all tutorials",
        "👑",
    ),
    (
        "multi-topic",
        "Multi-Topic",
        "Created 5+ topics",
        "📚",
    ),
    (
        "data-analyst",
        "Data Analyst",
        "Executed 10+ SQL queries",
        "📊",
    ),
];

/// Progress tracker with persistence.
pub struct ProgressTracker {
    state: ProgressState,
    save_path: Option<PathBuf>,
}

impl ProgressTracker {
    /// Create a new in-memory tracker (no persistence).
    pub fn new() -> Self {
        Self {
            state: ProgressState::default(),
            save_path: None,
        }
    }

    /// Create a tracker with file persistence.
    pub fn with_persistence(path: PathBuf) -> Result<Self> {
        let state = if path.exists() {
            let content = std::fs::read_to_string(&path)
                .map_err(|e| StreamlineError::storage_msg(format!("Read progress: {}", e)))?;
            serde_json::from_str(&content)
                .map_err(|e| StreamlineError::storage_msg(format!("Parse progress: {}", e)))?
        } else {
            ProgressState::default()
        };

        Ok(Self {
            state,
            save_path: Some(path),
        })
    }

    /// Record a produced message.
    pub fn record_produce(&mut self) -> Vec<Badge> {
        self.state.messages_produced += 1;
        self.state.last_activity = Utc::now();
        self.check_badges()
    }

    /// Record a consumed message.
    pub fn record_consume(&mut self) -> Vec<Badge> {
        self.state.messages_consumed += 1;
        self.state.last_activity = Utc::now();
        self.check_badges()
    }

    /// Record a SQL query execution.
    pub fn record_query(&mut self) -> Vec<Badge> {
        self.state.queries_executed += 1;
        self.state.last_activity = Utc::now();
        self.check_badges()
    }

    /// Record a topic creation.
    pub fn record_topic_created(&mut self) -> Vec<Badge> {
        self.state.topics_created += 1;
        self.state.last_activity = Utc::now();
        self.check_badges()
    }

    /// Complete a tutorial step.
    pub fn complete_step(&mut self, tutorial_id: &str, step_index: usize) -> Vec<Badge> {
        let progress = self
            .state
            .tutorial_progress
            .entry(tutorial_id.to_string())
            .or_default();

        while progress.len() <= step_index {
            progress.push(false);
        }
        progress[step_index] = true;

        // Check if all steps are done
        if progress.iter().all(|&done| done) {
            self.state
                .completed_tutorials
                .insert(tutorial_id.to_string());
        }

        self.state.last_activity = Utc::now();
        self.check_badges()
    }

    /// Get current progress.
    pub fn progress(&self) -> &ProgressState {
        &self.state
    }

    /// Get tutorial completion percentage.
    pub fn tutorial_progress_pct(&self, tutorial_id: &str) -> f64 {
        match self.state.tutorial_progress.get(tutorial_id) {
            Some(steps) if !steps.is_empty() => {
                let done = steps.iter().filter(|&&s| s).count();
                (done as f64 / steps.len() as f64) * 100.0
            }
            _ => 0.0,
        }
    }

    /// Export progress as JSON for sharing.
    pub fn export(&self) -> Result<String> {
        serde_json::to_string_pretty(&self.state)
            .map_err(|e| StreamlineError::storage_msg(format!("Export error: {}", e)))
    }

    /// Import progress from JSON.
    pub fn import(&mut self, json: &str) -> Result<()> {
        self.state = serde_json::from_str(json)
            .map_err(|e| StreamlineError::storage_msg(format!("Import error: {}", e)))?;
        self.save()
    }

    /// Save progress to disk.
    pub fn save(&self) -> Result<()> {
        if let Some(ref path) = self.save_path {
            let json = serde_json::to_string_pretty(&self.state)
                .map_err(|e| StreamlineError::storage_msg(format!("Serialize: {}", e)))?;
            std::fs::write(path, json)
                .map_err(|e| StreamlineError::storage_msg(format!("Write: {}", e)))?;
        }
        Ok(())
    }

    /// Check and award any newly-earned badges.
    fn check_badges(&mut self) -> Vec<Badge> {
        let mut new_badges = Vec::new();
        let earned_ids: HashSet<String> =
            self.state.badges.iter().map(|b| b.id.clone()).collect();

        for (id, name, desc, icon) in BADGE_DEFINITIONS {
            if earned_ids.contains(*id) {
                continue;
            }

            let earned = match *id {
                "first-message" => self.state.messages_produced >= 1,
                "topic-creator" => self.state.topics_created >= 1,
                "consumer" => self.state.messages_consumed >= 1,
                "sql-explorer" => self.state.queries_executed >= 1,
                "prolific-producer" => self.state.messages_produced >= 100,
                "tutorial-graduate" => {
                    // Check if all beginner tutorials are done
                    self.state.completed_tutorials.len() >= 3
                }
                "power-user" => self.state.completed_tutorials.len() >= 6,
                "streamline-master" => self.state.completed_tutorials.len() >= 10,
                "multi-topic" => self.state.topics_created >= 5,
                "data-analyst" => self.state.queries_executed >= 10,
                _ => false,
            };

            if earned {
                let badge = Badge {
                    id: id.to_string(),
                    name: name.to_string(),
                    description: desc.to_string(),
                    icon: icon.to_string(),
                    earned_at: Utc::now(),
                };
                self.state.badges.push(badge.clone());
                new_badges.push(badge);
            }
        }

        // Update curriculum level
        self.state.curriculum_level = if self.state.completed_tutorials.len() >= 10 {
            CurriculumLevel::Expert
        } else if self.state.completed_tutorials.len() >= 6 {
            CurriculumLevel::Advanced
        } else if self.state.completed_tutorials.len() >= 3 {
            CurriculumLevel::Intermediate
        } else {
            CurriculumLevel::Beginner
        };

        // Auto-save
        let _ = self.save();

        new_badges
    }
}

impl Default for ProgressTracker {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Guided Curriculum
// ---------------------------------------------------------------------------

/// A curriculum item in the learning path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurriculumItem {
    /// Tutorial ID.
    pub tutorial_id: String,
    /// Display title.
    pub title: String,
    /// Level.
    pub level: CurriculumLevel,
    /// Prerequisite tutorial IDs.
    pub prerequisites: Vec<String>,
    /// Estimated time in minutes.
    pub estimated_minutes: u32,
}

/// The default learning curriculum.
pub fn default_curriculum() -> Vec<CurriculumItem> {
    vec![
        // Beginner
        CurriculumItem {
            tutorial_id: "basics-produce".to_string(),
            title: "Producing Messages".to_string(),
            level: CurriculumLevel::Beginner,
            prerequisites: vec![],
            estimated_minutes: 5,
        },
        CurriculumItem {
            tutorial_id: "basics-consume".to_string(),
            title: "Consuming Messages".to_string(),
            level: CurriculumLevel::Beginner,
            prerequisites: vec!["basics-produce".to_string()],
            estimated_minutes: 5,
        },
        CurriculumItem {
            tutorial_id: "basics-topics".to_string(),
            title: "Topic Management".to_string(),
            level: CurriculumLevel::Beginner,
            prerequisites: vec![],
            estimated_minutes: 5,
        },
        // Intermediate
        CurriculumItem {
            tutorial_id: "intermediate-groups".to_string(),
            title: "Consumer Groups".to_string(),
            level: CurriculumLevel::Intermediate,
            prerequisites: vec!["basics-consume".to_string()],
            estimated_minutes: 10,
        },
        CurriculumItem {
            tutorial_id: "intermediate-sql".to_string(),
            title: "SQL Analytics on Streams".to_string(),
            level: CurriculumLevel::Intermediate,
            prerequisites: vec!["basics-produce".to_string()],
            estimated_minutes: 10,
        },
        CurriculumItem {
            tutorial_id: "intermediate-keys".to_string(),
            title: "Message Keys & Partitioning".to_string(),
            level: CurriculumLevel::Intermediate,
            prerequisites: vec!["basics-topics".to_string()],
            estimated_minutes: 10,
        },
        // Advanced
        CurriculumItem {
            tutorial_id: "advanced-transforms".to_string(),
            title: "Stream Transforms (WASM)".to_string(),
            level: CurriculumLevel::Advanced,
            prerequisites: vec!["intermediate-groups".to_string()],
            estimated_minutes: 15,
        },
        CurriculumItem {
            tutorial_id: "advanced-schemas".to_string(),
            title: "Schema Registry".to_string(),
            level: CurriculumLevel::Advanced,
            prerequisites: vec!["intermediate-sql".to_string()],
            estimated_minutes: 15,
        },
        CurriculumItem {
            tutorial_id: "advanced-migration".to_string(),
            title: "Migrating from Kafka".to_string(),
            level: CurriculumLevel::Advanced,
            prerequisites: vec![],
            estimated_minutes: 20,
        },
        CurriculumItem {
            tutorial_id: "advanced-monitoring".to_string(),
            title: "Monitoring & Observability".to_string(),
            level: CurriculumLevel::Advanced,
            prerequisites: vec!["intermediate-groups".to_string()],
            estimated_minutes: 15,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_tracker_creation() {
        let tracker = ProgressTracker::new();
        assert_eq!(tracker.progress().curriculum_level, CurriculumLevel::Beginner);
        assert!(tracker.progress().badges.is_empty());
    }

    #[test]
    fn test_badge_first_message() {
        let mut tracker = ProgressTracker::new();
        let badges = tracker.record_produce();
        assert_eq!(badges.len(), 1);
        assert_eq!(badges[0].id, "first-message");
    }

    #[test]
    fn test_badge_not_duplicated() {
        let mut tracker = ProgressTracker::new();
        tracker.record_produce();
        let badges = tracker.record_produce();
        assert!(badges.is_empty()); // Already earned
    }

    #[test]
    fn test_prolific_producer() {
        let mut tracker = ProgressTracker::new();
        for _ in 0..99 {
            tracker.record_produce();
        }
        let badges = tracker.record_produce(); // 100th
        assert!(badges.iter().any(|b| b.id == "prolific-producer"));
    }

    #[test]
    fn test_tutorial_progress() {
        let mut tracker = ProgressTracker::new();
        tracker.complete_step("basics-produce", 0);
        tracker.complete_step("basics-produce", 1);

        assert_eq!(tracker.tutorial_progress_pct("basics-produce"), 100.0);
        assert!(tracker
            .progress()
            .completed_tutorials
            .contains("basics-produce"));
    }

    #[test]
    fn test_export_import() {
        let mut tracker = ProgressTracker::new();
        tracker.record_produce();
        tracker.record_topic_created();

        let exported = tracker.export().unwrap();

        let mut tracker2 = ProgressTracker::new();
        tracker2.import(&exported).unwrap();

        assert_eq!(tracker2.progress().messages_produced, 1);
        assert_eq!(tracker2.progress().topics_created, 1);
    }

    #[test]
    fn test_default_curriculum() {
        let curriculum = default_curriculum();
        assert_eq!(curriculum.len(), 10);

        let beginner_count = curriculum
            .iter()
            .filter(|c| c.level == CurriculumLevel::Beginner)
            .count();
        assert_eq!(beginner_count, 3);
    }

    #[test]
    fn test_curriculum_level_progression() {
        let mut tracker = ProgressTracker::new();
        // Complete 3 tutorials → Intermediate
        for i in 0..3 {
            tracker.complete_step(&format!("t{}", i), 0);
        }
        assert_eq!(
            tracker.progress().curriculum_level,
            CurriculumLevel::Intermediate
        );
    }
}

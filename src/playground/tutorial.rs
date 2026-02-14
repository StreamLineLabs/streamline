//! Interactive tutorial engine for the playground.

use serde::{Deserialize, Serialize};

/// A step type in a tutorial.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TutorialStepType {
    /// Display information to the user
    Info,
    /// Execute a command
    Command { command: String },
    /// Wait for user to run a specific command
    UserAction {
        expected_command: String,
        hint: String,
    },
    /// Verify a condition
    Verify { check: String, expected: String },
}

/// A single tutorial step.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TutorialStep {
    /// Step title
    pub title: String,
    /// Step description/instructions
    pub description: String,
    /// Step type
    pub step_type: TutorialStepType,
    /// Whether this step has been completed
    #[serde(default)]
    pub completed: bool,
}

/// A complete tutorial.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tutorial {
    /// Tutorial ID
    pub id: String,
    /// Tutorial title
    pub title: String,
    /// Tutorial description
    pub description: String,
    /// Difficulty level (1-5)
    pub difficulty: u8,
    /// Estimated time in minutes
    pub estimated_minutes: u32,
    /// Tutorial steps
    pub steps: Vec<TutorialStep>,
    /// Tags
    pub tags: Vec<String>,
}

impl Tutorial {
    /// Get the current progress as a percentage.
    pub fn progress_pct(&self) -> f64 {
        if self.steps.is_empty() {
            return 100.0;
        }
        let completed = self.steps.iter().filter(|s| s.completed).count();
        (completed as f64 / self.steps.len() as f64) * 100.0
    }

    /// Get the current (next uncompleted) step.
    pub fn current_step(&self) -> Option<(usize, &TutorialStep)> {
        self.steps.iter().enumerate().find(|(_, s)| !s.completed)
    }

    /// Mark the current step as completed.
    pub fn complete_current_step(&mut self) -> bool {
        if let Some((idx, _)) = self.current_step() {
            self.steps[idx].completed = true;
            true
        } else {
            false
        }
    }

    /// Whether the tutorial is complete.
    pub fn is_complete(&self) -> bool {
        self.steps.iter().all(|s| s.completed)
    }
}

/// Engine that manages available tutorials.
pub struct TutorialEngine {
    tutorials: Vec<Tutorial>,
}

impl TutorialEngine {
    /// Create a tutorial engine with built-in tutorials.
    pub fn with_builtins() -> Self {
        Self {
            tutorials: Self::builtin_tutorials(),
        }
    }

    /// List available tutorials.
    pub fn list(&self) -> &[Tutorial] {
        &self.tutorials
    }

    /// Get a tutorial by ID.
    pub fn get(&self, id: &str) -> Option<&Tutorial> {
        self.tutorials.iter().find(|t| t.id == id)
    }

    /// Get a mutable tutorial by ID.
    pub fn get_mut(&mut self, id: &str) -> Option<&mut Tutorial> {
        self.tutorials.iter_mut().find(|t| t.id == id)
    }

    fn builtin_tutorials() -> Vec<Tutorial> {
        vec![
            Tutorial {
                id: "first-topic".to_string(),
                title: "Your First Topic".to_string(),
                description: "Learn how to create topics, produce and consume messages.".to_string(),
                difficulty: 1,
                estimated_minutes: 5,
                steps: vec![
                    TutorialStep {
                        title: "Create a topic".to_string(),
                        description: "Create your first topic with 3 partitions.".to_string(),
                        step_type: TutorialStepType::Command {
                            command: "streamline-cli topics create my-first-topic --partitions 3".to_string(),
                        },
                        completed: false,
                    },
                    TutorialStep {
                        title: "Produce a message".to_string(),
                        description: "Send your first message to the topic.".to_string(),
                        step_type: TutorialStepType::Command {
                            command: r#"streamline-cli produce my-first-topic -m "Hello, Streamline!""#.to_string(),
                        },
                        completed: false,
                    },
                    TutorialStep {
                        title: "Consume messages".to_string(),
                        description: "Read the message you just produced.".to_string(),
                        step_type: TutorialStepType::Command {
                            command: "streamline-cli consume my-first-topic --from-beginning".to_string(),
                        },
                        completed: false,
                    },
                ],
                tags: vec!["basics".to_string(), "getting-started".to_string()],
            },
            Tutorial {
                id: "consumer-groups".to_string(),
                title: "Consumer Groups".to_string(),
                description: "Understand how consumer groups enable parallel processing.".to_string(),
                difficulty: 2,
                estimated_minutes: 10,
                steps: vec![
                    TutorialStep {
                        title: "Understanding consumer groups".to_string(),
                        description: "Consumer groups allow multiple consumers to share the work of processing messages. Each partition is assigned to exactly one consumer in the group.".to_string(),
                        step_type: TutorialStepType::Info,
                        completed: false,
                    },
                    TutorialStep {
                        title: "Generate test data".to_string(),
                        description: "Create 100 messages using templates.".to_string(),
                        step_type: TutorialStepType::Command {
                            command: r#"streamline-cli produce events --template '{"id":"{{uuid}}","value":{{random:1:100}}}' -n 100"#.to_string(),
                        },
                        completed: false,
                    },
                    TutorialStep {
                        title: "List consumer groups".to_string(),
                        description: "Check the active consumer groups.".to_string(),
                        step_type: TutorialStepType::Command {
                            command: "streamline-cli groups list".to_string(),
                        },
                        completed: false,
                    },
                ],
                tags: vec!["consumer-groups".to_string(), "intermediate".to_string()],
            },
            Tutorial {
                id: "time-travel".to_string(),
                title: "Time-Travel Debugging".to_string(),
                description: "Learn to consume messages from specific points in time.".to_string(),
                difficulty: 2,
                estimated_minutes: 8,
                steps: vec![
                    TutorialStep {
                        title: "Produce timestamped data".to_string(),
                        description: "Generate messages with timestamps.".to_string(),
                        step_type: TutorialStepType::Command {
                            command: r#"streamline-cli produce logs --template '{"ts":{{timestamp}},"msg":"log-{{i}}"}' -n 50"#.to_string(),
                        },
                        completed: false,
                    },
                    TutorialStep {
                        title: "Consume recent messages".to_string(),
                        description: "Read only messages from the last 5 minutes.".to_string(),
                        step_type: TutorialStepType::Command {
                            command: r#"streamline-cli consume logs --last "5m""#.to_string(),
                        },
                        completed: false,
                    },
                ],
                tags: vec!["time-travel".to_string(), "debugging".to_string()],
            },
            Tutorial {
                id: "sql-analytics".to_string(),
                title: "SQL Analytics on Streams".to_string(),
                description: "Query streaming data with SQL using embedded DuckDB.".to_string(),
                difficulty: 3,
                estimated_minutes: 15,
                steps: vec![
                    TutorialStep {
                        title: "Enable analytics".to_string(),
                        description: "Build with analytics feature: cargo build --features analytics".to_string(),
                        step_type: TutorialStepType::Info,
                        completed: false,
                    },
                    TutorialStep {
                        title: "Query stream data".to_string(),
                        description: "Run a SQL query on a topic.".to_string(),
                        step_type: TutorialStepType::Command {
                            command: r#"streamline-cli query "SELECT * FROM streamline_topic('events') LIMIT 10""#.to_string(),
                        },
                        completed: false,
                    },
                ],
                tags: vec!["analytics".to_string(), "sql".to_string(), "advanced".to_string()],
            },
            Tutorial {
                id: "kafka-migration".to_string(),
                title: "Migrating from Kafka".to_string(),
                description: "Migrate your existing Kafka topics and data to Streamline.".to_string(),
                difficulty: 3,
                estimated_minutes: 20,
                steps: vec![
                    TutorialStep {
                        title: "Understand compatibility".to_string(),
                        description: "Streamline implements 50+ Kafka APIs. Most Kafka clients work unchanged — just point them at Streamline's address.".to_string(),
                        step_type: TutorialStepType::Info,
                        completed: false,
                    },
                    TutorialStep {
                        title: "Dry-run migration".to_string(),
                        description: "Preview what would be migrated without making changes.".to_string(),
                        step_type: TutorialStepType::Command {
                            command: "streamline-cli migrate from-kafka --kafka-servers localhost:9092".to_string(),
                        },
                        completed: false,
                    },
                ],
                tags: vec!["migration".to_string(), "kafka".to_string()],
            },
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tutorial_progress() {
        let engine = TutorialEngine::with_builtins();
        let tutorial = engine.get("first-topic").unwrap();
        assert_eq!(tutorial.progress_pct(), 0.0);
        assert!(!tutorial.is_complete());
    }

    #[test]
    fn test_tutorial_step_completion() {
        let mut engine = TutorialEngine::with_builtins();
        let tutorial = engine.get_mut("first-topic").unwrap();

        assert!(tutorial.complete_current_step());
        assert!(tutorial.progress_pct() > 0.0);
        assert!(!tutorial.is_complete());

        // Complete all steps
        while tutorial.complete_current_step() {}
        assert!(tutorial.is_complete());
        assert_eq!(tutorial.progress_pct(), 100.0);
    }

    #[test]
    fn test_builtin_tutorials() {
        let engine = TutorialEngine::with_builtins();
        assert!(engine.list().len() >= 4);
        assert!(engine.get("first-topic").is_some());
        assert!(engine.get("consumer-groups").is_some());
    }
}

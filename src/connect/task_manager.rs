//! Connector task lifecycle management.
//!
//! Manages the lifecycle of connector tasks: create, start, pause, resume, stop, delete.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use dashmap::DashMap;
use chrono::{DateTime, Utc};

/// State of a connector task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskState {
    Unassigned,
    Running,
    Paused,
    Failed,
    Stopped,
}

/// A connector task instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorTask {
    pub connector_name: String,
    pub task_id: i32,
    pub state: TaskState,
    pub worker_id: String,
    pub config: HashMap<String, String>,
    pub started_at: Option<DateTime<Utc>>,
    pub error_trace: Option<String>,
}

/// Offset tracking for connector tasks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskOffset {
    pub connector_name: String,
    pub task_id: i32,
    pub partition: i32,
    pub offset: i64,
    pub updated_at: DateTime<Utc>,
}

/// Manages connector tasks across the cluster.
pub struct TaskManager {
    tasks: Arc<DashMap<String, Vec<ConnectorTask>>>,
    offsets: Arc<DashMap<String, Vec<TaskOffset>>>,
    worker_id: String,
}

impl TaskManager {
    pub fn new(worker_id: String) -> Self {
        Self {
            tasks: Arc::new(DashMap::new()),
            offsets: Arc::new(DashMap::new()),
            worker_id,
        }
    }

    /// Create tasks for a connector.
    pub fn create_tasks(
        &self,
        connector_name: &str,
        num_tasks: i32,
        config: &HashMap<String, String>,
    ) -> Vec<ConnectorTask> {
        let tasks: Vec<ConnectorTask> = (0..num_tasks)
            .map(|i| ConnectorTask {
                connector_name: connector_name.to_string(),
                task_id: i,
                state: TaskState::Unassigned,
                worker_id: self.worker_id.clone(),
                config: config.clone(),
                started_at: None,
                error_trace: None,
            })
            .collect();

        self.tasks.insert(connector_name.to_string(), tasks.clone());
        tasks
    }

    /// Start all tasks for a connector.
    pub fn start_tasks(&self, connector_name: &str) -> Result<(), String> {
        if let Some(mut tasks) = self.tasks.get_mut(connector_name) {
            for task in tasks.iter_mut() {
                task.state = TaskState::Running;
                task.started_at = Some(Utc::now());
            }
            Ok(())
        } else {
            Err(format!("Connector '{}' not found", connector_name))
        }
    }

    /// Pause all tasks for a connector.
    pub fn pause_tasks(&self, connector_name: &str) -> Result<(), String> {
        if let Some(mut tasks) = self.tasks.get_mut(connector_name) {
            for task in tasks.iter_mut() {
                if task.state == TaskState::Running {
                    task.state = TaskState::Paused;
                }
            }
            Ok(())
        } else {
            Err(format!("Connector '{}' not found", connector_name))
        }
    }

    /// Resume paused tasks for a connector.
    pub fn resume_tasks(&self, connector_name: &str) -> Result<(), String> {
        if let Some(mut tasks) = self.tasks.get_mut(connector_name) {
            for task in tasks.iter_mut() {
                if task.state == TaskState::Paused {
                    task.state = TaskState::Running;
                }
            }
            Ok(())
        } else {
            Err(format!("Connector '{}' not found", connector_name))
        }
    }

    /// Stop all tasks for a connector.
    pub fn stop_tasks(&self, connector_name: &str) -> Result<(), String> {
        if let Some(mut tasks) = self.tasks.get_mut(connector_name) {
            for task in tasks.iter_mut() {
                task.state = TaskState::Stopped;
            }
            Ok(())
        } else {
            Err(format!("Connector '{}' not found", connector_name))
        }
    }

    /// Delete a connector and all its tasks.
    pub fn delete_tasks(&self, connector_name: &str) {
        self.tasks.remove(connector_name);
        self.offsets.remove(connector_name);
    }

    /// Get tasks for a connector.
    pub fn get_tasks(&self, connector_name: &str) -> Option<Vec<ConnectorTask>> {
        self.tasks.get(connector_name).map(|t| t.clone())
    }

    /// Commit offset for a task.
    pub fn commit_offset(&self, connector_name: &str, task_id: i32, partition: i32, offset: i64) {
        let task_offset = TaskOffset {
            connector_name: connector_name.to_string(),
            task_id,
            partition,
            offset,
            updated_at: Utc::now(),
        };

        self.offsets
            .entry(connector_name.to_string())
            .or_insert_with(Vec::new)
            .push(task_offset);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("connector.class".to_string(), "TestConnector".to_string());
        m
    }

    #[test]
    fn test_create_tasks() {
        let mgr = TaskManager::new("worker-1".to_string());
        let tasks = mgr.create_tasks("my-connector", 3, &test_config());
        assert_eq!(tasks.len(), 3);
        assert_eq!(tasks[0].state, TaskState::Unassigned);
        assert_eq!(tasks[2].task_id, 2);
    }

    #[test]
    fn test_start_tasks() {
        let mgr = TaskManager::new("worker-1".to_string());
        mgr.create_tasks("my-connector", 2, &test_config());
        mgr.start_tasks("my-connector").unwrap();
        let tasks = mgr.get_tasks("my-connector").unwrap();
        assert!(tasks.iter().all(|t| t.state == TaskState::Running));
        assert!(tasks.iter().all(|t| t.started_at.is_some()));
    }

    #[test]
    fn test_pause_resume() {
        let mgr = TaskManager::new("worker-1".to_string());
        mgr.create_tasks("my-connector", 1, &test_config());
        mgr.start_tasks("my-connector").unwrap();
        mgr.pause_tasks("my-connector").unwrap();
        let tasks = mgr.get_tasks("my-connector").unwrap();
        assert_eq!(tasks[0].state, TaskState::Paused);
        mgr.resume_tasks("my-connector").unwrap();
        let tasks = mgr.get_tasks("my-connector").unwrap();
        assert_eq!(tasks[0].state, TaskState::Running);
    }

    #[test]
    fn test_stop_tasks() {
        let mgr = TaskManager::new("worker-1".to_string());
        mgr.create_tasks("my-connector", 1, &test_config());
        mgr.start_tasks("my-connector").unwrap();
        mgr.stop_tasks("my-connector").unwrap();
        let tasks = mgr.get_tasks("my-connector").unwrap();
        assert_eq!(tasks[0].state, TaskState::Stopped);
    }

    #[test]
    fn test_delete_tasks() {
        let mgr = TaskManager::new("worker-1".to_string());
        mgr.create_tasks("my-connector", 1, &test_config());
        mgr.delete_tasks("my-connector");
        assert!(mgr.get_tasks("my-connector").is_none());
    }

    #[test]
    fn test_not_found() {
        let mgr = TaskManager::new("worker-1".to_string());
        assert!(mgr.start_tasks("nonexistent").is_err());
    }
}

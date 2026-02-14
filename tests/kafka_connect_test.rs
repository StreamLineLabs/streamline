//! Integration tests for the Kafka Connect REST API compatibility layer
//!
//! Tests cover:
//! - Connector lifecycle (create, get, update, pause, resume, restart, delete)
//! - Task management (list tasks, get task status, restart task)
//! - Plugin discovery (list plugins, validate config)
//! - Error handling (not found, conflict, bad request)
//!
//! These tests exercise the HTTP API handlers through the axum `Router`
//! using `axum::body::Body` and tower's `ServiceExt`.

#[cfg(feature = "kafka-connect")]
mod kafka_connect_tests {
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use http_body_util::BodyExt;
    use serde_json::{json, Value};
    use std::sync::Arc;
    use tower::ServiceExt;

    use streamline::{
        create_connect_router, ConnectApiState, ConnectorManager, ConnectorState,
        ConnectorType, CreateConnectorRequest, TaskState,
    };

    /// Helper: build a test router with a fresh ConnectorManager
    fn test_router() -> axum::Router {
        let state = ConnectApiState {
            connector_manager: Arc::new(ConnectorManager::new()),
        };
        create_connect_router(state)
    }

    /// Helper: send a JSON request and return (status, body as Value)
    async fn send_json(
        app: &axum::Router,
        method: Method,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let body = match body {
            Some(v) => Body::from(serde_json::to_vec(&v).unwrap()),
            None => Body::empty(),
        };

        let request = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(body)
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value: Value = if body_bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&body_bytes).unwrap_or(Value::Null)
        };
        (status, value)
    }

    /// Helper: create a connector with the given name and class
    async fn create_test_connector(app: &axum::Router, name: &str) -> (StatusCode, Value) {
        let body = json!({
            "name": name,
            "config": {
                "connector.class": "io.streamline.connect.file.FileStreamSourceConnector",
                "tasks.max": "2",
                "file": "/tmp/test.txt",
                "topic": "test-topic"
            }
        });
        send_json(app, Method::POST, "/connectors", Some(body)).await
    }

    // =========================================================================
    // Worker info
    // =========================================================================

    #[tokio::test]
    async fn test_get_worker_info() {
        let app = test_router();
        let (status, body) = send_json(&app, Method::GET, "/", None).await;

        assert_eq!(status, StatusCode::OK);
        assert!(body.get("version").is_some());
        assert!(body.get("kafka_cluster_id").is_some());
        assert!(body.get("commit").is_some());
    }

    // =========================================================================
    // Connector CRUD
    // =========================================================================

    #[tokio::test]
    async fn test_list_connectors_empty() {
        let app = test_router();
        let (status, body) = send_json(&app, Method::GET, "/connectors", None).await;

        assert_eq!(status, StatusCode::OK);
        assert!(body.as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_create_connector() {
        let app = test_router();
        let (status, body) = create_test_connector(&app, "test-source").await;

        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body.get("name").unwrap(), "test-source");
        assert!(body.get("tasks").is_some());
        assert_eq!(body.get("tasks").unwrap().as_array().unwrap().len(), 2);
        assert_eq!(body.get("type").unwrap(), "source");
    }

    #[tokio::test]
    async fn test_create_connector_missing_class() {
        let app = test_router();
        let body = json!({
            "name": "bad-connector",
            "config": {
                "tasks.max": "1"
            }
        });
        let (status, _body) = send_json(&app, Method::POST, "/connectors", Some(body)).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_create_connector_duplicate() {
        let app = test_router();
        create_test_connector(&app, "dup-connector").await;

        let (status, body) = create_test_connector(&app, "dup-connector").await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(body.get("message").is_some());
    }

    #[tokio::test]
    async fn test_list_connectors_after_create() {
        let app = test_router();
        create_test_connector(&app, "conn-1").await;
        create_test_connector(&app, "conn-2").await;

        let (status, body) = send_json(&app, Method::GET, "/connectors", None).await;
        assert_eq!(status, StatusCode::OK);
        let names = body.as_array().unwrap();
        assert_eq!(names.len(), 2);
    }

    #[tokio::test]
    async fn test_get_connector() {
        let app = test_router();
        create_test_connector(&app, "my-source").await;

        let (status, body) = send_json(&app, Method::GET, "/connectors/my-source", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.get("name").unwrap(), "my-source");
        assert!(body.get("config").is_some());
        assert!(body.get("tasks").is_some());
    }

    #[tokio::test]
    async fn test_get_connector_not_found() {
        let app = test_router();
        let (status, _body) =
            send_json(&app, Method::GET, "/connectors/nonexistent", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_get_connector_config() {
        let app = test_router();
        create_test_connector(&app, "cfg-test").await;

        let (status, body) =
            send_json(&app, Method::GET, "/connectors/cfg-test/config", None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.get("connector.class").is_some());
        assert!(body.get("tasks.max").is_some());
    }

    #[tokio::test]
    async fn test_get_connector_config_not_found() {
        let app = test_router();
        let (status, _body) =
            send_json(&app, Method::GET, "/connectors/missing/config", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_update_connector_config() {
        let app = test_router();
        create_test_connector(&app, "update-test").await;

        let new_config = json!({
            "connector.class": "io.streamline.connect.file.FileStreamSourceConnector",
            "tasks.max": "4",
            "file": "/tmp/updated.txt",
            "topic": "updated-topic"
        });

        let (status, body) = send_json(
            &app,
            Method::PUT,
            "/connectors/update-test/config",
            Some(new_config),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.get("name").unwrap(), "update-test");
        // After update, tasks.max is 4, so there should be 4 tasks
        assert_eq!(body.get("tasks").unwrap().as_array().unwrap().len(), 4);
    }

    #[tokio::test]
    async fn test_update_connector_config_not_found() {
        let app = test_router();
        let new_config = json!({
            "connector.class": "TestConnector",
            "tasks.max": "1"
        });
        let (status, _) = send_json(
            &app,
            Method::PUT,
            "/connectors/nonexistent/config",
            Some(new_config),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_connector() {
        let app = test_router();
        create_test_connector(&app, "to-delete").await;

        let (status, _) = send_json(&app, Method::DELETE, "/connectors/to-delete", None).await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        // Verify it's gone
        let (status, _) = send_json(&app, Method::GET, "/connectors/to-delete", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_connector_not_found() {
        let app = test_router();
        let (status, _) =
            send_json(&app, Method::DELETE, "/connectors/nonexistent", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    // =========================================================================
    // Connector status
    // =========================================================================

    #[tokio::test]
    async fn test_get_connector_status() {
        let app = test_router();
        create_test_connector(&app, "status-test").await;

        let (status, body) =
            send_json(&app, Method::GET, "/connectors/status-test/status", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.get("name").unwrap(), "status-test");
        assert!(body.get("connector").is_some());
        assert!(body.get("tasks").is_some());
        assert_eq!(
            body.get("connector").unwrap().get("state").unwrap(),
            "RUNNING"
        );
    }

    #[tokio::test]
    async fn test_get_connector_status_not_found() {
        let app = test_router();
        let (status, _) =
            send_json(&app, Method::GET, "/connectors/missing/status", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    // =========================================================================
    // Connector lifecycle: pause, resume, restart
    // =========================================================================

    #[tokio::test]
    async fn test_pause_connector() {
        let app = test_router();
        create_test_connector(&app, "pause-test").await;

        let (status, _) =
            send_json(&app, Method::PUT, "/connectors/pause-test/pause", None).await;
        assert_eq!(status, StatusCode::ACCEPTED);

        // Verify paused state
        let (_, body) =
            send_json(&app, Method::GET, "/connectors/pause-test/status", None).await;
        assert_eq!(
            body.get("connector").unwrap().get("state").unwrap(),
            "PAUSED"
        );
        // All tasks should be paused
        for task in body.get("tasks").unwrap().as_array().unwrap() {
            assert_eq!(task.get("state").unwrap(), "PAUSED");
        }
    }

    #[tokio::test]
    async fn test_pause_connector_not_found() {
        let app = test_router();
        let (status, _) =
            send_json(&app, Method::PUT, "/connectors/missing/pause", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_resume_connector() {
        let app = test_router();
        create_test_connector(&app, "resume-test").await;

        // Pause first
        send_json(&app, Method::PUT, "/connectors/resume-test/pause", None).await;

        // Resume
        let (status, _) =
            send_json(&app, Method::PUT, "/connectors/resume-test/resume", None).await;
        assert_eq!(status, StatusCode::ACCEPTED);

        // Verify running state
        let (_, body) =
            send_json(&app, Method::GET, "/connectors/resume-test/status", None).await;
        assert_eq!(
            body.get("connector").unwrap().get("state").unwrap(),
            "RUNNING"
        );
    }

    #[tokio::test]
    async fn test_resume_connector_not_found() {
        let app = test_router();
        let (status, _) =
            send_json(&app, Method::PUT, "/connectors/missing/resume", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_restart_connector() {
        let app = test_router();
        create_test_connector(&app, "restart-test").await;

        let (status, _) =
            send_json(&app, Method::POST, "/connectors/restart-test/restart", None).await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        // Verify still running after restart
        let (_, body) =
            send_json(&app, Method::GET, "/connectors/restart-test/status", None).await;
        assert_eq!(
            body.get("connector").unwrap().get("state").unwrap(),
            "RUNNING"
        );
    }

    #[tokio::test]
    async fn test_restart_connector_not_found() {
        let app = test_router();
        let (status, _) =
            send_json(&app, Method::POST, "/connectors/missing/restart", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_restart_with_include_tasks() {
        let app = test_router();
        create_test_connector(&app, "restart-tasks-test").await;

        let (status, _) = send_json(
            &app,
            Method::POST,
            "/connectors/restart-tasks-test/restart?include_tasks=true",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
    }

    // =========================================================================
    // Task management
    // =========================================================================

    #[tokio::test]
    async fn test_get_connector_tasks() {
        let app = test_router();
        create_test_connector(&app, "tasks-test").await;

        let (status, body) =
            send_json(&app, Method::GET, "/connectors/tasks-test/tasks", None).await;
        assert_eq!(status, StatusCode::OK);
        let tasks = body.as_array().unwrap();
        assert_eq!(tasks.len(), 2); // tasks.max = 2 in our test connector
        // Each task should have an id and config
        for task in tasks {
            assert!(task.get("id").is_some());
            assert!(task.get("config").is_some());
        }
    }

    #[tokio::test]
    async fn test_get_connector_tasks_not_found() {
        let app = test_router();
        let (status, _) =
            send_json(&app, Method::GET, "/connectors/missing/tasks", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_get_task_status() {
        let app = test_router();
        create_test_connector(&app, "task-status-test").await;

        let (status, body) = send_json(
            &app,
            Method::GET,
            "/connectors/task-status-test/tasks/0/status",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.get("id").unwrap(), 0);
        assert_eq!(body.get("state").unwrap(), "RUNNING");
        assert!(body.get("worker_id").is_some());
    }

    #[tokio::test]
    async fn test_get_task_status_invalid_task_id() {
        let app = test_router();
        create_test_connector(&app, "task-invalid-test").await;

        let (status, _) = send_json(
            &app,
            Method::GET,
            "/connectors/task-invalid-test/tasks/99/status",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_restart_task() {
        let app = test_router();
        create_test_connector(&app, "task-restart-test").await;

        let (status, _) = send_json(
            &app,
            Method::POST,
            "/connectors/task-restart-test/tasks/0/restart",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_restart_task_not_found() {
        let app = test_router();
        let (status, _) = send_json(
            &app,
            Method::POST,
            "/connectors/missing/tasks/0/restart",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    // =========================================================================
    // Plugin discovery
    // =========================================================================

    #[tokio::test]
    async fn test_list_connector_plugins() {
        let app = test_router();
        let (status, body) = send_json(&app, Method::GET, "/connector-plugins", None).await;

        assert_eq!(status, StatusCode::OK);
        let plugins = body.as_array().unwrap();
        assert!(!plugins.is_empty());
        // Each plugin should have class, type, version
        for plugin in plugins {
            assert!(plugin.get("class").is_some());
            assert!(plugin.get("type").is_some());
            assert!(plugin.get("version").is_some());
        }
    }

    #[tokio::test]
    async fn test_validate_connector_config() {
        let app = test_router();
        let config = json!({
            "connector.class": "io.streamline.connect.file.FileStreamSourceConnector",
            "tasks.max": "1",
            "topics": "test-topic"
        });

        let (status, body) = send_json(
            &app,
            Method::PUT,
            "/connector-plugins/FileStreamSourceConnector/config/validate",
            Some(config),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert!(body.get("error_count").is_some());
        assert!(body.get("configs").is_some());
        assert_eq!(body.get("error_count").unwrap(), 0);
    }

    #[tokio::test]
    async fn test_validate_connector_config_missing_required() {
        let app = test_router();
        let config = json!({});

        let (status, body) = send_json(
            &app,
            Method::PUT,
            "/connector-plugins/SomePlugin/config/validate",
            Some(config),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        // Should have errors for missing connector.class and tasks.max
        assert!(body.get("error_count").unwrap().as_i64().unwrap() > 0);
    }

    // =========================================================================
    // Health monitoring
    // =========================================================================

    #[tokio::test]
    async fn test_connectors_health_empty() {
        let app = test_router();
        let (status, body) = send_json(&app, Method::GET, "/connectors/health", None).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.get("total_connectors").unwrap(), 0);
        assert_eq!(body.get("status").unwrap(), "healthy");
    }

    #[tokio::test]
    async fn test_connectors_health_with_connectors() {
        let app = test_router();
        create_test_connector(&app, "health-1").await;
        create_test_connector(&app, "health-2").await;

        let (status, body) = send_json(&app, Method::GET, "/connectors/health", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.get("total_connectors").unwrap(), 2);
        assert_eq!(body.get("healthy").unwrap(), 2);
        assert_eq!(body.get("unhealthy").unwrap(), 0);
    }

    // =========================================================================
    // Full connector lifecycle integration test
    // =========================================================================

    #[tokio::test]
    async fn test_full_connector_lifecycle() {
        let app = test_router();

        // 1. List connectors -- empty
        let (status, body) = send_json(&app, Method::GET, "/connectors", None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.as_array().unwrap().is_empty());

        // 2. Create connector
        let (status, body) = create_test_connector(&app, "lifecycle-conn").await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body.get("name").unwrap(), "lifecycle-conn");

        // 3. List connectors -- should have one
        let (_, body) = send_json(&app, Method::GET, "/connectors", None).await;
        assert_eq!(body.as_array().unwrap().len(), 1);

        // 4. Get connector info
        let (status, body) =
            send_json(&app, Method::GET, "/connectors/lifecycle-conn", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.get("name").unwrap(), "lifecycle-conn");

        // 5. Get connector config
        let (status, body) =
            send_json(&app, Method::GET, "/connectors/lifecycle-conn/config", None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.get("connector.class").is_some());

        // 6. Get connector status -- RUNNING
        let (status, body) =
            send_json(&app, Method::GET, "/connectors/lifecycle-conn/status", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body.get("connector").unwrap().get("state").unwrap(),
            "RUNNING"
        );

        // 7. Pause
        let (status, _) =
            send_json(&app, Method::PUT, "/connectors/lifecycle-conn/pause", None).await;
        assert_eq!(status, StatusCode::ACCEPTED);

        // 8. Verify paused
        let (_, body) =
            send_json(&app, Method::GET, "/connectors/lifecycle-conn/status", None).await;
        assert_eq!(
            body.get("connector").unwrap().get("state").unwrap(),
            "PAUSED"
        );

        // 9. Resume
        let (status, _) =
            send_json(&app, Method::PUT, "/connectors/lifecycle-conn/resume", None).await;
        assert_eq!(status, StatusCode::ACCEPTED);

        // 10. Verify running again
        let (_, body) =
            send_json(&app, Method::GET, "/connectors/lifecycle-conn/status", None).await;
        assert_eq!(
            body.get("connector").unwrap().get("state").unwrap(),
            "RUNNING"
        );

        // 11. Update config (change tasks.max)
        let new_config = json!({
            "connector.class": "io.streamline.connect.file.FileStreamSourceConnector",
            "tasks.max": "3",
            "file": "/tmp/updated.txt",
            "topic": "updated-topic"
        });
        let (status, body) = send_json(
            &app,
            Method::PUT,
            "/connectors/lifecycle-conn/config",
            Some(new_config),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.get("tasks").unwrap().as_array().unwrap().len(), 3);

        // 12. Get tasks
        let (status, body) =
            send_json(&app, Method::GET, "/connectors/lifecycle-conn/tasks", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.as_array().unwrap().len(), 3);

        // 13. Get task 0 status
        let (status, body) = send_json(
            &app,
            Method::GET,
            "/connectors/lifecycle-conn/tasks/0/status",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.get("id").unwrap(), 0);
        assert_eq!(body.get("state").unwrap(), "RUNNING");

        // 14. Restart task 1
        let (status, _) = send_json(
            &app,
            Method::POST,
            "/connectors/lifecycle-conn/tasks/1/restart",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        // 15. Restart connector
        let (status, _) = send_json(
            &app,
            Method::POST,
            "/connectors/lifecycle-conn/restart",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        // 16. Delete connector
        let (status, _) =
            send_json(&app, Method::DELETE, "/connectors/lifecycle-conn", None).await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        // 17. Verify deleted
        let (status, _) =
            send_json(&app, Method::GET, "/connectors/lifecycle-conn", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // 18. List connectors -- empty again
        let (_, body) = send_json(&app, Method::GET, "/connectors", None).await;
        assert!(body.as_array().unwrap().is_empty());
    }

    // =========================================================================
    // Connector type detection
    // =========================================================================

    #[tokio::test]
    async fn test_connector_type_source() {
        let app = test_router();
        let body = json!({
            "name": "type-source-test",
            "config": {
                "connector.class": "io.streamline.connect.file.FileStreamSourceConnector",
                "tasks.max": "1"
            }
        });
        let (status, response) = send_json(&app, Method::POST, "/connectors", Some(body)).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(response.get("type").unwrap(), "source");
    }

    #[tokio::test]
    async fn test_connector_type_sink() {
        let app = test_router();
        let body = json!({
            "name": "type-sink-test",
            "config": {
                "connector.class": "io.streamline.connect.file.FileStreamSinkConnector",
                "tasks.max": "1"
            }
        });
        let (status, response) = send_json(&app, Method::POST, "/connectors", Some(body)).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(response.get("type").unwrap(), "sink");
    }

    // =========================================================================
    // Dead letter queue
    // =========================================================================

    #[tokio::test]
    async fn test_get_dlq_not_found() {
        let app = test_router();
        let (status, _) =
            send_json(&app, Method::GET, "/connectors/missing/dlq", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_get_dlq_empty() {
        let app = test_router();
        create_test_connector(&app, "dlq-test").await;

        let (status, body) =
            send_json(&app, Method::GET, "/connectors/dlq-test/dlq", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.get("connector").unwrap(), "dlq-test");
        assert!(body.get("entries").unwrap().as_array().unwrap().is_empty());
        assert_eq!(body.get("total").unwrap(), 0);
    }

    // =========================================================================
    // Unit tests for ConnectorManager internals
    // =========================================================================

    #[test]
    fn test_connector_manager_new() {
        let manager = ConnectorManager::new();
        assert!(manager.list_connectors().is_empty());
        assert!(!manager.get_plugins().is_empty());
    }

    #[test]
    fn test_connector_manager_create_and_get() {
        let manager = ConnectorManager::new();

        let mut config = std::collections::HashMap::new();
        config.insert(
            "connector.class".to_string(),
            "io.streamline.connect.file.FileStreamSourceConnector".to_string(),
        );
        config.insert("tasks.max".to_string(), "2".to_string());

        let request = CreateConnectorRequest {
            name: "test-conn".to_string(),
            config,
        };

        let info = manager.create_connector(request).unwrap();
        assert_eq!(info.name, "test-conn");
        assert_eq!(info.tasks.len(), 2);
        assert_eq!(info.connector_type, ConnectorType::Source);

        // Get connector
        let fetched = manager.get_connector("test-conn").unwrap();
        assert_eq!(fetched.name, "test-conn");

        // List connectors
        let names = manager.list_connectors();
        assert_eq!(names.len(), 1);
        assert!(names.contains(&"test-conn".to_string()));
    }

    #[test]
    fn test_connector_manager_pause_resume() {
        let manager = ConnectorManager::new();

        let mut config = std::collections::HashMap::new();
        config.insert(
            "connector.class".to_string(),
            "TestSinkConnector".to_string(),
        );
        config.insert("tasks.max".to_string(), "1".to_string());

        let request = CreateConnectorRequest {
            name: "pause-test".to_string(),
            config,
        };
        manager.create_connector(request).unwrap();

        // Pause
        manager.pause_connector("pause-test").unwrap();
        let status = manager.get_connector_status("pause-test").unwrap();
        assert_eq!(status.connector.state, ConnectorState::Paused);

        // Resume
        manager.resume_connector("pause-test").unwrap();
        let status = manager.get_connector_status("pause-test").unwrap();
        assert_eq!(status.connector.state, ConnectorState::Running);
    }

    #[test]
    fn test_connector_manager_delete() {
        let manager = ConnectorManager::new();

        let mut config = std::collections::HashMap::new();
        config.insert(
            "connector.class".to_string(),
            "TestConnector".to_string(),
        );
        config.insert("tasks.max".to_string(), "1".to_string());

        let request = CreateConnectorRequest {
            name: "delete-test".to_string(),
            config,
        };
        manager.create_connector(request).unwrap();
        assert!(manager.get_connector("delete-test").is_some());

        manager.delete_connector("delete-test").unwrap();
        assert!(manager.get_connector("delete-test").is_none());
    }

    #[test]
    fn test_connector_manager_delete_not_found() {
        let manager = ConnectorManager::new();
        let result = manager.delete_connector("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_connector_manager_task_status() {
        let manager = ConnectorManager::new();

        let mut config = std::collections::HashMap::new();
        config.insert(
            "connector.class".to_string(),
            "TestSourceConnector".to_string(),
        );
        config.insert("tasks.max".to_string(), "3".to_string());

        let request = CreateConnectorRequest {
            name: "task-test".to_string(),
            config,
        };
        manager.create_connector(request).unwrap();

        // Get task status for each task
        for i in 0..3 {
            let status = manager.get_task_status("task-test", i).unwrap();
            assert_eq!(status.id, i);
            assert_eq!(status.state, TaskState::Running);
        }

        // Invalid task id
        let result = manager.get_task_status("task-test", 99);
        assert!(result.is_err());
    }

    #[test]
    fn test_connector_manager_restart_task() {
        let manager = ConnectorManager::new();

        let mut config = std::collections::HashMap::new();
        config.insert(
            "connector.class".to_string(),
            "TestConnector".to_string(),
        );
        config.insert("tasks.max".to_string(), "2".to_string());

        let request = CreateConnectorRequest {
            name: "restart-task-test".to_string(),
            config,
        };
        manager.create_connector(request).unwrap();

        // Pause to make tasks PAUSED
        manager.pause_connector("restart-task-test").unwrap();
        let task_status = manager.get_task_status("restart-task-test", 0).unwrap();
        assert_eq!(task_status.state, TaskState::Paused);

        // Restart individual task
        manager.restart_task("restart-task-test", 0).unwrap();
        let task_status = manager.get_task_status("restart-task-test", 0).unwrap();
        assert_eq!(task_status.state, TaskState::Running);
    }

    #[test]
    fn test_connector_manager_config_validation() {
        let manager = ConnectorManager::new();

        // Valid config
        let mut config = std::collections::HashMap::new();
        config.insert("connector.class".to_string(), "TestConnector".to_string());
        config.insert("tasks.max".to_string(), "1".to_string());
        let result = manager.validate_config("TestConnector", config);
        assert_eq!(result.error_count, 0);

        // Missing required fields
        let config = std::collections::HashMap::new();
        let result = manager.validate_config("TestConnector", config);
        assert!(result.error_count > 0);
    }

    #[test]
    fn test_connector_manager_health_check() {
        let manager = ConnectorManager::new();

        // Empty: should be healthy
        let report = manager.health_check();
        assert_eq!(report.status, "healthy");
        assert_eq!(report.total_connectors, 0);

        // Add connector
        let mut config = std::collections::HashMap::new();
        config.insert(
            "connector.class".to_string(),
            "TestConnector".to_string(),
        );
        config.insert("tasks.max".to_string(), "1".to_string());
        let request = CreateConnectorRequest {
            name: "health-conn".to_string(),
            config,
        };
        manager.create_connector(request).unwrap();

        let report = manager.health_check();
        assert_eq!(report.total_connectors, 1);
        assert_eq!(report.healthy, 1);
        assert_eq!(report.unhealthy, 0);

        // Pause: should still be healthy (paused is not unhealthy)
        manager.pause_connector("health-conn").unwrap();
        let report = manager.health_check();
        assert_eq!(report.paused, 1);
    }

    #[test]
    fn test_connector_manager_update_config() {
        let manager = ConnectorManager::new();

        let mut config = std::collections::HashMap::new();
        config.insert(
            "connector.class".to_string(),
            "TestConnector".to_string(),
        );
        config.insert("tasks.max".to_string(), "1".to_string());

        let request = CreateConnectorRequest {
            name: "config-update-test".to_string(),
            config,
        };
        manager.create_connector(request).unwrap();

        // Update config
        let mut new_config = std::collections::HashMap::new();
        new_config.insert(
            "connector.class".to_string(),
            "TestConnector".to_string(),
        );
        new_config.insert("tasks.max".to_string(), "5".to_string());

        let info = manager
            .update_connector_config("config-update-test", new_config)
            .unwrap();
        assert_eq!(info.tasks.len(), 5);
    }

    #[test]
    fn test_connector_manager_worker_info() {
        let manager = ConnectorManager::new();
        let info = manager.worker_info();
        assert_eq!(info.kafka_cluster_id, "streamline-cluster");
        assert!(!info.version.is_empty());
    }

    // =========================================================================
    // ConnectorState and TaskState serialization
    // =========================================================================

    #[test]
    fn test_connector_state_serialization() {
        let running = serde_json::to_string(&ConnectorState::Running).unwrap();
        assert_eq!(running, "\"RUNNING\"");

        let paused = serde_json::to_string(&ConnectorState::Paused).unwrap();
        assert_eq!(paused, "\"PAUSED\"");

        let failed = serde_json::to_string(&ConnectorState::Failed).unwrap();
        assert_eq!(failed, "\"FAILED\"");

        let unassigned = serde_json::to_string(&ConnectorState::Unassigned).unwrap();
        assert_eq!(unassigned, "\"UNASSIGNED\"");

        let restarting = serde_json::to_string(&ConnectorState::Restarting).unwrap();
        assert_eq!(restarting, "\"RESTARTING\"");
    }

    #[test]
    fn test_task_state_serialization() {
        let running = serde_json::to_string(&TaskState::Running).unwrap();
        assert_eq!(running, "\"RUNNING\"");

        let paused = serde_json::to_string(&TaskState::Paused).unwrap();
        assert_eq!(paused, "\"PAUSED\"");

        let failed = serde_json::to_string(&TaskState::Failed).unwrap();
        assert_eq!(failed, "\"FAILED\"");

        let unassigned = serde_json::to_string(&TaskState::Unassigned).unwrap();
        assert_eq!(unassigned, "\"UNASSIGNED\"");
    }

    #[test]
    fn test_connector_state_display() {
        assert_eq!(ConnectorState::Running.to_string(), "RUNNING");
        assert_eq!(ConnectorState::Paused.to_string(), "PAUSED");
        assert_eq!(ConnectorState::Failed.to_string(), "FAILED");
        assert_eq!(ConnectorState::Unassigned.to_string(), "UNASSIGNED");
        assert_eq!(ConnectorState::Restarting.to_string(), "RESTARTING");
    }

    #[test]
    fn test_task_state_display() {
        assert_eq!(TaskState::Running.to_string(), "RUNNING");
        assert_eq!(TaskState::Paused.to_string(), "PAUSED");
        assert_eq!(TaskState::Failed.to_string(), "FAILED");
        assert_eq!(TaskState::Unassigned.to_string(), "UNASSIGNED");
    }
}

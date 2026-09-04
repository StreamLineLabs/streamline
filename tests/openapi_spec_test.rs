//! Structural regression tests for the generated OpenAPI document.

use serde_yaml::{Mapping, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn key(name: &str) -> Value {
    Value::String(name.to_string())
}

fn mapping_value<'a>(mapping: &'a Mapping, name: &str) -> Option<&'a Value> {
    mapping.get(key(name))
}

fn resolve_pointer<'a>(root: &'a Value, pointer: &str) -> Option<&'a Value> {
    let mut current = root;
    for token in pointer.strip_prefix("#/")?.split('/') {
        let token = token.replace("~1", "/").replace("~0", "~");
        current = mapping_value(current.as_mapping()?, &token)?;
    }
    Some(current)
}

fn operation<'a>(document: &'a Value, path: &str, method: &str) -> &'a Mapping {
    document
        .as_mapping()
        .and_then(|mapping| mapping_value(mapping, "paths"))
        .and_then(Value::as_mapping)
        .and_then(|paths| mapping_value(paths, path))
        .and_then(Value::as_mapping)
        .and_then(|path_item| mapping_value(path_item, method))
        .and_then(Value::as_mapping)
        .unwrap_or_else(|| panic!("missing mounted operation {method} {path}"))
}

fn schema_ref<'a>(schema: &'a Value, context: &str) -> &'a str {
    schema
        .as_mapping()
        .and_then(|mapping| mapping_value(mapping, "$ref"))
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("{context} must use a component $ref"))
}

fn request_schema<'a>(operation: &'a Mapping, context: &str) -> &'a Value {
    mapping_value(operation, "requestBody")
        .and_then(Value::as_mapping)
        .and_then(|body| mapping_value(body, "content"))
        .and_then(Value::as_mapping)
        .and_then(|content| mapping_value(content, "application/json"))
        .and_then(Value::as_mapping)
        .and_then(|json| mapping_value(json, "schema"))
        .unwrap_or_else(|| panic!("{context} must define a JSON request schema"))
}

fn response_schema<'a>(operation: &'a Mapping, context: &str) -> &'a Value {
    mapping_value(operation, "responses")
        .and_then(Value::as_mapping)
        .and_then(|responses| mapping_value(responses, "200"))
        .and_then(Value::as_mapping)
        .and_then(|response| mapping_value(response, "content"))
        .and_then(Value::as_mapping)
        .and_then(|content| mapping_value(content, "application/json"))
        .and_then(Value::as_mapping)
        .and_then(|json| mapping_value(json, "schema"))
        .unwrap_or_else(|| panic!("{context} must define a JSON success schema"))
}

fn component<'a>(document: &'a Value, name: &str) -> &'a Mapping {
    document
        .as_mapping()
        .and_then(|mapping| mapping_value(mapping, "components"))
        .and_then(Value::as_mapping)
        .and_then(|components| mapping_value(components, "schemas"))
        .and_then(Value::as_mapping)
        .and_then(|schemas| mapping_value(schemas, name))
        .and_then(Value::as_mapping)
        .unwrap_or_else(|| panic!("missing component schema {name}"))
}

fn property<'a>(schema: &'a Mapping, name: &str) -> &'a Value {
    mapping_value(schema, "properties")
        .and_then(Value::as_mapping)
        .and_then(|properties| mapping_value(properties, name))
        .unwrap_or_else(|| panic!("missing property {name}"))
}

fn required_fields(schema: &Mapping) -> Vec<&str> {
    mapping_value(schema, "required")
        .and_then(Value::as_sequence)
        .into_iter()
        .flatten()
        .map(|value| {
            value
                .as_str()
                .expect("required field names must be strings")
        })
        .collect()
}

fn tagged_variant<'a>(schema: &'a Mapping, tag: &str, value: &str) -> &'a Mapping {
    mapping_value(schema, "oneOf")
        .and_then(Value::as_sequence)
        .and_then(|variants| {
            variants.iter().find_map(|variant| {
                let variant = variant.as_mapping()?;
                let values = property(variant, tag)
                    .as_mapping()
                    .and_then(|tag_schema| mapping_value(tag_schema, "enum"))
                    .and_then(Value::as_sequence)?;
                values.contains(&key(value)).then_some(variant)
            })
        })
        .unwrap_or_else(|| panic!("missing `{tag}: {value}` tagged variant"))
}

fn validate_nodes(root: &Value, value: &Value, path: &str) {
    match value {
        Value::Mapping(mapping) => {
            if let Some(reference) = mapping_value(mapping, "$ref") {
                assert_eq!(
                    mapping.len(),
                    1,
                    "{path}: OpenAPI 3.0 forbids siblings next to $ref"
                );
                let reference = reference
                    .as_str()
                    .unwrap_or_else(|| panic!("{path}: $ref must be a string"));
                assert!(
                    !['<', '>', '&', '\'', ' ']
                        .iter()
                        .any(|character| reference.contains(*character))
                        && !reference.contains("::"),
                    "{path}: $ref exposes a Rust type instead of an OpenAPI schema: {reference}"
                );
                if reference.starts_with("#/") {
                    assert!(
                        resolve_pointer(root, reference).is_some(),
                        "{path}: unresolved local $ref {reference}"
                    );
                }
            }

            if let Some(additional) = mapping_value(mapping, "additionalProperties") {
                assert!(
                    matches!(additional, Value::Bool(_) | Value::Mapping(_)),
                    "{path}: additionalProperties must be a boolean or schema"
                );
            }

            if path.ends_with(".properties") {
                for (name, schema) in mapping {
                    assert!(
                        matches!(schema, Value::Mapping(_)),
                        "{path}.{} must contain a concrete schema",
                        name.as_str().unwrap_or("<non-string property>")
                    );
                }
            }

            for (name, child) in mapping {
                let name = name.as_str().unwrap_or("<non-string key>");
                validate_nodes(root, child, &format!("{path}.{name}"));
            }
        }
        Value::Sequence(sequence) => {
            for (index, child) in sequence.iter().enumerate() {
                validate_nodes(root, child, &format!("{path}[{index}]"));
            }
        }
        _ => {}
    }
}

#[test]
fn generated_openapi_has_unique_operations_and_resolved_schemas() {
    let spec_path = repo_root().join("openapi/streamline-api-v1.yaml");
    let source = std::fs::read_to_string(&spec_path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", spec_path.display()));
    let document: Value =
        serde_yaml::from_str(&source).expect("OpenAPI document must be valid YAML");

    let paths = document
        .as_mapping()
        .and_then(|mapping| mapping_value(mapping, "paths"))
        .and_then(Value::as_mapping)
        .expect("OpenAPI document must define paths");

    let mut operations = BTreeMap::<String, String>::new();
    for (path, path_item) in paths {
        let path = path.as_str().expect("path keys must be strings");
        let path_item = path_item.as_mapping().expect("path item must be a mapping");
        for method in [
            "get", "put", "post", "delete", "patch", "options", "head", "trace",
        ] {
            let Some(operation) = mapping_value(path_item, method) else {
                continue;
            };
            let operation_id = operation
                .as_mapping()
                .and_then(|mapping| mapping_value(mapping, "operationId"))
                .and_then(Value::as_str)
                .unwrap_or_else(|| panic!("{method} {path} must define operationId"));
            if let Some(previous) =
                operations.insert(operation_id.to_string(), format!("{method} {path}"))
            {
                panic!("duplicate operationId `{operation_id}` for {previous} and {method} {path}");
            }
        }
    }

    assert!(
        operations.len() >= 250,
        "generated OpenAPI unexpectedly lost mounted operations"
    );
    validate_nodes(&document, &document, "$");
}

#[test]
fn generated_openapi_includes_representative_mounted_routers() {
    let source = std::fs::read_to_string(repo_root().join("openapi/streamline-api-v1.yaml"))
        .expect("OpenAPI document must exist");
    let document: Value = serde_yaml::from_str(&source).expect("OpenAPI document must be YAML");

    for (path, method) in [
        ("/metrics", "get"),
        ("/metrics/jmx", "get"),
        ("/api/v1/query", "post"),
        ("/api/v1/query/explain", "post"),
        ("/api/v1/branches", "post"),
        ("/api/v1/memory/remember", "post"),
        ("/api/v1/memory/recall", "post"),
        ("/api/v1/sqlite/query", "post"),
        ("/api/v1/topics/{topic}/search", "post"),
        ("/scaling/metrics", "get"),
    ] {
        operation(&document, path, method);
    }

    for path in ["/metrics", "/metrics/jmx"] {
        let description = mapping_value(operation(&document, path, "get"), "description")
            .and_then(Value::as_str)
            .expect("metrics routes must retain their feature gate");
        assert!(description.contains("metrics"));
    }
}

#[test]
fn generated_openapi_keeps_colliding_dtos_module_specific() {
    let source = std::fs::read_to_string(repo_root().join("openapi/streamline-api-v1.yaml"))
        .expect("OpenAPI document must exist");
    let document: Value = serde_yaml::from_str(&source).expect("OpenAPI document must be YAML");

    let topic_search = operation(&document, "/api/v1/topics/{topic}/search", "post");
    assert_eq!(
        schema_ref(
            request_schema(topic_search, "topic search"),
            "topic search request"
        ),
        "#/components/schemas/SearchApiSearchRequest"
    );
    assert_eq!(
        schema_ref(
            response_schema(topic_search, "topic search"),
            "topic search response"
        ),
        "#/components/schemas/SearchApiSearchResponse"
    );

    let ai_search = operation(&document, "/api/v1/ai/search", "post");
    assert_eq!(
        schema_ref(request_schema(ai_search, "AI search"), "AI search request"),
        "#/components/schemas/AiApiSearchRequest"
    );
    assert!(property(component(&document, "SearchApiSearchRequest"), "k").is_mapping());
    assert!(property(component(&document, "AiApiSearchRequest"), "limit").is_mapping());

    let function = operation(&document, "/api/v1/functions/{name}", "get");
    assert_eq!(
        schema_ref(
            response_schema(function, "function lookup"),
            "function response"
        ),
        "#/components/schemas/WasmApiFunctionResponse"
    );
    let function_schema = component(&document, "WasmApiFunctionResponse");
    assert_eq!(
        schema_ref(property(function_schema, "status"), "function status"),
        "#/components/schemas/WasmFunctionRegistryFunctionStatus"
    );
    assert_eq!(
        schema_ref(property(function_schema, "metrics"), "function metrics"),
        "#/components/schemas/WasmFunctionRegistryFunctionMetrics"
    );

    let alert_config = component(&document, "AlertConfig");
    let alert_condition = property(alert_config, "condition")
        .as_mapping()
        .and_then(|condition| mapping_value(condition, "allOf"))
        .and_then(Value::as_sequence)
        .and_then(|items| items.first())
        .expect("AlertConfig.condition must wrap its source-specific schema");
    assert_eq!(
        schema_ref(alert_condition, "alert condition"),
        "#/components/schemas/AlertsAlertCondition"
    );

    let observability_rule = component(&document, "AlertRuleConfig");
    assert_eq!(
        schema_ref(
            property(observability_rule, "condition"),
            "observability alert condition"
        ),
        "#/components/schemas/ObservabilityApiAlertCondition"
    );
}

#[test]
fn connect_routes_use_module_level_handler_request_bodies() {
    let source = std::fs::read_to_string(repo_root().join("openapi/streamline-api-v1.yaml"))
        .expect("OpenAPI document must exist");
    let document: Value = serde_yaml::from_str(&source).expect("OpenAPI document must be YAML");

    let create = operation(&document, "/connectors", "post");
    assert_eq!(
        schema_ref(
            request_schema(create, "create connector"),
            "create connector"
        ),
        "#/components/schemas/ConnectApiCreateConnectorRequest"
    );

    let update = request_schema(
        operation(&document, "/connectors/{name}/config", "put"),
        "update connector config",
    )
    .as_mapping()
    .expect("connector config request must be an object schema");
    assert_eq!(
        mapping_value(update, "type").and_then(Value::as_str),
        Some("object")
    );
    let values = mapping_value(update, "additionalProperties")
        .and_then(Value::as_mapping)
        .expect("connector config values must have a schema");
    assert_eq!(
        mapping_value(values, "type").and_then(Value::as_str),
        Some("string")
    );

    let offsets = operation(&document, "/connectors/{name}/offsets", "patch");
    assert_eq!(
        schema_ref(
            request_schema(offsets, "alter connector offsets"),
            "alter connector offsets"
        ),
        "#/components/schemas/AlterConnectorOffsetsRequest"
    );
}

#[test]
fn generated_schemas_preserve_serde_variant_and_optional_field_contracts() {
    let source = std::fs::read_to_string(repo_root().join("openapi/streamline-api-v1.yaml"))
        .expect("OpenAPI document must exist");
    let document: Value = serde_yaml::from_str(&source).expect("OpenAPI document must be YAML");

    let direction = component(&document, "MarketplaceDeclarativeConnectorDirection");
    let direction_values = mapping_value(direction, "enum")
        .and_then(Value::as_sequence)
        .expect("ConnectorDirection must define serialized variants");
    assert_eq!(
        direction_values,
        &[key("source"), key("sink")],
        "variant-level serde renames must be preserved"
    );

    let benchmark = component(&document, "BenchmarkApiBenchmark");
    let required = mapping_value(benchmark, "required")
        .and_then(Value::as_sequence)
        .expect("Benchmark must define required fields");
    assert!(!required.contains(&key("results")));
    assert!(!required.contains(&key("completed_at")));
    assert!(property(benchmark, "results").is_mapping());
    assert!(property(benchmark, "completed_at").is_mapping());

    let status = component(&document, "WasmFunctionRegistryFunctionStatus");
    let states = mapping_value(status, "oneOf")
        .and_then(Value::as_sequence)
        .expect("FunctionStatus must expose tagged serialized states")
        .iter()
        .map(|variant| {
            property(
                variant
                    .as_mapping()
                    .expect("FunctionStatus variants must be objects"),
                "state",
            )
            .as_mapping()
            .and_then(|state| mapping_value(state, "enum"))
            .and_then(Value::as_sequence)
            .and_then(|values| values.first())
            .and_then(Value::as_str)
            .expect("FunctionStatus variants must define one serialized state")
        })
        .collect::<Vec<_>>();
    assert!(states.contains(&"stopped"));
    assert!(!states.contains(&"deployed"));
}

#[test]
fn generated_schemas_match_existing_serde_container_and_flattened_payloads() {
    let source = std::fs::read_to_string(repo_root().join("openapi/streamline-api-v1.yaml"))
        .expect("OpenAPI document must exist");
    let document: Value = serde_yaml::from_str(&source).expect("OpenAPI document must be YAML");

    let reconnect = component(&document, "ReconnectConfig");
    assert_eq!(
        required_fields(reconnect),
        ["initialDelayMs", "maxDelayMs", "multiplier", "maxAttempts"]
    );
    for field in ["initialDelayMs", "maxDelayMs", "multiplier", "maxAttempts"] {
        assert!(
            property(reconnect, field).is_mapping(),
            "ReconnectConfig must expose its camelCase `{field}` wire field"
        );
    }
    assert!(
        mapping_value(
            mapping_value(reconnect, "properties")
                .and_then(Value::as_mapping)
                .expect("ReconnectConfig must define properties"),
            "initial_delay_ms",
        )
        .is_none(),
        "Rust field names must not leak through a container rename_all"
    );

    let remember_operation = operation(&document, "/api/v1/memory/remember", "post");
    assert_eq!(
        schema_ref(
            request_schema(remember_operation, "remember memory"),
            "remember memory request",
        ),
        "#/components/schemas/RememberRequest"
    );

    let remember = component(&document, "RememberRequest");
    assert!(
        mapping_value(remember, "properties").is_none(),
        "a flattened enum must not appear as a nested `kind` object"
    );
    let composition = mapping_value(remember, "allOf")
        .and_then(Value::as_sequence)
        .expect("RememberRequest must compose its flattened WriteKindWire");
    assert_eq!(composition.len(), 2);
    let base = composition[0]
        .as_mapping()
        .expect("RememberRequest base fields must be an object");
    assert_eq!(required_fields(base), ["agent_id", "content"]);
    assert_eq!(
        schema_ref(&composition[1], "flattened memory write kind"),
        "#/components/schemas/WriteKindWire"
    );

    let write_kind = component(&document, "WriteKindWire");
    assert_eq!(
        mapping_value(write_kind, "discriminator")
            .and_then(Value::as_mapping)
            .and_then(|discriminator| mapping_value(discriminator, "propertyName"))
            .and_then(Value::as_str),
        Some("kind")
    );

    for kind in ["observation", "fact"] {
        let variant = tagged_variant(write_kind, "kind", kind);
        assert_eq!(required_fields(variant), ["kind"]);
    }

    let procedure = tagged_variant(write_kind, "kind", "procedure");
    assert_eq!(required_fields(procedure), ["kind", "skill"]);
    assert_eq!(
        property(procedure, "skill")
            .as_mapping()
            .and_then(|skill| mapping_value(skill, "type"))
            .and_then(Value::as_str),
        Some("string"),
        "the existing procedure payload requires a top-level string `skill`"
    );
}

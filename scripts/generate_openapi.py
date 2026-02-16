#!/usr/bin/env python3
"""
Generate OpenAPI 3.0.3 specification from Streamline Rust source files.

Parses src/server/*_api.rs and src/server/api.rs to extract route definitions,
request/response types, and generates a comprehensive OpenAPI YAML spec.

Usage:
    python3 scripts/generate_openapi.py           # Generate spec
    python3 scripts/generate_openapi.py --check    # Verify spec is up-to-date
"""

import argparse
import os
import re
import sys
from collections import OrderedDict
from pathlib import Path


def find_repo_root():
    """Find the repository root (directory containing Cargo.toml)."""
    script_dir = Path(__file__).resolve().parent
    root = script_dir.parent
    if (root / "Cargo.toml").exists():
        return root
    # Fallback: walk up
    for parent in script_dir.parents:
        if (parent / "Cargo.toml").exists():
            return parent
    print("Error: Could not find repository root (no Cargo.toml found)", file=sys.stderr)
    sys.exit(1)


def get_version(root: Path) -> str:
    """Extract version from Cargo.toml."""
    cargo_toml = root / "Cargo.toml"
    for line in cargo_toml.read_text().splitlines():
        m = re.match(r'^version\s*=\s*"([^"]+)"', line)
        if m:
            return m.group(1)
    return "0.0.0"


# ---------------------------------------------------------------------------
# Route extraction
# ---------------------------------------------------------------------------

class Route:
    """Represents a parsed HTTP route."""
    def __init__(self, path: str, method: str, handler: str, source_file: str, feature: str = None):
        self.path = path
        self.method = method.upper()
        self.handler = handler
        self.source_file = source_file
        self.feature = feature  # Feature gate if any

    def __repr__(self):
        feat = f" [feature={self.feature}]" if self.feature else ""
        return f"Route({self.method} {self.path} -> {self.handler}{feat})"


def normalize_path(path: str) -> str:
    """Convert Axum path params (:param and {param}) to OpenAPI {param} style."""
    # :param -> {param}
    path = re.sub(r':(\w+)', r'{\1}', path)
    # Already {param} is fine
    return path


def _find_balanced_paren(text: str, start: int) -> int:
    """Find the index of the closing paren matching the opening paren at *start*."""
    depth = 0
    i = start
    while i < len(text):
        if text[i] == '(':
            depth += 1
        elif text[i] == ')':
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return len(text)


def _find_balanced_brace(text: str, start: int) -> int:
    """Find the index of the closing brace matching the opening brace at *start*."""
    depth = 1
    i = start + 1
    while i < len(text) and depth > 0:
        if text[i] == '{':
            depth += 1
        elif text[i] == '}':
            depth -= 1
        i += 1
    return i


def extract_routes_from_file(filepath: Path) -> list:
    """Extract routes from a single Rust API file."""
    content = filepath.read_text()
    routes = []
    source = filepath.name

    # Find all create_*_router function bodies
    router_pattern = re.compile(
        r'pub(?:\(crate\))?\s+fn\s+create_\w+_router\s*\([^)]*\)\s*->\s*Router\s*\{',
        re.DOTALL
    )

    for m in router_pattern.finditer(content):
        brace_pos = content.rindex('{', m.start(), m.end())
        body_end = _find_balanced_brace(content, brace_pos)
        body = content[brace_pos + 1:body_end - 1]

        # Find each .route( call using balanced parentheses
        for rm in re.finditer(r'\.route\(', body):
            paren_open = rm.start() + len('.route')
            paren_close = _find_balanced_paren(body, paren_open)
            route_args = body[paren_open + 1:paren_close]

            # Extract path string
            path_match = re.match(r'\s*"([^"]+)"\s*,\s*(.*)', route_args, re.DOTALL)
            if not path_match:
                continue
            path_raw = path_match.group(1)
            methods_str = path_match.group(2).strip()
            path = normalize_path(path_raw)

            # Parse method(handler) chains: get(h1).post(h2).delete(h3)
            method_pattern = re.compile(r'(get|post|put|delete|patch)\((\w+)\)')
            for mm in method_pattern.finditer(methods_str):
                method = mm.group(1)
                handler = mm.group(2)
                routes.append(Route(path, method, handler, source))

    return routes


def determine_feature_gates(root: Path) -> dict:
    """Read http.rs to determine which API modules are behind feature gates.

    Scans the build_http_router function body for ``#[cfg(feature = "X")]``
    blocks that contain ``create_*_router`` calls and maps the corresponding
    source file to its feature gate.
    """
    http_path = root / "src" / "server" / "http.rs"
    if not http_path.exists():
        return {}

    content = http_path.read_text()
    feature_map = {}  # source_file -> feature_name

    # Map from create_*_router name stems to their actual source files.
    # Most follow the pattern create_foo_api_router -> foo_api.rs, but a few don't.
    stem_to_file = {
        'schema_api': 'schema_api.rs',
        'schema_ui': 'schema_ui.rs',
        'analytics_api': 'analytics_api.rs',
        'sqlite_api': 'sqlite_routes.rs',
        'graphql': 'graphql_routes.rs',
        'connect': 'connector_mgmt_api.rs',
        'ai_api': 'ai_api.rs',
        'featurestore_api': 'featurestore_api.rs',
        'failover_api': 'failover_api.rs',
        'raft_cluster_api': 'raft_cluster_api.rs',
        'edge_api': 'edge_api.rs',
        'marketplace': 'wasm_api.rs',
    }

    def _stem_to_source(stem: str) -> str:
        if stem in stem_to_file:
            return stem_to_file[stem]
        # Default: stem.rs  (create_foo_router -> foo.rs) but most are _api
        if stem.endswith('_api'):
            return f"{stem}.rs"
        return f"{stem}_api.rs"

    lines = content.splitlines()
    current_feature = None
    in_cfg_block = False
    brace_depth = 0

    for line in lines:
        stripped = line.strip()

        # Detect #[cfg(feature = "...")]
        cfg_match = re.match(r'#\[cfg\(feature\s*=\s*"([^"]+)"\)\]', stripped)
        if cfg_match:
            current_feature = cfg_match.group(1)
            continue

        if current_feature and not in_cfg_block:
            if stripped == '{':
                in_cfg_block = True
                brace_depth = 1
                continue
            elif stripped == '' or stripped.startswith('//'):
                continue
            elif stripped.startswith('use '):
                # This is an import-level cfg, not a block cfg — skip
                current_feature = None
                continue
            else:
                # Single-line cfg (rare for merge blocks, but handle it)
                router_match = re.search(r'create_(\w+)_router', stripped)
                if router_match:
                    src = _stem_to_source(router_match.group(1))
                    feature_map[src] = current_feature
                current_feature = None
                continue

        if in_cfg_block:
            brace_depth += stripped.count('{') - stripped.count('}')
            router_match = re.search(r'create_(\w+)_router', stripped)
            if router_match:
                src = _stem_to_source(router_match.group(1))
                feature_map[src] = current_feature
            if brace_depth <= 0:
                in_cfg_block = False
                current_feature = None

    return feature_map


def collect_all_routes(root: Path) -> list:
    """Collect routes from all API files."""
    server_dir = root / "src" / "server"
    api_files = sorted(server_dir.glob("*_api.rs"))
    api_files.append(server_dir / "api.rs")

    feature_gates = determine_feature_gates(root)

    all_routes = []
    for filepath in api_files:
        if not filepath.exists():
            continue
        routes = extract_routes_from_file(filepath)
        # Apply feature gates
        feature = feature_gates.get(filepath.name)
        if feature:
            for r in routes:
                r.feature = feature
        all_routes.extend(routes)

    return all_routes


# ---------------------------------------------------------------------------
# Struct extraction
# ---------------------------------------------------------------------------

class StructField:
    """A field in a Rust struct."""
    def __init__(self, name: str, rust_type: str, doc: str = "", optional: bool = False,
                 serde_default: bool = False, serde_skip: bool = False):
        self.name = name
        self.rust_type = rust_type
        self.doc = doc
        self.optional = optional
        self.serde_default = serde_default
        self.serde_skip = serde_skip


class RustStruct:
    """A parsed Rust struct."""
    def __init__(self, name: str, doc: str, fields: list, derives: list):
        self.name = name
        self.doc = doc
        self.fields = fields
        self.derives = derives
        self.is_request = "Deserialize" in derives
        self.is_response = "Serialize" in derives


def extract_structs_from_file(filepath: Path) -> list:
    """Extract pub struct definitions with their fields from a Rust file."""
    content = filepath.read_text()
    structs = []

    # Pattern for struct with derives and doc comments
    struct_pattern = re.compile(
        r'(?:((?:\s*///[^\n]*\n)+))?\s*'
        r'(?:#\[derive\(([^)]+)\)\]\s*)*'
        r'pub\s+struct\s+(\w+)\s*\{',
        re.MULTILINE
    )

    for m in struct_pattern.finditer(content):
        doc_block = m.group(1) or ""
        full_match_start = m.start()

        # Re-extract derives by looking backwards from struct keyword
        derives = []
        before_struct = content[max(0, full_match_start - 200):m.start(3)]
        for dm in re.finditer(r'#\[derive\(([^)]+)\)\]', before_struct):
            derives.extend([d.strip() for d in dm.group(1).split(',')])

        name = m.group(3)

        # Extract doc comment
        doc_lines = []
        for line in doc_block.strip().splitlines():
            doc_lines.append(re.sub(r'^\s*///\s?', '', line))
        doc = ' '.join(doc_lines).strip()

        # Find struct body
        start = m.end()
        depth = 1
        pos = start
        while pos < len(content) and depth > 0:
            if content[pos] == '{':
                depth += 1
            elif content[pos] == '}':
                depth -= 1
            pos += 1
        body = content[start:pos - 1]

        # Parse fields
        fields = []
        field_doc = ""
        serde_default = False
        serde_skip = False
        serde_rename = None

        for line in body.splitlines():
            stripped = line.strip()

            # Doc comment for field
            if stripped.startswith('///'):
                field_doc = re.sub(r'^///\s?', '', stripped)
                continue

            # Serde attributes
            if '#[serde(' in stripped:
                if 'default' in stripped:
                    serde_default = True
                if 'skip_serializing_if' in stripped:
                    serde_skip = True
                if 'rename' in stripped:
                    rm = re.search(r'rename\s*=\s*"([^"]+)"', stripped)
                    if rm:
                        serde_rename = rm.group(1)
                continue

            # Skip non-field lines
            if stripped.startswith('#[') or stripped.startswith('//') or not stripped:
                continue

            # Field: pub name: Type,
            field_match = re.match(r'pub\s+(\w+)\s*:\s*(.+?),?\s*$', stripped)
            if field_match:
                fname = serde_rename or field_match.group(1)
                ftype = field_match.group(2).strip().rstrip(',')
                optional = ftype.startswith('Option<')
                fields.append(StructField(
                    name=fname,
                    rust_type=ftype,
                    doc=field_doc,
                    optional=optional or serde_default,
                    serde_default=serde_default,
                    serde_skip=serde_skip,
                ))
                field_doc = ""
                serde_default = False
                serde_skip = False
                serde_rename = None

        if fields and ("Serialize" in derives or "Deserialize" in derives):
            structs.append(RustStruct(name, doc, fields, derives))

    return structs


def collect_all_structs(root: Path) -> dict:
    """Collect structs from all API files. Returns dict name -> RustStruct."""
    server_dir = root / "src" / "server"
    api_files = sorted(server_dir.glob("*_api.rs"))
    api_files.append(server_dir / "api.rs")

    all_structs = {}
    for filepath in api_files:
        if not filepath.exists():
            continue
        for s in extract_structs_from_file(filepath):
            all_structs[s.name] = s

    return all_structs


# ---------------------------------------------------------------------------
# OpenAPI generation
# ---------------------------------------------------------------------------

def rust_type_to_openapi(rust_type: str) -> dict:
    """Convert a Rust type to an OpenAPI schema fragment."""
    # Strip Option<>
    inner = rust_type
    if inner.startswith('Option<') and inner.endswith('>'):
        inner = inner[7:-1]

    # Primitives
    type_map = {
        'String': {'type': 'string'},
        'str': {'type': 'string'},
        'bool': {'type': 'boolean'},
        'i8': {'type': 'integer', 'format': 'int32'},
        'i16': {'type': 'integer', 'format': 'int32'},
        'i32': {'type': 'integer', 'format': 'int32'},
        'i64': {'type': 'integer', 'format': 'int64'},
        'u8': {'type': 'integer', 'format': 'int32'},
        'u16': {'type': 'integer', 'format': 'int32'},
        'u32': {'type': 'integer', 'format': 'int32'},
        'u64': {'type': 'integer', 'format': 'int64'},
        'usize': {'type': 'integer'},
        'isize': {'type': 'integer'},
        'f32': {'type': 'number', 'format': 'float'},
        'f64': {'type': 'number', 'format': 'double'},
        'serde_json::Value': {},
    }

    if inner in type_map:
        return type_map[inner]

    # Vec<T>
    vec_match = re.match(r'Vec<(.+)>', inner)
    if vec_match:
        item_type = vec_match.group(1).strip()
        items = rust_type_to_openapi(item_type)
        return {'type': 'array', 'items': items}

    # HashMap<String, String>
    hm_match = re.match(r'(?:std::collections::)?HashMap<\s*String\s*,\s*String\s*>', inner)
    if hm_match:
        return {'type': 'object', 'additionalProperties': {'type': 'string'}}

    # HashMap<String, Value>
    hm_match2 = re.match(r'(?:std::collections::)?HashMap<\s*String\s*,\s*(.+)\s*>', inner)
    if hm_match2:
        val_schema = rust_type_to_openapi(hm_match2.group(1))
        return {'type': 'object', 'additionalProperties': val_schema}

    # Known struct reference
    return {'$ref': f'#/components/schemas/{inner}'}


def tag_from_source(source_file: str, path: str) -> str:
    """Derive an API tag from the source file name and route path."""
    tag_map = {
        'api.rs': 'Topics',
        'consumer_api.rs': 'Consumer Groups',
        'schema_api.rs': 'Schema Registry',
        'analytics_api.rs': 'Analytics',
        'dashboard_api.rs': 'Dashboard',
        'cluster_api.rs': 'Cluster',
        'benchmark_api.rs': 'Benchmark',
        'logs_api.rs': 'Logs',
        'connections_api.rs': 'Connections',
        'alerts_api.rs': 'Alerts',
        'cdc_api.rs': 'CDC',
        'cloud_api.rs': 'Cloud',
        'gitops_api.rs': 'GitOps',
        'wasm_api.rs': 'WASM',
        'observability_api.rs': 'Observability',
        'playground_api.rs': 'Playground',
        'ai_api.rs': 'AI Pipelines',
        'featurestore_api.rs': 'Feature Store',
        'plugin_api.rs': 'Plugins',
        'replication_api.rs': 'Replication',
        'edge_api.rs': 'Edge',
        'failover_api.rs': 'Failover',
        'raft_cluster_api.rs': 'Raft Cluster',
        'connector_mgmt_api.rs': 'Connector Management',
        'console_api.rs': 'Console',
        'governor_api.rs': 'Resource Governor',
    }
    return tag_map.get(source_file, 'General')


def handler_to_operation_id(handler: str) -> str:
    """Convert a Rust handler function name to an operationId."""
    # Remove _handler suffix
    name = re.sub(r'_handler$', '', handler)
    # Convert snake_case to camelCase
    parts = name.split('_')
    return parts[0] + ''.join(p.capitalize() for p in parts[1:])


def handler_to_summary(handler: str) -> str:
    """Convert handler function name to a human-readable summary."""
    name = re.sub(r'_handler$', '', handler)
    words = name.split('_')
    return ' '.join(w.capitalize() for w in words)


def infer_request_schema(handler: str, path: str) -> str:
    """Infer the likely request body schema name for a handler."""
    # Common naming conventions
    name_map = {
        'create_topic': 'CreateTopicRequest',
        'produce_messages': 'ProduceRequest',
        'create_alert': 'CreateAlertRequest',
        'execute_query': 'QueryRequest',
        'explain_query': 'QueryRequest',
        'create_view': 'CreateViewRequest',
        'create_source': 'CreateCdcSourceRequest',
        'apply_manifest': 'GitOpsManifest',
        'validate_manifest': 'GitOpsManifest',
        'diff_manifest': 'GitOpsManifest',
        'register_transform': 'RegisterTransformRequest',
        'test_transform': 'TestTransformRequest',
        'deploy_function': 'DeployFunctionRequest',
        'reset_offsets': 'ResetOffsetsRequest',
        'install_plugin': 'InstallPluginRequest',
        'semantic_search': 'SearchRequest',
        'embed_text': 'EmbedRequest',
        'summarize_messages': 'SummarizeRequest',
        'rag_query': 'RagQueryRequest',
        'rag_ingest': 'RagIngestRequest',
        'apply_spec': 'ConnectorSpec',
    }
    clean = re.sub(r'_handler$', '', handler)
    return name_map.get(clean)


def build_path_parameters(path: str) -> list:
    """Extract path parameters from an OpenAPI path."""
    params = []
    for m in re.finditer(r'\{(\w+)\}', path):
        param_name = m.group(1)
        schema = {'type': 'string'}
        if param_name in ('partition', 'id'):
            schema = {'type': 'integer'}
        params.append({
            'name': param_name,
            'in': 'path',
            'required': True,
            'schema': schema,
        })
    return params


def generate_openapi(root: Path) -> str:
    """Generate the complete OpenAPI YAML specification."""
    version = get_version(root)
    routes = collect_all_routes(root)
    structs = collect_all_structs(root)

    # Group routes by path
    path_groups = OrderedDict()
    for route in routes:
        key = route.path
        if key not in path_groups:
            path_groups[key] = []
        path_groups[key].append(route)

    # Sort paths for deterministic output
    sorted_paths = sorted(path_groups.keys())

    # Collect all tags
    tags_set = OrderedDict()
    for route in routes:
        tag = tag_from_source(route.source_file, route.path)
        if tag not in tags_set:
            tags_set[tag] = route.feature

    # Build YAML manually for control over formatting
    lines = []

    def w(line="", indent=0):
        lines.append("  " * indent + line)

    # Header
    w("openapi: 3.0.3")
    w("info:")
    w("title: Streamline REST API", 1)
    w("description: |", 1)
    w("HTTP REST API for Streamline - The Redis of Streaming.", 2)
    w("", 2)
    w("This API provides a simple alternative to the Kafka protocol for producing and", 2)
    w("consuming messages, managing topics, and monitoring consumer groups.", 2)
    w("", 2)
    w("## Authentication", 2)
    w("", 2)
    w("When authentication is enabled, include an `Authorization` header with a Bearer token", 2)
    w("or use SASL credentials via the `X-Streamline-Auth` header.", 2)
    w("", 2)
    w("## Rate Limiting", 2)
    w("", 2)
    w("Clients may be subject to rate limiting based on server configuration. When rate", 2)
    w("limited, you'll receive a 429 status code with a `Retry-After` header.", 2)
    w("", 2)
    w("## WebSocket Streaming", 2)
    w("", 2)
    w("For real-time message streaming, use the WebSocket API at `/ws/v2/stream`.", 2)
    w("See the WebSocket section below for protocol details.", 2)
    w(f"version: \"{version}\"", 1)
    w("contact:", 1)
    w("name: Streamline Support", 2)
    w("url: https://github.com/streamline/streamline", 2)
    w("license:", 1)
    w("name: Apache 2.0", 2)
    w("url: https://www.apache.org/licenses/LICENSE-2.0", 2)
    w()

    # Servers
    w("servers:")
    w("- url: http://localhost:9094", 1)
    w("description: Local development server (HTTP API port)", 2)
    w("- url: https://api.streamline.example.com", 1)
    w("description: Production server", 2)
    w()

    # Tags
    w("tags:")
    for tag_name, feature in tags_set.items():
        w(f"- name: {tag_name}", 1)
        desc = f"{tag_name} operations"
        if feature:
            desc += f" (requires '{feature}' feature)"
        w(f"  description: {desc}", 1)
    w()

    # Paths
    w("paths:")
    for path in sorted_paths:
        route_list = path_groups[path]
        w(f"{path}:", 1)
        for route in route_list:
            method_lower = route.method.lower()
            tag = tag_from_source(route.source_file, route.path)
            op_id = handler_to_operation_id(route.handler)
            summary = handler_to_summary(route.handler)

            w(f"{method_lower}:", 2)
            w("tags:", 3)
            w(f"- {tag}", 4)
            w(f"summary: {summary}", 3)
            w(f"operationId: {op_id}", 3)

            if route.feature:
                w(f"description: \"Requires feature: {route.feature}\"", 3)

            # Path parameters
            path_params = build_path_parameters(route.path)
            if path_params:
                w("parameters:", 3)
                for p in path_params:
                    w(f"- name: {p['name']}", 4)
                    w(f"in: {p['in']}", 5)
                    w(f"required: {str(p['required']).lower()}", 5)
                    w("schema:", 5)
                    for k, v in p['schema'].items():
                        w(f"{k}: {v}", 6)

            # Request body for POST/PUT/PATCH
            if method_lower in ('post', 'put', 'patch'):
                req_schema = infer_request_schema(route.handler, route.path)
                if req_schema and req_schema in structs:
                    w("requestBody:", 3)
                    w("required: true", 4)
                    w("content:", 4)
                    w("application/json:", 5)
                    w("schema:", 6)
                    w(f"$ref: '#/components/schemas/{req_schema}'", 7)

            # Responses
            w("responses:", 3)
            w("'200':", 4)
            w("description: Successful operation", 5)
            w("content:", 5)
            w("application/json:", 6)
            w("schema:", 7)
            w("type: object", 8)
            if method_lower in ('post', 'put'):
                w("'400':", 4)
                w("description: Bad request", 5)
                w("content:", 5)
                w("application/json:", 6)
                w("schema:", 7)
                w("$ref: '#/components/schemas/ErrorResponse'", 8)
            if path_params:
                w("'404':", 4)
                w("description: Resource not found", 5)
                w("content:", 5)
                w("application/json:", 6)
                w("schema:", 7)
                w("$ref: '#/components/schemas/ErrorResponse'", 8)
            w("'500':", 4)
            w("$ref: '#/components/responses/InternalError'", 5)
    w()

    # Components
    w("components:")

    # Parameters
    w("parameters:", 1)
    w("TopicName:", 2)
    w("name: topic", 3)
    w("in: path", 3)
    w("required: true", 3)
    w("description: Topic name", 3)
    w("schema:", 3)
    w("type: string", 4)
    w("pattern: '^[a-zA-Z0-9._-]+$'", 4)
    w("minLength: 1", 4)
    w("maxLength: 255", 4)
    w("example: events", 3)
    w()

    # Schemas
    w("schemas:", 1)

    def write_schema_value(schema: dict, indent: int):
        """Write an OpenAPI schema dict as YAML at the given indent level."""
        for k, v in schema.items():
            if isinstance(v, dict):
                w(f"{k}:", indent)
                write_schema_value(v, indent + 1)
            elif isinstance(v, list):
                w(f"{k}:", indent)
                for item in v:
                    if isinstance(item, dict):
                        w(f"- ", indent + 1)
                        for ik, iv in item.items():
                            w(f"  {ik}: {iv}", indent + 1)
                    else:
                        w(f"- {item}", indent + 1)
            elif isinstance(v, str) and ('#' in v or ':' in v or '{' in v):
                w(f"{k}: '{v}'", indent)
            else:
                w(f"{k}: {v}", indent)

    # Generate schemas for all extracted structs
    for name in sorted(structs.keys()):
        s = structs[name]
        w(f"{name}:", 2)
        w("type: object", 3)

        # Required fields
        required = [f.name for f in s.fields if not f.optional and not f.serde_skip]
        if required:
            w("required:", 3)
            for r in required:
                w(f"- {r}", 4)

        # Properties
        if s.fields:
            w("properties:", 3)
            for f in s.fields:
                if f.serde_skip:
                    continue
                w(f"{f.name}:", 4)
                schema = rust_type_to_openapi(f.rust_type)
                write_schema_value(schema, 5)
                if f.doc:
                    # Escape special YAML chars in description
                    desc = f.doc.replace('"', '\\"')
                    w(f"description: \"{desc}\"", 5)
        w()

    # Ensure ErrorResponse is always present
    if 'ErrorResponse' not in structs:
        w("ErrorResponse:", 2)
        w("type: object", 3)
        w("required:", 3)
        w("- error", 4)
        w("- message", 4)
        w("properties:", 3)
        w("error:", 4)
        w("type: string", 5)
        w("description: \"Error code (e.g., TOPIC_NOT_FOUND, INVALID_REQUEST)\"", 5)
        w("message:", 4)
        w("type: string", 5)
        w("description: \"Human-readable error message\"", 5)
        w("hint:", 4)
        w("type: string", 5)
        w("description: \"Actionable hint for resolving the error\"", 5)
        w("docs_url:", 4)
        w("type: string", 5)
        w("format: uri", 5)
        w("description: \"Documentation URL for more information\"", 5)
        w()

    # Responses
    w("responses:", 1)
    w("InternalError:", 2)
    w("description: Internal server error", 3)
    w("content:", 3)
    w("application/json:", 4)
    w("schema:", 5)
    w("$ref: '#/components/schemas/ErrorResponse'", 6)
    w("example:", 5)
    w("error: INTERNAL_ERROR", 6)
    w("message: \"An unexpected error occurred\"", 6)
    w()

    # Security schemes
    w("securitySchemes:", 1)
    w("bearerAuth:", 2)
    w("type: http", 3)
    w("scheme: bearer", 3)
    w("bearerFormat: JWT", 3)
    w("description: JWT Bearer token authentication", 3)
    w()
    w("basicAuth:", 2)
    w("type: http", 3)
    w("scheme: basic", 3)
    w("description: Basic authentication (username:password)", 3)

    return '\n'.join(lines) + '\n'


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate OpenAPI spec from Streamline sources")
    parser.add_argument("--check", action="store_true",
                        help="Verify the spec is up-to-date (exit 1 if stale)")
    parser.add_argument("--output", type=str, default=None,
                        help="Output file path (default: openapi/streamline-api-v1.yaml)")
    args = parser.parse_args()

    root = find_repo_root()
    output_path = Path(args.output) if args.output else root / "openapi" / "streamline-api-v1.yaml"

    spec = generate_openapi(root)

    if args.check:
        if not output_path.exists():
            print(f"ERROR: OpenAPI spec not found at {output_path}", file=sys.stderr)
            print("Run: python3 scripts/generate_openapi.py", file=sys.stderr)
            sys.exit(1)
        existing = output_path.read_text()
        if existing != spec:
            print(f"ERROR: OpenAPI spec at {output_path} is out of date.", file=sys.stderr)
            print("Run: python3 scripts/generate_openapi.py", file=sys.stderr)
            sys.exit(1)
        print(f"OK: OpenAPI spec at {output_path} is up to date.")
        sys.exit(0)

    # Generate
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(spec)
    print(f"Generated OpenAPI spec: {output_path}")

    # Count routes for summary
    routes = collect_all_routes(root)
    unique_paths = len(set(r.path for r in routes))
    print(f"  Routes: {len(routes)} operations across {unique_paths} paths")

    # Copy to docs repo if it exists
    docs_dest = root.parent / "streamline-docs" / "static" / "openapi" / "streamline-api-v1.yaml"
    if docs_dest.parent.exists():
        docs_dest.write_text(spec)
        print(f"  Copied to docs: {docs_dest}")


if __name__ == "__main__":
    main()

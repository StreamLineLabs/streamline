#!/usr/bin/env python3
"""
Generate OpenAPI 3.0.3 specification from Streamline Rust source files.

Starts at `src/server/http.rs`, follows every mounted router builder (including
nested modules and non-`create_*` builders), resolves handler DTOs by module,
and generates a comprehensive OpenAPI YAML spec.

Usage:
    python3 scripts/generate_openapi.py           # Generate spec
    python3 scripts/generate_openapi.py --check    # Verify spec is up-to-date
"""

import argparse
import json
import re
import sys
from collections import Counter, OrderedDict, defaultdict
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
    def __init__(
        self,
        path: str,
        method: str,
        handler: str,
        source_file: str,
        router: str,
        feature: str = None,
    ):
        self.path = path
        self.method = method.upper()
        self.handler = handler
        self.source_file = source_file
        self.router = router
        self.feature = feature  # Feature gate if any
        self.alternates: list[Route] = []

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


def rust_raw_string_prefix(source: str, index: int) -> tuple[int, int] | None:
    cursor = index
    if source.startswith(("br", "cr"), cursor):
        cursor += 2
    elif source.startswith("r", cursor):
        cursor += 1
    else:
        return None
    hashes = 0
    while cursor < len(source) and source[cursor] == "#":
        hashes += 1
        cursor += 1
    if cursor >= len(source) or source[cursor] != '"':
        return None
    return cursor + 1, hashes


def rust_char_literal_end(source: str, index: int) -> int | None:
    cursor = index + 1
    if cursor >= len(source) or source[cursor] == "\n":
        return None
    if source[cursor].isalpha() or source[cursor] == "_":
        next_cursor = cursor + 1
        return (
            next_cursor
            if next_cursor < len(source) and source[next_cursor] == "'"
            else None
        )
    escaped = False
    while cursor < len(source) and source[cursor] != "\n":
        char = source[cursor]
        if escaped:
            escaped = False
        elif char == "\\":
            escaped = True
        elif char == "'":
            return cursor
        cursor += 1
    return None


def masked_rust_code(source: str) -> str:
    """Blank comments/literals while preserving byte offsets and newlines."""
    output = list(source)
    index = 0
    state = "code"
    block_depth = 0
    raw_hashes = 0
    escaped = False

    def blank(position: int) -> None:
        if output[position] != "\n":
            output[position] = " "

    while index < len(source):
        char = source[index]
        if state == "line_comment":
            blank(index)
            if char == "\n":
                state = "code"
            index += 1
            continue
        if state == "block_comment":
            blank(index)
            if source.startswith("/*", index):
                block_depth += 1
                if index + 1 < len(source):
                    blank(index + 1)
                index += 2
            elif source.startswith("*/", index):
                block_depth -= 1
                if index + 1 < len(source):
                    blank(index + 1)
                index += 2
                if block_depth == 0:
                    state = "code"
            else:
                index += 1
            continue
        if state in {"string", "char"}:
            blank(index)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif (state == "string" and char == '"') or (
                state == "char" and char == "'"
            ):
                state = "code"
            index += 1
            continue
        if state == "raw_string":
            blank(index)
            if char == '"' and source.startswith("#" * raw_hashes, index + 1):
                for suffix in range(1, raw_hashes + 1):
                    blank(index + suffix)
                index += raw_hashes + 1
                state = "code"
            else:
                index += 1
            continue

        if source.startswith("//", index):
            blank(index)
            blank(index + 1)
            state = "line_comment"
            index += 2
            continue
        if source.startswith("/*", index):
            blank(index)
            blank(index + 1)
            state = "block_comment"
            block_depth = 1
            index += 2
            continue

        raw_prefix = rust_raw_string_prefix(source, index)
        if raw_prefix is not None:
            content_start, raw_hashes = raw_prefix
            while index < content_start:
                blank(index)
                index += 1
            state = "raw_string"
            continue
        if source.startswith(("b\"", "c\""), index):
            blank(index)
            blank(index + 1)
            state = "string"
            escaped = False
            index += 2
            continue
        if char == '"':
            blank(index)
            state = "string"
            escaped = False
            index += 1
            continue
        if source.startswith("b'", index) and rust_char_literal_end(source, index + 1):
            blank(index)
            blank(index + 1)
            state = "char"
            escaped = False
            index += 2
            continue
        if char == "'" and rust_char_literal_end(source, index) is not None:
            blank(index)
            state = "char"
            escaped = False
            index += 1
            continue
        index += 1
    return "".join(output)


class ModuleFunction:
    def __init__(
        self,
        name: str,
        parameters: str,
        return_type: str,
        body: str,
        start: int,
        required_feature: str | None,
        excluded_feature: str | None,
    ):
        self.name = name
        self.parameters = parameters
        self.return_type = return_type
        self.body = body
        self.start = start
        self.required_feature = required_feature
        self.excluded_feature = excluded_feature


def function_feature_gate(content: str, start: int) -> tuple[str | None, str | None]:
    required = None
    excluded = None
    for line in reversed(content[:start].splitlines()):
        stripped = line.strip()
        if not stripped or stripped.startswith("///"):
            continue
        if stripped.startswith("#["):
            required_match = re.fullmatch(
                r'#\[cfg\(feature\s*=\s*"([^"]+)"\)\]',
                stripped,
            )
            excluded_match = re.fullmatch(
                r'#\[cfg\(not\(feature\s*=\s*"([^"]+)"\)\)\]',
                stripped,
            )
            if required_match:
                required = required_match.group(1)
            elif excluded_match:
                excluded = excluded_match.group(1)
            continue
        break
    return required, excluded


def module_functions(content: str) -> list[ModuleFunction]:
    """Return real module-level functions, excluding impl/test/nested methods."""
    masked = masked_rust_code(content)
    pattern = re.compile(
        r'\b(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+'
        r'([A-Za-z_][A-Za-z0-9_]*)\s*\('
    )
    functions = []
    depth = 0
    cursor = 0
    for match in pattern.finditer(masked):
        for char in masked[cursor:match.start()]:
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
        cursor = match.start()
        if depth != 0:
            continue

        paren_open = match.end() - 1
        paren_close = _find_balanced_paren(masked, paren_open)
        brace_open = masked.find("{", paren_close)
        if brace_open < 0:
            continue
        brace_close = _find_balanced_brace(masked, brace_open)
        required_feature, excluded_feature = function_feature_gate(
            content,
            match.start(),
        )
        functions.append(
            ModuleFunction(
                match.group(1),
                content[paren_open + 1:paren_close],
                content[paren_close + 1:brace_open],
                content[brace_open + 1:brace_close - 1],
                match.start(),
                required_feature,
                excluded_feature,
            )
        )
        cursor = match.start()
    return functions


def source_name(root: Path, filepath: Path) -> str:
    """Return a stable Rust module path for a source file."""
    relative = filepath.relative_to(root / "src").with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "mod":
        parts.pop()
    return "::".join(parts)


def router_calls(text: str) -> set[str]:
    """Return router-builder function names called in *text*."""
    pattern = re.compile(
        r'\b((?:create_[A-Za-z0-9_]*router[A-Za-z0-9_]*|'
        r'[A-Za-z_][A-Za-z0-9_]*_router))\s*\('
    )
    return {match.group(1) for match in pattern.finditer(text)}


class RouterDefinition:
    """A router builder and its source body."""

    def __init__(self, name: str, filepath: Path, source_file: str, body: str):
        self.name = name
        self.filepath = filepath
        self.source_file = source_file
        self.body = body


def function_body(content: str, function_name: str) -> str | None:
    """Return a Rust function body using balanced delimiters."""
    candidates = [
        function for function in module_functions(content)
        if function.name == function_name
    ]
    if not candidates:
        return None
    if len(candidates) != 1:
        raise RuntimeError(f"module-level function `{function_name}` is ambiguous")
    return candidates[0].body


def router_definitions(root: Path) -> dict[str, list[RouterDefinition]]:
    """Index every Rust function that returns an Axum Router."""
    definitions: dict[str, list[RouterDefinition]] = defaultdict(list)
    for filepath in sorted((root / "src").rglob("*.rs")):
        content = filepath.read_text()
        for function in module_functions(content):
            if not re.fullmatch(
                r'(?:create_[A-Za-z0-9_]*router[A-Za-z0-9_]*|'
                r'[A-Za-z_][A-Za-z0-9_]*_router)',
                function.name,
            ):
                continue
            if "Router" not in function.return_type:
                continue
            definitions[function.name].append(
                RouterDefinition(
                    function.name,
                    filepath,
                    source_name(root, filepath),
                    function.body,
                )
            )
    return definitions


def feature_by_line(body: str) -> dict[int, str | None]:
    """Map each line in a router composition body to its active feature gate."""
    features: dict[int, str | None] = {}
    pending_feature: str | None = None
    active_feature: str | None = None
    feature_depth = 0

    masked_lines = masked_rust_code(body).splitlines()
    for line_number, (line, masked_line) in enumerate(
        zip(body.splitlines(), masked_lines),
        start=1,
    ):
        stripped = line.strip()
        features[line_number] = active_feature
        cfg_match = re.match(r'#\[cfg\(feature\s*=\s*"([^"]+)"\)\]', stripped)
        if cfg_match:
            pending_feature = cfg_match.group(1)
            continue

        if pending_feature is not None and active_feature is None:
            if not stripped or stripped.startswith("//"):
                continue
            if stripped == "{":
                active_feature = pending_feature
                feature_depth = 1
                features[line_number] = active_feature
                pending_feature = None
                continue
            features[line_number] = pending_feature
            pending_feature = None

        if active_feature is not None:
            features[line_number] = active_feature
            feature_depth += masked_line.count('{') - masked_line.count('}')
            if feature_depth <= 0:
                active_feature = None
    return features


def mounted_router_features(root: Path) -> dict[str, str | None]:
    """Map routers mounted by `build_http_router` to their feature gates."""
    http_path = root / "src" / "server" / "http.rs"
    content = http_path.read_text()
    body = function_body(content, "build_http_router")
    if body is None:
        raise RuntimeError("src/server/http.rs must define build_http_router")

    features: dict[str, str | None] = {
        name: None for name in router_calls(body)
    }
    line_features = feature_by_line(body)
    for line_number, line in enumerate(body.splitlines(), start=1):
        for name in router_calls(line):
            features[name] = line_features.get(line_number)
    return features


def mounted_router_definitions(root: Path) -> list[tuple[RouterDefinition, str | None]]:
    """Resolve the complete router-builder call graph mounted by the HTTP server."""
    definitions = router_definitions(root)
    pending = list(mounted_router_features(root).items())
    resolved: dict[tuple[str, str], tuple[RouterDefinition, str | None]] = {}

    while pending:
        name, feature = pending.pop()
        candidates = definitions.get(name, [])
        if not candidates:
            raise RuntimeError(
                f"mounted router builder `{name}` has no discoverable Rust definition"
            )
        if len(candidates) != 1:
            locations = ", ".join(str(candidate.filepath) for candidate in candidates)
            raise RuntimeError(
                f"mounted router builder `{name}` is ambiguous across: {locations}"
            )

        definition = candidates[0]
        key = (definition.source_file, definition.name)
        existing = resolved.get(key)
        if existing is not None:
            if existing[1] is not None and feature is None:
                resolved[key] = (definition, None)
            continue
        resolved[key] = (definition, feature)

        for child in router_calls(definition.body):
            if child in definitions:
                pending.append((child, feature))

    return sorted(resolved.values(), key=lambda item: (item[0].source_file, item[0].name))


def extract_routes_from_definition(
    definition: RouterDefinition,
    feature: str | None,
    line_features: dict[int, str | None] | None = None,
) -> list[Route]:
    """Extract routes from one mounted router builder."""
    routes = []
    body = definition.body
    for route_match in re.finditer(r'\.route\(', body):
        route_feature = feature
        if line_features is not None:
            line_number = body.count("\n", 0, route_match.start()) + 1
            route_feature = line_features.get(line_number, feature)
        paren_open = route_match.start() + len('.route')
        paren_close = _find_balanced_paren(body, paren_open)
        route_args = body[paren_open + 1:paren_close]
        path_match = re.match(r'\s*"([^"]+)"\s*,\s*(.*)', route_args, re.DOTALL)
        if not path_match:
            continue
        path = normalize_path(path_match.group(1))
        methods = path_match.group(2).strip()
        for method_match in re.finditer(
            r'(get|post|put|delete|patch|options|head)\((\w+)\)',
            methods,
        ):
            routes.append(
                Route(
                    path,
                    method_match.group(1),
                    method_match.group(2),
                    definition.source_file,
                    definition.name,
                    route_feature,
                )
            )
    return routes


def collect_all_routes(root: Path) -> list:
    """Collect routes from all API files."""
    exact: OrderedDict[tuple[str, str, str, str], Route] = OrderedDict()
    http_path = root / "src" / "server" / "http.rs"
    http_body = function_body(http_path.read_text(), "build_http_router")
    if http_body is None:
        raise RuntimeError("src/server/http.rs must define build_http_router")
    direct_definition = RouterDefinition(
        "build_http_router",
        http_path,
        "server::http",
        http_body,
    )
    for route in extract_routes_from_definition(
        direct_definition,
        None,
        feature_by_line(http_body),
    ):
        key = (route.path, route.method, route.handler, route.source_file)
        exact[key] = route

    for definition, feature in mounted_router_definitions(root):
        for route in extract_routes_from_definition(definition, feature):
            key = (route.path, route.method, route.handler, route.source_file)
            exact[key] = route

    operations: OrderedDict[tuple[str, str], Route] = OrderedDict()
    for route in exact.values():
        key = (route.path, route.method)
        existing = operations.get(key)
        if existing is None:
            operations[key] = route
            continue
        if existing.feature is not None and route.feature is None:
            route.alternates = [existing, *existing.alternates]
            operations[key] = route
        else:
            existing.alternates.append(route)
    return list(operations.values())


# ---------------------------------------------------------------------------
# Struct extraction
# ---------------------------------------------------------------------------

class StructField:
    """A field in a Rust struct."""
    def __init__(self, name: str, rust_type: str, doc: str = "", optional: bool = False,
                 serde_default: bool = False, serde_skip: bool = False,
                 serde_flatten: bool = False):
        self.name = name
        self.rust_type = rust_type
        self.doc = doc
        self.optional = optional
        self.serde_default = serde_default
        self.serde_skip = serde_skip
        self.serde_flatten = serde_flatten


class RustStruct:
    """A parsed Rust struct."""
    def __init__(
        self,
        name: str,
        doc: str,
        fields: list,
        derives: list,
        source_file: str,
        rename_all: str | None,
    ):
        self.name = name
        self.doc = doc
        self.fields = fields
        self.derives = derives
        self.source_file = source_file
        self.rename_all = rename_all
        self.schema_name = name
        self.is_request = "Deserialize" in derives
        self.is_response = "Serialize" in derives


class EnumVariant:
    """A serializable Rust enum variant."""

    def __init__(
        self,
        name: str,
        explicit_name: str | None,
        fields: list[StructField],
        tuple_types: list[str],
    ):
        self.name = name
        self.explicit_name = explicit_name
        self.fields = fields
        self.tuple_types = tuple_types

    @property
    def has_payload(self) -> bool:
        return bool(self.fields or self.tuple_types)


class RustEnum:
    """A serializable Rust enum."""

    def __init__(
        self,
        name: str,
        variants: list[EnumVariant],
        source_file: str,
        rename_all: str | None,
        tag: str | None,
        content: str | None,
        untagged: bool,
    ):
        self.name = name
        self.variants = variants
        self.source_file = source_file
        self.rename_all = rename_all
        self.tag = tag
        self.content = content
        self.untagged = untagged
        self.schema_name = name


def split_top_level(text: str) -> list[str]:
    """Split comma-separated Rust items while respecting nested delimiters."""
    items = []
    start = 0
    depths = {'(': 0, '{': 0, '[': 0, '<': 0}
    pairs = {')': '(', '}': '{', ']': '[', '>': '<'}
    for index, char in enumerate(masked_rust_code(text)):
        if char in depths:
            depths[char] += 1
        elif char in pairs and depths[pairs[char]] > 0:
            depths[pairs[char]] -= 1
        elif char == ',' and all(depth == 0 for depth in depths.values()):
            items.append(text[start:index])
            start = index + 1
    items.append(text[start:])
    return items


def parse_named_fields(body: str, rename_all: str | None = None) -> list[StructField]:
    """Parse named Rust fields and their Serde wire attributes."""
    fields = []
    for item in split_top_level(body):
        item = item.strip()
        if not item:
            continue

        field_docs = [
            re.sub(r'^\s*///\s?', '', line)
            for line in item.splitlines()
            if line.strip().startswith('///')
        ]
        serde_attrs = re.findall(r'#\[serde\(([^]]+)\)\]', item)
        serde = ','.join(serde_attrs)
        serde_default = re.search(r'\bdefault\b', serde) is not None
        serde_optional = 'skip_serializing_if' in serde
        serde_skip = (
            'skip_serializing_if' not in serde
            and re.search(
                r'\bskip(?:_serializing|_deserializing)?\b',
                serde,
            )
            is not None
        )
        serde_flatten = re.search(r'\bflatten\b', serde) is not None
        rename = re.search(r'rename\s*=\s*"([^"]+)"', serde)
        serde_rename = rename.group(1) if rename else None

        declaration = re.sub(
            r'(?m)^\s*(?:#\[[^\n]+\]|///[^\n]*)\s*$',
            '',
            item,
        ).strip()
        field_match = re.fullmatch(
            r'(?:pub(?:\([^)]*\))?\s+)?(\w+)\s*:\s*(.+)',
            declaration,
            re.DOTALL,
        )
        if field_match:
            raw_name = field_match.group(1)
            field_type = field_match.group(2).strip().rstrip(',')
            fields.append(StructField(
                name=serde_rename or renamed_variant(raw_name, rename_all),
                rust_type=field_type,
                doc=' '.join(field_docs),
                optional=(
                    field_type.startswith('Option<')
                    or serde_default
                    or serde_optional
                ),
                serde_default=serde_default,
                serde_skip=serde_skip,
                serde_flatten=serde_flatten,
            ))

    return fields


def extract_enums_from_file(
    filepath: Path,
    root: Path | None = None,
    source_file: str | None = None,
) -> list[RustEnum]:
    """Extract serializable named enums from a Rust source file."""
    content = filepath.read_text()
    content = re.split(
        r'(?m)^#\[cfg\(test\)\]\s*\n(?:#\[[^\n]+\]\s*\n)*mod\s+tests\s*\{',
        content,
        maxsplit=1,
    )[0]
    source_file = (
        source_file
        or (source_name(root, filepath) if root is not None else filepath.as_posix())
    )
    enums = []
    pattern = re.compile(
        r'((?:#\[[^\n]+\]\s*)*)'
        r'(?:pub(?:\([^)]*\))?\s+)?enum\s+(\w+)\s*\{',
        re.MULTILINE,
    )
    for match in pattern.finditer(content):
        attributes = match.group(1)
        derives = []
        for derive in re.finditer(r'#\[derive\(([^)]+)\)\]', attributes):
            derives.extend(
                item.strip().split("::")[-1]
                for item in derive.group(1).split(',')
            )
        if "Serialize" not in derives and "Deserialize" not in derives:
            continue

        serde_attrs = list(re.finditer(r'#\[serde\(([^]]+)\)\]', attributes))
        serde = serde_attrs[-1].group(1) if serde_attrs else ""
        container_rename_match = re.search(
            r'rename_all\s*=\s*"([^"]+)"',
            serde,
        )
        tag_match = re.search(r'tag\s*=\s*"([^"]+)"', serde)
        content_match = re.search(r'content\s*=\s*"([^"]+)"', serde)
        untagged = re.search(r'\buntagged\b', serde) is not None

        brace_open = content.find('{', match.start())
        brace_close = _find_balanced_brace(content, brace_open)
        body = content[brace_open + 1:brace_close - 1]
        variants = []
        for item in split_top_level(body):
            variant = item.strip()
            variant_serde = re.findall(r'#\[serde\(([^]]+)\)\]', variant)
            rename = None
            if variant_serde:
                variant_rename_match = re.search(
                    r'rename\s*=\s*"([^"]+)"',
                    variant_serde[-1],
                )
                if variant_rename_match:
                    rename = variant_rename_match.group(1)
            variant = re.sub(
                r'(?m)^\s*(?:#\[[^\n]+\]|///[^\n]*)\s*$',
                '',
                variant,
            ).strip()
            variant_match = re.match(
                r'([A-Za-z_][A-Za-z0-9_]*)(.*)',
                variant,
                re.DOTALL,
            )
            if variant_match:
                payload = variant_match.group(2).strip()
                fields = []
                tuple_types = []
                if payload.startswith('{') and payload.endswith('}'):
                    fields = parse_named_fields(payload[1:-1])
                elif payload.startswith('(') and payload.endswith(')'):
                    tuple_types = [
                        item.strip()
                        for item in split_top_level(payload[1:-1])
                        if item.strip()
                    ]
                variants.append(
                    EnumVariant(
                        variant_match.group(1),
                        rename,
                        fields,
                        tuple_types,
                    )
                )
        if variants:
            enums.append(
                RustEnum(
                    match.group(2),
                    variants,
                    source_file,
                    (
                        container_rename_match.group(1)
                        if container_rename_match
                        else None
                    ),
                    tag_match.group(1) if tag_match else None,
                    content_match.group(1) if content_match else None,
                    untagged,
                )
            )
    return enums


def extract_structs_from_file(
    filepath: Path,
    root: Path | None = None,
    source_file: str | None = None,
) -> list:
    """Extract pub struct definitions with their fields from a Rust file."""
    content = filepath.read_text()
    content = re.split(
        r'(?m)^#\[cfg\(test\)\]\s*\n(?:#\[[^\n]+\]\s*\n)*mod\s+tests\s*\{',
        content,
        maxsplit=1,
    )[0]
    structs = []
    source_file = (
        source_file
        or (source_name(root, filepath) if root is not None else filepath.as_posix())
    )

    # Pattern for a struct and its contiguous doc/attribute prefix.
    struct_pattern = re.compile(
        r'((?:(?:[ \t]*///[^\n]*\n)|'
        r'(?:[ \t]*#\[[^\n]+\][ \t]*\n))*)'
        r'[ \t]*(?:pub(?:\([^)]*\))?\s+)?struct\s+(\w+)\s*\{',
        re.MULTILINE
    )

    for m in struct_pattern.finditer(content):
        attributes = m.group(1) or ""
        derives = []
        for dm in re.finditer(r'#\[derive\(([^)]+)\)\]', attributes):
            derives.extend(
                d.strip().split("::")[-1]
                for d in dm.group(1).split(',')
            )

        serde_attrs = re.findall(r'#\[serde\(([^]]+)\)\]', attributes)
        serde = ','.join(serde_attrs)
        rename_match = re.search(r'rename_all\s*=\s*"([^"]+)"', serde)
        rename_all = rename_match.group(1) if rename_match else None
        name = m.group(2)

        # Extract doc comment
        doc_lines = []
        for line in attributes.strip().splitlines():
            if line.strip().startswith('///'):
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

        fields = parse_named_fields(body, rename_all)

        if fields and ("Serialize" in derives or "Deserialize" in derives):
            structs.append(
                RustStruct(
                    name,
                    doc,
                    fields,
                    derives,
                    source_file,
                    rename_all,
                )
            )

    return structs


def module_prefix(source_file: str) -> str:
    """Convert a Rust module path to a stable PascalCase schema prefix."""
    parts = source_file.split("::")
    if parts and parts[0] == "server":
        parts = parts[1:]
    return "".join(
        "".join(word.capitalize() for word in part.split("_"))
        for part in parts
    )


FORCE_QUALIFIED_SCHEMA_NAMES = {
    "AlertCondition",
    "FunctionResponse",
    "SearchRequest",
}


def extract_imports(filepath: Path) -> dict[str, str]:
    """Map imported type aliases to their crate module paths."""
    content = filepath.read_text()
    imports: dict[str, str] = {}

    for match in re.finditer(
        r'use\s+crate::([A-Za-z0-9_:]+)::\{([^{}]+)\}\s*;',
        content,
        re.DOTALL,
    ):
        module = match.group(1)
        for item in match.group(2).split(','):
            item = item.strip()
            if not item or item == "self" or "::" in item:
                continue
            alias_match = re.fullmatch(
                r'([A-Za-z_][A-Za-z0-9_]*)(?:\s+as\s+([A-Za-z_][A-Za-z0-9_]*))?',
                item,
            )
            if alias_match:
                imports[alias_match.group(2) or alias_match.group(1)] = (
                    f"{module}::{alias_match.group(1)}"
                )

    for match in re.finditer(
        r'use\s+crate::([A-Za-z0-9_:]+)::'
        r'([A-Za-z_][A-Za-z0-9_]*)'
        r'(?:\s+as\s+([A-Za-z_][A-Za-z0-9_]*))?\s*;',
        content,
    ):
        imports[match.group(3) or match.group(2)] = (
            f"{match.group(1)}::{match.group(2)}"
        )
    return imports


class SchemaCatalog:
    """Resolve local, imported, qualified, and colliding Rust DTO names."""

    def __init__(self, root: Path, active_sources: set[str]):
        self.root = root
        self.structs: list[RustStruct] = []
        self.by_source_name: dict[tuple[str, str], RustStruct] = {}
        self.by_name: dict[str, list[RustStruct]] = defaultdict(list)
        self.enums: list[RustEnum] = []
        self.enums_by_source_name: dict[tuple[str, str], RustEnum] = {}
        self.enums_by_name: dict[str, list[RustEnum]] = defaultdict(list)
        self.imports: dict[str, dict[str, str]] = {}
        self.source_paths: dict[str, Path] = {}

        source_files = [
            (filepath, source_name(root, filepath))
            for filepath in sorted((root / "src").rglob("*.rs"))
        ]
        workspace_type_roots = [
            (root / "crates" / "streamline-analytics" / "src", "analytics"),
            (root / "crates" / "streamline-wasm" / "src", "wasm"),
        ]
        for crate_root, prefix in workspace_type_roots:
            if not crate_root.exists():
                continue
            for filepath in sorted(crate_root.rglob("*.rs")):
                relative = filepath.relative_to(crate_root).with_suffix("")
                parts = list(relative.parts)
                if parts[-1] == "lib":
                    parts.pop()
                module = "::".join([prefix, *parts]).rstrip(":")
                source_files.append((filepath, module))

        for filepath, source_file in source_files:
            self.source_paths[source_file] = filepath
            self.imports[source_file] = extract_imports(filepath)
            for struct in extract_structs_from_file(
                filepath,
                root,
                source_file=source_file,
            ):
                self.structs.append(struct)
                self.by_source_name[(source_file, struct.name)] = struct
                self.by_name[struct.name].append(struct)
            for enum in extract_enums_from_file(
                filepath,
                root,
                source_file=source_file,
            ):
                self.enums.append(enum)
                self.enums_by_source_name[(source_file, enum.name)] = enum
                self.enums_by_name[enum.name].append(enum)

        collision_counts = Counter(
            [struct.name for struct in self.structs]
            + [enum.name for enum in self.enums]
        )
        for struct in self.structs:
            if (
                collision_counts[struct.name] > 1
                or struct.name in FORCE_QUALIFIED_SCHEMA_NAMES
            ):
                struct.schema_name = f"{module_prefix(struct.source_file)}{struct.name}"
        for enum in self.enums:
            if (
                collision_counts[enum.name] > 1
                or enum.name in FORCE_QUALIFIED_SCHEMA_NAMES
            ):
                enum.schema_name = f"{module_prefix(enum.source_file)}{enum.name}"

        self.by_schema_name = {
            struct.schema_name: struct for struct in self.structs
        }
        self.enums_by_schema_name = {
            enum.schema_name: enum for enum in self.enums
        }
        self.selected: dict[str, RustStruct] = {}
        self.selected_enums: dict[str, RustEnum] = {}
        for struct in self.structs:
            if struct.source_file in active_sources:
                self.selected[struct.schema_name] = struct

    def qualified_type(self, rust_type: str, source_file: str) -> str:
        """Resolve a short Rust type through local definitions and `use` imports."""
        short = rust_type.split("::")[-1]
        if "::" in rust_type:
            qualified = rust_type.removeprefix("crate::")
            head, remainder = qualified.split("::", 1)
            imported_module = self.imports.get(source_file, {}).get(head)
            if imported_module:
                return f"{imported_module}::{remainder}"
            return qualified
        if (source_file, short) in self.by_source_name:
            return f"{source_file}::{short}"
        imported = self.imports.get(source_file, {}).get(short)
        if imported:
            return imported
        candidates = self.by_name.get(short, [])
        if len(candidates) == 1:
            return f"{candidates[0].source_file}::{short}"
        return short

    def resolve_struct(self, rust_type: str, source_file: str) -> RustStruct | None:
        qualified = self.qualified_type(rust_type, source_file)
        if "::" in qualified:
            module, name = qualified.rsplit("::", 1)
            exact = self.by_source_name.get((module, name))
            if exact is not None:
                return exact
            candidates = [
                struct
                for struct in self.by_name.get(name, [])
                if struct.source_file.startswith(f"{module}::")
            ]
            return candidates[0] if len(candidates) == 1 else None
        candidates = self.by_name.get(qualified, [])
        parent = source_file.rsplit("::", 1)[0] if "::" in source_file else source_file
        local_candidates = [
            struct
            for struct in candidates
            if struct.source_file.startswith(f"{parent}::")
        ]
        if len(local_candidates) == 1:
            return local_candidates[0]
        return candidates[0] if len(candidates) == 1 else None

    def select(self, struct: RustStruct) -> str:
        self.selected[struct.schema_name] = struct
        return struct.schema_name

    def resolve_enum(self, rust_type: str, source_file: str) -> RustEnum | None:
        short = rust_type.split("::")[-1]
        local = self.enums_by_source_name.get((source_file, short))
        if local is not None:
            return local
        qualified = self.qualified_type(rust_type, source_file)
        if "::" in qualified:
            module, name = qualified.rsplit("::", 1)
            exact = self.enums_by_source_name.get((module, name))
            if exact is not None:
                return exact
            candidates = [
                enum
                for enum in self.enums_by_name.get(name, [])
                if enum.source_file.startswith(f"{module}::")
            ]
            return candidates[0] if len(candidates) == 1 else None
        candidates = self.enums_by_name.get(qualified, [])
        parent = source_file.rsplit("::", 1)[0] if "::" in source_file else source_file
        local_candidates = [
            enum
            for enum in candidates
            if enum.source_file.startswith(f"{parent}::")
        ]
        if len(local_candidates) == 1:
            return local_candidates[0]
        return candidates[0] if len(candidates) == 1 else None

    def select_enum(self, enum: RustEnum) -> str:
        self.selected_enums[enum.schema_name] = enum
        return enum.schema_name

    def external_component(self, rust_type: str, source_file: str) -> str | None:
        short = rust_type.split("::")[-1]
        local_component = EXTERNAL_TYPE_COMPONENTS.get(
            f"{source_file}::{short}"
        )
        if local_component:
            return local_component
        qualified = self.qualified_type(rust_type, source_file)
        component = EXTERNAL_TYPE_COMPONENTS.get(qualified)
        if component:
            return component
        if rust_type in EXTERNAL_SCHEMAS:
            return rust_type
        return None


# ---------------------------------------------------------------------------
# OpenAPI generation
# ---------------------------------------------------------------------------

STRING_ENUM_TYPES = {
    'ActionApproval',
    'AgentStatus',
    'AgentType',
    'ApiScope',
    'AutonomyLevel',
    'BenchmarkType',
    'CdcConnectorType',
    'CellType',
    'CloudProvider',
    'ClusterSize',
    'ComplianceFramework',
    'ConsistencyLevel',
    'ConnectorType',
    'ConnectorState',
    'DiscoveryMethod',
    'DomainStatus',
    'FeatureDType',
    'FunctionRuntime',
    'FunctionTrigger',
    'FunctionType',
    'GatewayProtocol',
    'LagSeverity',
    'LineageEdgeType',
    'ListenerStatus',
    'OutputType',
    'PeerRole',
    'PeerStatus',
    'RegionStatus',
    'ResetStrategy',
    'ScanStatus',
    'TenantStatus',
    'TenantTier',
    'TaskState',
    'TransformStatus',
    'WebhookEvent',
}

OBJECT_ENUM_TYPES = {
    'DeletionStatus',
    'GovernorAction',
    'RuleType',
    'TriggerAction',
}

EXTERNAL_SCHEMAS = {
    'AlertsAlertCondition': {
        'type': 'object',
        'required': ['type'],
        'properties': {
            'type': {
                'type': 'string',
                'enum': [
                    'consumer_lag_exceeds',
                    'offline_partitions',
                    'under_replicated_partitions',
                    'message_rate_exceeds',
                    'byte_rate_exceeds',
                ],
            },
            'group': {'type': 'string'},
            'topic': {'type': 'string'},
        },
    },
    'ObservabilityApiAlertCondition': {
        'type': 'string',
        'enum': ['greater_than', 'less_than', 'equal', 'not_equal'],
    },
    'BenchmarkApiBenchmarkStatus': {
        'type': 'object',
        'required': ['state'],
        'properties': {
            'state': {
                'type': 'string',
                'enum': ['pending', 'running', 'completed', 'failed'],
            },
            'error': {'type': 'string', 'nullable': True},
        },
    },
    'CdcApiConnectorStatus': {
        'oneOf': [
            {
                'type': 'string',
                'enum': ['Created', 'Running', 'Stopped', 'Snapshotting'],
            },
            {
                'type': 'object',
                'required': ['Failed'],
                'properties': {'Failed': {'type': 'string'}},
            },
        ],
    },
    'AnalyticsCacheStats': {
        'type': 'object',
        'required': ['total_entries', 'valid_entries', 'expired_entries'],
        'properties': {
            'total_entries': {'type': 'integer'},
            'valid_entries': {'type': 'integer'},
            'expired_entries': {'type': 'integer'},
        },
    },
    'GraphqlRequest': {
        'type': 'object',
        'required': ['query'],
        'properties': {
            'query': {'type': 'string'},
            'operationName': {'type': 'string', 'nullable': True},
            'variables': {'type': 'object', 'additionalProperties': True},
        },
    },
    'GraphqlResponse': {
        'type': 'object',
        'properties': {
            'data': {},
            'errors': {'type': 'array', 'items': {'type': 'object'}},
            'extensions': {'type': 'object', 'additionalProperties': True},
        },
    },
}

EXTERNAL_TYPE_COMPONENTS = {
    'server::alerts::AlertCondition': 'AlertsAlertCondition',
    'server::observability_api::AlertCondition': 'ObservabilityApiAlertCondition',
    'server::benchmark_api::BenchmarkStatus': 'BenchmarkApiBenchmarkStatus',
    'server::cdc_api::ConnectorStatus': 'CdcApiConnectorStatus',
    'analytics::duckdb::CacheStats': 'AnalyticsCacheStats',
    'async_graphql::Request': 'GraphqlRequest',
    'async_graphql::Response': 'GraphqlResponse',
}


def renamed_variant(name: str, rename_all: str | None) -> str:
    words = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name).lower().split('_')
    if rename_all == "kebab-case":
        return "-".join(words)
    if rename_all == "SCREAMING-KEBAB-CASE":
        return "-".join(words).upper()
    if rename_all == "lowercase":
        return name.lower()
    if rename_all == "UPPERCASE":
        return name.upper()
    if rename_all == "SCREAMING_SNAKE_CASE":
        return "_".join(words).upper()
    if rename_all == "camelCase":
        return words[0] + "".join(word.capitalize() for word in words[1:])
    if rename_all == "PascalCase":
        return "".join(word.capitalize() for word in words)
    return "_".join(words) if rename_all == "snake_case" else name


def schema_with_description(schema: dict, description: str) -> dict:
    """Attach a field description without adding an illegal sibling to `$ref`."""
    if not description:
        return schema
    if '$ref' in schema:
        return {'allOf': [schema], 'description': description}
    return {**schema, 'description': description}


def fields_to_openapi_object(
    fields: list[StructField],
    catalog: SchemaCatalog,
    source_file: str,
    initial_properties: dict | None = None,
    initial_required: list[str] | None = None,
) -> dict:
    """Build an object schema, composing flattened Serde fields with `allOf`."""
    properties = OrderedDict(initial_properties or {})
    required = list(initial_required or [])
    flattened = []

    for field in fields:
        if field.serde_skip:
            continue
        schema = rust_type_to_openapi(
            field.rust_type,
            catalog,
            source_file,
        )
        if field.serde_flatten:
            flattened.append(schema)
            continue
        properties[field.name] = schema_with_description(schema, field.doc)
        if not field.optional:
            required.append(field.name)

    base = {'type': 'object'}
    if required:
        base['required'] = required
    if properties:
        base['properties'] = properties
    if not flattened:
        return base

    return {'allOf': [base, *flattened]}


def enum_variant_payload(
    variant: EnumVariant,
    catalog: SchemaCatalog,
    source_file: str,
) -> dict:
    """Return the serialized payload schema for an enum variant."""
    if variant.fields:
        return fields_to_openapi_object(
            variant.fields,
            catalog,
            source_file,
        )
    if len(variant.tuple_types) == 1:
        return rust_type_to_openapi(
            variant.tuple_types[0],
            catalog,
            source_file,
        )
    if variant.tuple_types:
        return {
            'type': 'array',
            'items': {
                'oneOf': [
                    rust_type_to_openapi(item, catalog, source_file)
                    for item in variant.tuple_types
                ],
            },
            'minItems': len(variant.tuple_types),
            'maxItems': len(variant.tuple_types),
        }
    return {}


def enum_to_openapi(enum: RustEnum, catalog: SchemaCatalog) -> dict:
    variants = [
        (
            variant,
            variant.explicit_name
            or renamed_variant(variant.name, enum.rename_all),
        )
        for variant in enum.variants
    ]
    if enum.tag:
        choices = []
        for variant, serialized_name in variants:
            tag_schema = {
                'type': 'string',
                'enum': [serialized_name],
            }
            if enum.content:
                properties = {enum.tag: tag_schema}
                required = [enum.tag]
                if variant.has_payload:
                    properties[enum.content] = enum_variant_payload(
                        variant,
                        catalog,
                        enum.source_file,
                    )
                    required.append(enum.content)
                choices.append({
                    'type': 'object',
                    'required': required,
                    'properties': properties,
                })
            elif variant.fields:
                choices.append(
                    fields_to_openapi_object(
                        variant.fields,
                        catalog,
                        enum.source_file,
                        initial_properties={enum.tag: tag_schema},
                        initial_required=[enum.tag],
                    )
                )
            else:
                choice = {
                    'type': 'object',
                    'required': [enum.tag],
                    'properties': {enum.tag: tag_schema},
                }
                if variant.has_payload:
                    choice['additionalProperties'] = True
                choices.append(choice)
        return {
            'oneOf': choices,
            'discriminator': {'propertyName': enum.tag},
        }

    if enum.untagged:
        return {
            'oneOf': [
                (
                    enum_variant_payload(variant, catalog, enum.source_file)
                    if variant.has_payload
                    else {'nullable': True}
                )
                for variant, _ in variants
            ]
        }

    if all(not variant.has_payload for variant, _ in variants):
        return {
            'type': 'string',
            'enum': [serialized_name for _, serialized_name in variants],
        }

    choices = []
    for variant, serialized_name in variants:
        if variant.has_payload:
            choices.append({
                'type': 'object',
                'required': [serialized_name],
                'properties': {
                    serialized_name: enum_variant_payload(
                        variant,
                        catalog,
                        enum.source_file,
                    )
                },
            })
        else:
            choices.append({'type': 'string', 'enum': [serialized_name]})
    return {'oneOf': choices}


def rust_type_to_openapi(
    rust_type: str,
    catalog: SchemaCatalog,
    source_file: str,
) -> dict:
    """Convert a Rust type to an OpenAPI schema fragment."""
    inner = rust_type.strip()

    # Strip references and transparent wrappers.
    inner = re.sub(r"^&\s*(?:'[A-Za-z_][A-Za-z0-9_]*\s+)?", '', inner)
    wrapper = re.match(
        r'^(?:Option|Box|Arc|Rc|Cow|std::sync::Arc|std::rc::Rc)<(.+)>$',
        inner,
    )
    if wrapper:
        return rust_type_to_openapi(wrapper.group(1).strip(), catalog, source_file)

    # Primitives
    type_map = {
        'String': {'type': 'string'},
        'std::string::String': {'type': 'string'},
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
        'JsonValue': {},
        'serde_yaml::Value': {},
        'AtomicU32': {'type': 'integer', 'format': 'int32'},
        'AtomicU64': {'type': 'integer', 'format': 'int64'},
        'std::sync::atomic::AtomicU32': {'type': 'integer', 'format': 'int32'},
        'std::sync::atomic::AtomicU64': {'type': 'integer', 'format': 'int64'},
    }

    if inner in type_map:
        return type_map[inner]

    if re.fullmatch(r'(?:chrono::)?DateTime<(?:chrono::)?Utc>', inner):
        return {'type': 'string', 'format': 'date-time'}

    if inner in {'SystemTime', 'std::time::SystemTime'}:
        return {'type': 'string', 'format': 'date-time'}

    if inner in {'Uuid', 'uuid::Uuid'}:
        return {'type': 'string', 'format': 'uuid'}

    if inner in {
        'IpAddr',
        'Ipv4Addr',
        'Ipv6Addr',
        'SocketAddr',
        'std::net::IpAddr',
        'std::net::Ipv4Addr',
        'std::net::Ipv6Addr',
        'std::net::SocketAddr',
        'PathBuf',
        'std::path::PathBuf',
        'Url',
        'url::Url',
    }:
        return {'type': 'string'}

    # Sequence types.
    sequence_match = re.match(
        r'^(?:Vec|VecDeque|HashSet|BTreeSet|std::collections::(?:VecDeque|HashSet|BTreeSet))<(.+)>$',
        inner,
    )
    if sequence_match:
        item_type = sequence_match.group(1).strip()
        schema = {
            'type': 'array',
            'items': rust_type_to_openapi(item_type, catalog, source_file),
        }
        if 'Set<' in inner:
            schema['uniqueItems'] = True
        return schema

    array_match = re.match(r'^\[(.+);\s*(\d+)\]$', inner)
    if array_match:
        length = int(array_match.group(2))
        return {
            'type': 'array',
            'items': rust_type_to_openapi(
                array_match.group(1).strip(),
                catalog,
                source_file,
            ),
            'minItems': length,
            'maxItems': length,
        }

    # JSON object maps (non-string Rust keys serialize as property names).
    hm_match = re.match(
        r'^(?:(?:std::collections::)?(?:HashMap|BTreeMap)|IndexMap)<(.+)>$',
        inner,
    )
    if hm_match:
        arguments = split_top_level(hm_match.group(1))
        if len(arguments) != 2:
            raise ValueError(
                f"malformed Rust map type `{rust_type}` referenced from `{source_file}`"
            )
        val_schema = rust_type_to_openapi(
            arguments[1].strip(),
            catalog,
            source_file,
        )
        return {'type': 'object', 'additionalProperties': val_schema}

    # Rust tuples do not map to OpenAPI tuples; preserve their JSON array shape.
    if inner.startswith('(') and inner.endswith(')'):
        return {'type': 'array', 'items': {}}

    external_component = catalog.external_component(inner, source_file)
    if external_component is not None:
        return {'$ref': f'#/components/schemas/{external_component}'}

    struct = catalog.resolve_struct(inner, source_file)
    if struct is not None:
        return {
            '$ref': f'#/components/schemas/{catalog.select(struct)}'
        }

    enum = catalog.resolve_enum(inner, source_file)
    if enum is not None:
        return {
            '$ref': f'#/components/schemas/{catalog.select_enum(enum)}'
        }

    if inner in STRING_ENUM_TYPES:
        return {'type': 'string'}

    if inner in OBJECT_ENUM_TYPES:
        return {'type': 'object', 'additionalProperties': True}

    raise ValueError(
        f"unresolved Rust type `{rust_type}` referenced from `{source_file}`"
    )


def tag_from_source(source_file: str, path: str) -> str:
    """Derive an API tag from the source file name and route path."""
    tag_map = {
        'api': 'Topics',
        'consumer_api': 'Consumer Groups',
        'schema_api': 'Schema Registry',
        'search_api': 'Semantic Search',
        'analytics_api': 'Analytics',
        'dashboard_api': 'Dashboard',
        'cluster_api': 'Cluster',
        'benchmark_api': 'Benchmark',
        'logs_api': 'Logs',
        'connections_api': 'Connections',
        'alerts_api': 'Alerts',
        'cdc_api': 'CDC',
        'cloud_api': 'Cloud',
        'gitops_api': 'GitOps',
        'wasm_api': 'WASM',
        'observability_api': 'Observability',
        'playground_api': 'Playground',
        'ai_api': 'AI Pipelines',
        'featurestore_api': 'Feature Store',
        'feature_store_api': 'Feature Store',
        'plugin_api': 'Plugins',
        'replication_api': 'Replication',
        'edge_api': 'Edge',
        'failover_api': 'Failover',
        'raft_cluster_api': 'Raft Cluster',
        'connector_mgmt_api': 'Connector Management',
        'console_api': 'Console',
        'governor_api': 'Resource Governor',
        'branches_api': 'Branches',
        'memory_api': 'Agent Memory',
        'query_api': 'Query',
        'sqlite_routes': 'SQLite',
        'scaling_metrics': 'Scaling',
        'streamql_api': 'StreamQL',
    }
    return tag_map.get(source_file.split("::")[-1], 'General')


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


def route_operation_id(route: Route, duplicate_counts: Counter) -> str:
    """Build a deterministic operationId, namespacing repeated handler names."""
    base = handler_to_operation_id(route.handler)
    if duplicate_counts[base] == 1:
        return base

    source = re.sub(r'_api$', '', Path(route.source_file).stem)
    if source == 'api':
        source = 'topics'
    path_parts = [
        part.strip('{}')
        for part in route.path.split('/')
        if part and part not in {'api', 'v1', 'v2'}
    ]
    if route.path == "/":
        path_parts = ["root"]
    elif route.path.endswith("/"):
        path_parts.append("trailing_slash")
    raw = '_'.join([source, route.handler, route.method.lower(), *path_parts])
    raw = re.sub(r'[^A-Za-z0-9_]+', '_', raw).strip('_')
    return handler_to_operation_id(raw)


class HandlerSchemas:
    """Request and success-response Rust types from an Axum handler signature."""

    def __init__(
        self,
        request: str | None = None,
        response: str | None = None,
        source_file: str | None = None,
    ):
        self.request = request
        self.response = response
        self.source_file = source_file


def generic_arguments(text: str, generic: str) -> list[str]:
    """Extract balanced generic arguments such as `Json<Foo>`."""
    found = []
    for match in re.finditer(rf'\b{re.escape(generic)}\s*<', text):
        start = text.find('<', match.start())
        depth = 1
        cursor = start + 1
        while cursor < len(text) and depth:
            if text[cursor] == '<':
                depth += 1
            elif text[cursor] == '>':
                depth -= 1
            cursor += 1
        if depth == 0:
            found.append(text[start + 1:cursor - 1].strip())
    return found


def handler_schemas(root: Path, routes: list[Route]) -> dict[tuple[str, str], HandlerSchemas]:
    """Extract request/response DTOs for every mounted handler."""
    definitions: dict[str, list[tuple[str, Path, ModuleFunction]]] = defaultdict(list)
    for filepath in sorted((root / "src").rglob("*.rs")):
        content = filepath.read_text()
        source_file = source_name(root, filepath)
        for function in module_functions(content):
            definitions[function.name].append((source_file, filepath, function))

    result: dict[tuple[str, str], HandlerSchemas] = {}
    for route in routes:
        candidates = definitions.get(route.handler, [])
        local = [
            candidate
            for candidate in candidates
            if candidate[0] == route.source_file
        ]
        nested = [
            candidate
            for candidate in candidates
            if candidate[0].startswith(f"{route.source_file}::")
        ]
        preferred = local or nested or candidates
        if not preferred:
            continue

        compatible = [
            candidate
            for candidate in preferred
            if (
                candidate[2].required_feature in {None, route.feature}
                and (
                    candidate[2].excluded_feature is None
                    or candidate[2].excluded_feature != route.feature
                )
            )
        ]
        exact_feature = [
            candidate
            for candidate in compatible
            if candidate[2].required_feature == route.feature
            and route.feature is not None
        ]
        preferred = exact_feature or compatible
        if not preferred:
            continue

        source_modules = {candidate[0] for candidate in preferred}
        signatures = {
            (
                tuple(generic_arguments(candidate[2].parameters, "Json")),
                tuple(generic_arguments(candidate[2].return_type, "Json")),
            )
            for candidate in preferred
        }
        if len(source_modules) > 1 or len(signatures) > 1:
            locations = ", ".join(
                f"{candidate[0]} ({candidate[1]})" for candidate in preferred
            )
            raise RuntimeError(
                f"mounted handler `{route.handler}` for "
                f"{route.method} {route.path} is ambiguous across: {locations}"
            )

        source_file, _filepath, function = preferred[0]
        request_types = generic_arguments(function.parameters, "Json")
        response_types = generic_arguments(function.return_type, "Json")
        result[(route.source_file, route.handler)] = HandlerSchemas(
            request=request_types[0] if request_types else None,
            response=response_types[0] if response_types else None,
            source_file=source_file,
        )

    overrides = {
        ("server::search_api", "search_handler"): HandlerSchemas(
            request="SearchRequest",
            response="SearchResponse",
            source_file="server::search_api",
        ),
    }
    result.update(overrides)
    return result


def infer_request_schema(handler: str, path: str) -> str | None:
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
    catalog = SchemaCatalog(root, {route.source_file for route in routes})
    handlers = handler_schemas(root, routes)
    duplicate_operation_ids = Counter(handler_to_operation_id(route.handler) for route in routes)
    operation_schemas = {}

    for route in routes:
        handler = handlers.get((route.source_file, route.handler), HandlerSchemas())
        schema_source = handler.source_file or route.source_file
        request_type = handler.request
        if request_type is None:
            request_type = infer_request_schema(route.handler, route.path)
        request_schema = (
            rust_type_to_openapi(request_type, catalog, schema_source)
            if request_type
            else None
        )
        response_schema = (
            rust_type_to_openapi(handler.response, catalog, schema_source)
            if handler.response
            else {'type': 'object'}
        )
        operation_schemas[
            (route.path, route.method, route.handler, route.source_file)
        ] = (request_schema, response_schema)

    # Pull every referenced DTO into the emitted component set. Resolution is
    # source-aware, so imported and colliding names keep their module identity.
    while True:
        before = (len(catalog.selected), len(catalog.selected_enums))
        for struct in list(catalog.selected.values()):
            for field in struct.fields:
                if not field.serde_skip:
                    rust_type_to_openapi(
                        field.rust_type,
                        catalog,
                        struct.source_file,
                    )
        for enum in list(catalog.selected_enums.values()):
            for variant in enum.variants:
                for field in variant.fields:
                    if not field.serde_skip:
                        rust_type_to_openapi(
                            field.rust_type,
                            catalog,
                            enum.source_file,
                        )
                for tuple_type in variant.tuple_types:
                    rust_type_to_openapi(
                        tuple_type,
                        catalog,
                        enum.source_file,
                    )
        if (len(catalog.selected), len(catalog.selected_enums)) == before:
            break

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

    def write_schema_value(schema: dict, indent: int):
        """Write an OpenAPI schema dict as YAML at the given indent level."""
        if not schema:
            w("{}", indent)
            return

        def scalar(value):
            if isinstance(value, bool):
                return str(value).lower()
            if value is None:
                return "null"
            if isinstance(value, str):
                if (
                    re.fullmatch(r'[A-Za-z0-9_.-]+', value)
                    and value.lower() not in {'true', 'false', 'null', 'yes', 'no'}
                ):
                    return value
                return json.dumps(value, ensure_ascii=False)
            return str(value)

        for key, value in schema.items():
            if isinstance(value, dict):
                if value:
                    w(f"{key}:", indent)
                    write_schema_value(value, indent + 1)
                else:
                    w(f"{key}: {{}}", indent)
            elif isinstance(value, list):
                if not value:
                    w(f"{key}: []", indent)
                    continue
                w(f"{key}:", indent)
                for item in value:
                    if isinstance(item, dict):
                        w("-", indent + 1)
                        write_schema_value(item, indent + 2)
                    else:
                        w(f"- {scalar(item)}", indent + 1)
            else:
                w(f"{key}: {scalar(value)}", indent)

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
            op_id = route_operation_id(route, duplicate_operation_ids)
            summary = handler_to_summary(route.handler)
            request_schema, response_schema = operation_schemas[
                (route.path, route.method, route.handler, route.source_file)
            ]

            w(f"{method_lower}:", 2)
            w("tags:", 3)
            w(f"- {tag}", 4)
            w(f"summary: {summary}", 3)
            w(f"operationId: {op_id}", 3)

            if route.feature:
                w(f"description: \"Requires feature: {route.feature}\"", 3)
            elif route.alternates:
                alternate_handlers = ", ".join(
                    f"{alternate.handler}"
                    + (
                        f" (requires {alternate.feature})"
                        if alternate.feature
                        else ""
                    )
                    for alternate in route.alternates
                )
                w(
                    f"description: \"Runtime-conditional alternate handlers: "
                    f"{alternate_handlers}\"",
                    3,
                )

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
                if request_schema:
                    w("requestBody:", 3)
                    w("required: true", 4)
                    w("content:", 4)
                    w("application/json:", 5)
                    w("schema:", 6)
                    write_schema_value(request_schema, 7)

            # Responses
            w("responses:", 3)
            w("'200':", 4)
            w("description: Successful operation", 5)
            w("content:", 5)
            w("application/json:", 6)
            w("schema:", 7)
            write_schema_value(response_schema, 8)
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

    # Generate schemas for all extracted structs
    for name in sorted(catalog.selected):
        s = catalog.selected[name]
        w(f"{name}:", 2)
        write_schema_value(
            fields_to_openapi_object(
                s.fields,
                catalog,
                s.source_file,
            ),
            3,
        )
        w()

    for name in sorted(catalog.selected_enums):
        w(f"{name}:", 2)
        write_schema_value(
            enum_to_openapi(catalog.selected_enums[name], catalog),
            3,
        )
        w()

    used_schema_names = set(catalog.selected) | set(catalog.selected_enums)
    for name in sorted(set(EXTERNAL_SCHEMAS) - used_schema_names):
        w(f"{name}:", 2)
        write_schema_value(EXTERNAL_SCHEMAS[name], 3)
        w()

    # Ensure ErrorResponse is always present
    if 'ErrorResponse' not in catalog.selected:
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

if __name__ == "__main__":
    main()

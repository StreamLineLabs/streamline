#!/usr/bin/env python3
"""Fail when undocumented Rust unsafe usage exceeds the reviewed baseline."""

from __future__ import annotations

import argparse
import glob
import os
import re
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

DEFAULT_BUDGET = 120


@dataclass
class SourceScan:
    unsafe_lines: set[int]
    safety_comment_lines: set[int]
    comment_only_lines: set[int]
    code_lines: set[int]


def raw_string_prefix(source: str, index: int) -> tuple[int, int] | None:
    """Return `(content_start, hash_count)` for a Rust raw string prefix."""
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


def char_literal_end(source: str, index: int) -> int | None:
    """Return the closing quote index, or `None` when `'` starts a lifetime."""
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


def scan_rust_source(source: str) -> SourceScan:
    """Lex enough Rust to distinguish code, comments, and every string form."""
    unsafe_lines: set[int] = set()
    safety_comment_lines: set[int] = set()
    comment_lines: set[int] = set()
    code_lines: set[int] = set()
    comment_text: dict[int, list[str]] = {}

    line = 1
    index = 0
    block_depth = 0
    state = "code"
    raw_hashes = 0
    escaped = False

    def mark_comment(char: str = "") -> None:
        comment_lines.add(line)
        if char:
            comment_text.setdefault(line, []).append(char)

    def advance_newline(literal_continues: bool = False) -> None:
        nonlocal line
        if "SAFETY:" in "".join(comment_text.get(line, [])):
            safety_comment_lines.add(line)
        line += 1
        if literal_continues:
            code_lines.add(line)

    while index < len(source):
        char = source[index]

        if state == "line_comment":
            if char == "\n":
                advance_newline()
                state = "code"
            else:
                mark_comment(char)
            index += 1
            continue

        if state == "block_comment":
            mark_comment(char)
            if source.startswith("/*", index):
                block_depth += 1
                mark_comment("*")
                index += 2
            elif source.startswith("*/", index):
                block_depth -= 1
                mark_comment("/")
                index += 2
                if block_depth == 0:
                    state = "code"
            elif char == "\n":
                advance_newline()
                index += 1
            else:
                index += 1
            continue

        if state in {"string", "byte_string", "char", "byte_char"}:
            if char == "\n":
                advance_newline(literal_continues=True)
                escaped = False
                index += 1
            elif escaped:
                escaped = False
                index += 1
            elif char == "\\":
                escaped = True
                index += 1
            elif (state in {"string", "byte_string"} and char == '"') or (
                state in {"char", "byte_char"} and char == "'"
            ):
                state = "code"
                index += 1
            else:
                index += 1
            continue

        if state == "raw_string":
            if char == "\n":
                advance_newline(literal_continues=True)
                index += 1
                continue
            if char == '"' and source.startswith("#" * raw_hashes, index + 1):
                index += raw_hashes + 1
                state = "code"
                continue
            index += 1
            continue

        if source.startswith("//", index):
            mark_comment("/")
            mark_comment("/")
            state = "line_comment"
            index += 2
            continue

        if source.startswith("/*", index):
            mark_comment("/")
            mark_comment("*")
            state = "block_comment"
            block_depth = 1
            index += 2
            continue

        raw_prefix = raw_string_prefix(source, index)
        if raw_prefix is not None:
            content_start, raw_hashes = raw_prefix
            code_lines.add(line)
            state = "raw_string"
            index = content_start
            continue

        if source.startswith(("b\"", "c\""), index):
            code_lines.add(line)
            state = "byte_string"
            escaped = False
            index += 2
            continue

        if char == '"':
            code_lines.add(line)
            state = "string"
            escaped = False
            index += 1
            continue

        if source.startswith("b'", index):
            closing = char_literal_end(source, index + 1)
            if closing is not None:
                code_lines.add(line)
                state = "byte_char"
                escaped = False
                index += 2
                continue

        if char == "'" and char_literal_end(source, index) is not None:
            code_lines.add(line)
            state = "char"
            escaped = False
            index += 1
            continue

        if char == "\n":
            advance_newline()
            index += 1
            continue

        if char.isspace():
            index += 1
            continue

        if char == "r" and index + 2 < len(source) and source[index + 1] == "#":
            identifier = re.match(r"r#[A-Za-z_][A-Za-z0-9_]*", source[index:])
            if identifier:
                code_lines.add(line)
                index += len(identifier.group(0))
                continue

        if char.isalpha() or char == "_":
            identifier = re.match(r"[A-Za-z_][A-Za-z0-9_]*", source[index:])
            assert identifier is not None
            token = identifier.group(0)
            code_lines.add(line)
            if token == "unsafe":
                unsafe_lines.add(line)
            index += len(token)
            continue

        code_lines.add(line)
        index += 1

    if "SAFETY:" in "".join(comment_text.get(line, [])):
        safety_comment_lines.add(line)

    return SourceScan(
        unsafe_lines=unsafe_lines,
        safety_comment_lines=safety_comment_lines,
        comment_only_lines=comment_lines - code_lines,
        code_lines=code_lines,
    )


def has_adjacent_safety_comment(lines: list[str], scan: SourceScan, line: int) -> bool:
    if line in scan.safety_comment_lines:
        return True

    previous = line - 1
    while previous >= 1:
        stripped = lines[previous - 1].strip()
        if not stripped:
            break
        if previous in scan.comment_only_lines or stripped.startswith("#["):
            if previous in scan.safety_comment_lines:
                return True
            previous -= 1
            continue
        break

    following = line + 1
    while following <= len(lines) and not lines[following - 1].strip():
        following += 1
    return (
        following <= len(lines)
        and following in scan.comment_only_lines
        and following in scan.safety_comment_lines
    )


def production_source_roots(root: Path) -> list[Path]:
    """Return every production `src/` tree in the Cargo workspace."""
    manifest = root / "Cargo.toml"
    if not manifest.exists():
        source = root / "src"
        return [source] if source.is_dir() else []

    with manifest.open("rb") as cargo_file:
        cargo = tomllib.load(cargo_file)

    member_patterns = cargo.get("workspace", {}).get("members")
    if member_patterns is None:
        member_patterns = ["."]

    member_roots: set[Path] = set()
    for pattern in member_patterns:
        matches = glob.glob(str(root / pattern))
        for match in matches:
            member = Path(match).resolve()
            if (member / "Cargo.toml").is_file() and (member / "src").is_dir():
                member_roots.add(member)

    if cargo.get("package") and (root / "src").is_dir():
        member_roots.add(root)

    return sorted(member / "src" for member in member_roots)


def undocumented_unsafe(root: Path) -> list[str]:
    occurrences: list[str] = []
    for source_root in production_source_roots(root):
        for path in sorted(source_root.rglob("*.rs")):
            source = path.read_text()
            lines = source.splitlines()
            scan = scan_rust_source(source)
            for line in sorted(scan.unsafe_lines):
                if not has_adjacent_safety_comment(lines, scan, line):
                    occurrences.append(f"{path.relative_to(root)}:{line}")
    return occurrences


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parent.parent,
        help="repository or fixture root containing src/",
    )
    parser.add_argument(
        "--budget",
        type=int,
        default=None,
        help="override UNSAFE_BUDGET for fixture tests",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = args.root.resolve()
    try:
        budget = (
            args.budget
            if args.budget is not None
            else int(os.environ.get("UNSAFE_BUDGET", str(DEFAULT_BUDGET)))
        )
    except ValueError:
        print("UNSAFE_BUDGET must be an integer", file=sys.stderr)
        return 2

    occurrences = undocumented_unsafe(root)
    message = (
        f"unsafe source lines without an adjacent SAFETY justification: "
        f"{len(occurrences)} (budget: {budget})"
    )
    print(message)

    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a") as output:
            output.write(f"{message}\n")

    if len(occurrences) > budget:
        print("\n".join(occurrences), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Load and validate an ISA-JSON file using isatools."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable

IGNORED_ERROR_CODES = {4002}


def _format_issue(issue: Dict[str, Any]) -> str:
    code = issue.get("code", "n/a")
    message = issue.get("message", "Unknown issue")
    supplemental = issue.get("supplemental", "")
    if supplemental:
        return f"[{code}] {message}: {supplemental}"
    return f"[{code}] {message}"


def _print_issues(title: str, issues: Iterable[Dict[str, Any]]) -> None:
    issues = list(issues)
    if not issues:
        return
    print(f"{title} ({len(issues)}):")
    for issue in issues:
        print(f"  - {_format_issue(issue)}")


def _issue_code_as_int(issue: Dict[str, Any]) -> int | None:
    code = issue.get("code")
    try:
        return int(code)
    except (TypeError, ValueError):
        return None


def _format_path_key(path: str, key: str) -> str:
    if key.isidentifier():
        return f"{path}.{key}"
    escaped = key.replace("\\", "\\\\").replace('"', '\\"')
    return f'{path}["{escaped}"]'


def _find_non_string_comment_values(
    node: Any, path: str = "$"
) -> list[tuple[str, str, str]]:
    results: list[tuple[str, str, str]] = []
    if isinstance(node, dict):
        comments = node.get("comments")
        if isinstance(comments, list):
            for i, comment in enumerate(comments):
                if (
                    isinstance(comment, dict)
                    and "value" in comment
                    and not isinstance(comment["value"], str)
                ):
                    value = comment["value"]
                    results.append(
                        (
                            f"{path}.comments[{i}].value",
                            type(value).__name__,
                            repr(value),
                        )
                    )
        for key, value in node.items():
            child_path = _format_path_key(path, str(key))
            results.extend(_find_non_string_comment_values(value, child_path))
    elif isinstance(node, list):
        for i, value in enumerate(node):
            results.extend(_find_non_string_comment_values(value, f"{path}[{i}]"))
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load and validate an ISA-JSON file using the isatools API."
    )
    parser.add_argument(
        "filename",
        help="Path to the ISA-JSON file to verify.",
    )
    parser.add_argument(
        "--fail-on-warnings",
        action="store_true",
        help="Return exit code 1 when warnings are present.",
    )
    return parser.parse_args()


def _validate_via_normalized_utf8(
    isajson_module: Any, isa_json: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Validate using a normalized UTF-8 JSON temp file.

    isatools' validator uses chardet and can mis-detect UTF-8 files as cp1252
    with low confidence, then raises a SystemError. Writing canonical UTF-8
    (ASCII-safe) avoids that false negative while preserving JSON content.
    """
    temp_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            suffix=".json",
            delete=False,
        ) as temp_handle:
            temp_path = temp_handle.name
            json.dump(isa_json, temp_handle, ensure_ascii=True, separators=(",", ":"))
            temp_handle.flush()

        with open(temp_path, "r", encoding="utf-8") as handle:
            return isajson_module.validate(handle)
    finally:
        if temp_path:
            try:
                Path(temp_path).unlink(missing_ok=True)
            except Exception:
                pass


def main() -> int:
    args = parse_args()
    isa_path = Path(args.filename)

    if not isa_path.exists():
        print(f"File not found: {isa_path}", file=sys.stderr)
        return 2
    if not isa_path.is_file():
        print(f"Not a file: {isa_path}", file=sys.stderr)
        return 2

    try:
        with isa_path.open("r", encoding="utf-8") as handle:
            isa_json = json.load(handle)
    except Exception as exc:  # pragma: no cover - catches parser failures
        print(f"Failed to parse JSON: {exc}", file=sys.stderr)
        return 2

    bad_comment_values = _find_non_string_comment_values(isa_json)
    if bad_comment_values:
        print(
            f"Found {len(bad_comment_values)} invalid comment value(s) "
            "(each comments[].value must be a string):",
            file=sys.stderr,
        )
        for path, type_name, value_repr in bad_comment_values[:50]:
            print(
                f"  - {path} -> {type_name} {value_repr}",
                file=sys.stderr,
            )
        if len(bad_comment_values) > 50:
            print(
                f"  ... and {len(bad_comment_values) - 50} more",
                file=sys.stderr,
            )
        return 1

    try:
        from isatools import isajson
    except ModuleNotFoundError:
        print(
            "Missing dependency: isatools is not installed in this environment.",
            file=sys.stderr,
        )
        return 2

    try:
        with isa_path.open("r", encoding="utf-8") as handle:
            investigation = isajson.load(handle)
    except Exception as exc:  # pragma: no cover - catches parser/library failures
        print(f"Failed to load ISA-JSON: {exc}", file=sys.stderr)
        return 2

    study_count = len(getattr(investigation, "studies", []))
    identifier = getattr(investigation, "identifier", "") or "<no identifier>"
    print(f"Loaded ISA-JSON: identifier={identifier}, studies={study_count}")

    try:
        report = _validate_via_normalized_utf8(isajson, isa_json)
    except Exception as exc:  # pragma: no cover - catches validator/library failures
        print(f"Validation failed to run: {exc}", file=sys.stderr)
        return 2

    errors = report.get("errors", [])
    warnings = report.get("warnings", [])
    blocking_errors = [
        error for error in errors if _issue_code_as_int(error) not in IGNORED_ERROR_CODES
    ]
    ignored_errors = [
        error for error in errors if _issue_code_as_int(error) in IGNORED_ERROR_CODES
    ]

    _print_issues("Errors", blocking_errors)
    _print_issues("Ignored Errors", ignored_errors)
    _print_issues("Warnings", warnings)

    if blocking_errors:
        print("Result: INVALID (errors found)")
        return 1
    if warnings and args.fail_on_warnings:
        print("Result: WARNING-ONLY (treated as failure due to --fail-on-warnings)")
        return 1

    if warnings or ignored_errors:
        print("Result: VALID with warnings")
    else:
        print("Result: VALID")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

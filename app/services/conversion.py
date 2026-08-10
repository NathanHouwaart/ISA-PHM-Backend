from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import jsonschema
from fastapi import UploadFile

from app.config import Settings
from app.errors import APIError, ConverterFailedError, ConverterNotFoundError, ConverterTimeoutError
from app.semantic_validation import validate_payload_semantics


def check_converter_readiness(settings: Settings) -> list[str]:
    if not settings.converter_script_path.exists():
        return [f"Converter script not found: {settings.converter_script_path}"]
    try:
        result = subprocess.run(
            [settings.converter_python, "--version"], check=False,
            capture_output=True, text=True, timeout=5,
        )
    except FileNotFoundError:
        return [f"Converter interpreter not found: {settings.converter_python}"]
    except subprocess.TimeoutExpired:
        return [f"Converter interpreter timed out: {settings.converter_python}"]
    if result.returncode == 0:
        return []
    output = (result.stderr or result.stdout or "").strip()
    return [f"Converter interpreter check failed (exit {result.returncode}): {output}"]


def _validation_error_details(exc: jsonschema.ValidationError) -> dict[str, Any]:
    path = "$" + "".join(
        f"[{segment}]" if isinstance(segment, int) else f".{segment}"
        for segment in exc.path
    )
    return {"path": path, "validator": exc.validator, "message": exc.message}


async def read_and_validate_payload(
    file: UploadFile,
    schema: dict[str, Any] | None,
    schema_path: str,
    settings: Settings,
) -> tuple[dict[str, Any], int]:
    if not file.filename or not file.filename.lower().endswith(".json"):
        raise APIError(400, "invalid_file_extension", "Only .json files are allowed")
    allowed_types = {"application/json", "text/json"}
    if file.content_type not in allowed_types:
        raise APIError(400, "invalid_file_type", "Invalid file type", {
            "content_type": file.content_type, "allowed": sorted(allowed_types),
        })

    raw_bytes = await file.read()
    if len(raw_bytes) > settings.max_upload_bytes:
        raise APIError(413, "payload_too_large", f"Uploaded file exceeds {settings.max_upload_mb} MB limit")
    try:
        payload_text = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise APIError(400, "invalid_encoding", "Payload must be UTF-8 encoded JSON", {"message": str(exc)}) from exc
    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError as exc:
        raise APIError(400, "invalid_json", "Invalid JSON payload", {
            "line": exc.lineno, "column": exc.colno, "message": exc.msg,
        }) from exc
    if schema is None:
        raise APIError(503, "schema_unavailable", "Payload schema is not available", {"schema_path": schema_path})
    try:
        jsonschema.validate(instance=payload, schema=schema)
    except jsonschema.ValidationError as exc:
        raise APIError(422, "schema_validation_failed", "Payload validation failed", _validation_error_details(exc)) from exc

    semantic_issues = [issue.as_dict() for issue in validate_payload_semantics(payload)]
    if semantic_issues:
        raise APIError(422, "semantic_validation_failed", "Payload semantic validation failed", semantic_issues)
    return payload, len(raw_bytes)


def _run_converter(settings: Settings, input_path: str, output_path: str) -> None:
    command = [settings.converter_python, str(settings.converter_script_path), input_path, output_path]
    try:
        result = subprocess.run(
            command, check=False, capture_output=True, text=True,
            timeout=settings.converter_timeout_seconds,
        )
    except FileNotFoundError as exc:
        raise ConverterNotFoundError(str(exc)) from exc
    except subprocess.TimeoutExpired as exc:
        raise ConverterTimeoutError(str(exc)) from exc
    if result.returncode != 0:
        detail = (result.stderr or "").strip() or (result.stdout or "").strip()
        raise ConverterFailedError(detail or "converter process exited with non-zero status")


def convert_payload(payload: dict[str, Any], settings: Settings) -> str:
    input_path: str | None = None
    output_path: str | None = None
    try:
        encoded_payload = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as input_file:
            input_path = input_file.name
            input_file.write(encoded_payload)
        output_file = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        output_path = output_file.name
        output_file.close()

        try:
            _run_converter(settings, input_path, output_path)
        except ConverterNotFoundError as exc:
            raise APIError(503, "converter_not_found", "Converter runtime is not available", {
                "converter_python": settings.converter_python, "error": str(exc),
            }) from exc
        except ConverterTimeoutError as exc:
            raise APIError(504, "converter_timeout", "Converter process timed out", {
                "timeout_seconds": settings.converter_timeout_seconds, "error": str(exc),
            }) from exc
        except ConverterFailedError as exc:
            raise APIError(500, "converter_failed", "Conversion process failed", {"error": str(exc)}) from exc

        raw_json = Path(output_path).read_text(encoding="utf-8")
        try:
            json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise APIError(500, "invalid_converter_output", "Converter produced invalid JSON", {
                "line": exc.lineno, "column": exc.colno, "message": exc.msg,
            }) from exc
        return raw_json
    finally:
        if input_path:
            Path(input_path).unlink(missing_ok=True)
        if output_path:
            Path(output_path).unlink(missing_ok=True)

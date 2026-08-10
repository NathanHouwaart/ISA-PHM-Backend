from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from fastapi import UploadFile

from app.errors import APIError


@dataclass(frozen=True)
class DatasheetAttachment:
    archive_path: str
    content: bytes


def _safe_archive_path(original_name: str, attachment_id: str) -> str:
    stem = re.sub(r"[^A-Za-z0-9._-]+", "-", Path(original_name).stem).strip(".-") or "datasheet"
    safe_id = re.sub(r"[^A-Za-z0-9_-]+", "", attachment_id)[:16] or "file"
    return f"Datasheets/{stem}-{safe_id}.pdf"


def _parse_manifest(manifest_text: str) -> dict[str, dict[str, Any]]:
    try:
        manifest = json.loads(manifest_text or "[]")
    except json.JSONDecodeError as exc:
        raise APIError(400, "invalid_datasheet_manifest", "Datasheet manifest must be valid JSON") from exc
    if not isinstance(manifest, list):
        raise APIError(400, "invalid_datasheet_manifest", "Datasheet manifest must be a list")

    entries: dict[str, dict[str, Any]] = {}
    for entry in manifest:
        if not isinstance(entry, dict) or not entry.get("attachmentId"):
            continue
        attachment_id = str(entry["attachmentId"])
        if attachment_id in entries:
            raise APIError(400, "invalid_datasheet_manifest", "Datasheet manifest contains duplicate attachment IDs")
        entries[attachment_id] = entry
    return entries


async def _read_uploads(
    uploads: Iterable[UploadFile],
    manifest: dict[str, dict[str, Any]],
    max_upload_bytes: int,
) -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    total_bytes = 0
    for upload in uploads:
        attachment_id = Path(upload.filename or "").stem
        if not attachment_id or attachment_id not in manifest:
            raise APIError(400, "unexpected_datasheet", "Datasheet file does not match the attachment manifest")
        if attachment_id in files:
            raise APIError(400, "unexpected_datasheet", "A datasheet was uploaded more than once")

        content = await upload.read()
        total_bytes += len(content)
        if len(content) > max_upload_bytes or total_bytes > max_upload_bytes:
            raise APIError(413, "payload_too_large", "Uploaded datasheets exceed the configured size limit")
        if not content.startswith(b"%PDF-"):
            raise APIError(400, "invalid_datasheet", "Datasheets must be valid PDF files")
        files[attachment_id] = content
    return files


def _test_setups(payload: dict[str, Any]) -> list[dict[str, Any]]:
    setups = [payload.get("test_setup") or {}]
    setups.extend(
        study.get("used_setup") or {}
        for study in payload.get("studies", [])
        if isinstance(study, dict)
    )
    return setups


def _resolve_attachment(
    datasheet: Any,
    owner_kind: str,
    owner_id: Any,
    owner_label: str,
    manifest: dict[str, dict[str, Any]],
    files: dict[str, bytes],
) -> tuple[str, str, bytes] | None:
    if not isinstance(datasheet, dict) or datasheet.get("notAvailable"):
        return None
    attachment_id = str(datasheet.get("attachmentId") or "")
    if not attachment_id:
        return None

    entry = manifest.get(attachment_id)
    owner = entry.get("owner") if entry else None
    if not isinstance(owner, dict) or owner.get("kind") != owner_kind or owner.get("id") != owner_id:
        raise APIError(400, "missing_datasheet", f"Datasheet manifest does not match its {owner_label}")
    content = files.get(attachment_id)
    if content is None:
        raise APIError(400, "missing_datasheet", f"A declared {owner_label} datasheet was not uploaded")

    archive_path = _safe_archive_path(str(entry.get("originalFileName") or "datasheet.pdf"), attachment_id)
    return attachment_id, archive_path, content


async def prepare_datasheets(
    payload: dict[str, Any],
    manifest_text: str,
    uploaded_files: list[UploadFile],
    max_upload_bytes: int,
) -> list[DatasheetAttachment]:
    manifest = _parse_manifest(manifest_text)
    files = await _read_uploads(uploaded_files, manifest, max_upload_bytes)
    attachments: dict[str, DatasheetAttachment] = {}
    sensor_paths: dict[str, str] = {}

    for test_setup in _test_setups(payload):
        for characteristic in test_setup.get("characteristics", []):
            if not isinstance(characteristic, dict) or characteristic.get("isReplaceable"):
                continue
            resolved = _resolve_attachment(
                characteristic.get("datasheet"), "test_setup_characteristic",
                characteristic.get("id"), "characteristic", manifest, files,
            )
            if not resolved:
                continue
            attachment_id, archive_path, content = resolved
            comments = [
                comment for comment in characteristic.get("comments", [])
                if not isinstance(comment, dict) or comment.get("name") != "datasheet"
            ]
            characteristic["comments"] = [*comments, {"name": "datasheet", "value": f"./{archive_path}"}]
            attachments.setdefault(attachment_id, DatasheetAttachment(archive_path, content))

        for sensor_type in test_setup.get("sensorTypes", []):
            if not isinstance(sensor_type, dict):
                continue
            resolved = _resolve_attachment(
                sensor_type.get("datasheet"), "sensor_type", sensor_type.get("id"),
                "sensor type", manifest, files,
            )
            if not resolved:
                continue
            attachment_id, archive_path, content = resolved
            relative_path = f"./{archive_path}"
            sensor_type["datasheetPath"] = relative_path
            for sensor in test_setup.get("sensors", []):
                if isinstance(sensor, dict) and sensor.get("sensorTypeId") == sensor_type.get("id"):
                    sensor["datasheetPath"] = relative_path
                    if sensor.get("id"):
                        sensor_paths[str(sensor["id"])] = relative_path
            attachments.setdefault(attachment_id, DatasheetAttachment(archive_path, content))

        for component_type in test_setup.get("configurationTypes", []):
            if not isinstance(component_type, dict):
                continue
            resolved = _resolve_attachment(
                component_type.get("datasheet"), "component_type", component_type.get("id"),
                "component type", manifest, files,
            )
            if not resolved:
                continue
            attachment_id, archive_path, content = resolved
            component_type["datasheetPath"] = f"./{archive_path}"
            attachments.setdefault(attachment_id, DatasheetAttachment(archive_path, content))

    for study in payload.get("studies", []):
        if not isinstance(study, dict):
            continue
        for assay in study.get("assay_details", []):
            used_sensor = assay.get("used_sensor") if isinstance(assay, dict) else None
            sensor_id = str(used_sensor.get("id") or "") if isinstance(used_sensor, dict) else ""
            if sensor_id in sensor_paths:
                used_sensor["datasheetPath"] = sensor_paths[sensor_id]

    return list(attachments.values())

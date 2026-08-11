from __future__ import annotations

import json
import re
from dataclasses import dataclass
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import Any, Iterable

from fastapi import UploadFile
from fastapi.concurrency import run_in_threadpool
from pypdf import PdfReader
from pypdf.errors import PdfReadError
from pypdf.generic import ArrayObject, DictionaryObject, IndirectObject

from app.errors import APIError
from app.services.upload_validation import read_upload_limited

MAX_DATASHEETS = 50
MAX_DATASHEET_BYTES = 20 * 1024 * 1024
MAX_TOTAL_DATASHEET_BYTES = 100 * 1024 * 1024
MAX_PDF_PAGES = 1_000
MAX_PDF_STRUCTURES_INSPECTED = 50_000
DISALLOWED_PDF_KEYS = {
    "/AA",
    "/EmbeddedFiles",
    "/JavaScript",
    "/JS",
    "/Launch",
    "/RichMedia",
    "/XFA",
}


@dataclass(frozen=True)
class DatasheetAttachment:
    archive_path: str
    content: bytes


def _safe_archive_path(original_name: str, attachment_id: str) -> str:
    stem = (re.sub(r"[^A-Za-z0-9._-]+", "-", Path(original_name).stem).strip(".-") or "datasheet")[:80]
    safe_id = re.sub(r"[^A-Za-z0-9_-]+", "", attachment_id)[:24] or "file"
    identity = sha256(attachment_id.encode("utf-8")).hexdigest()[:10]
    return f"Datasheets/{stem}-{safe_id}-{identity}.pdf"


def _parse_manifest(manifest_text: str) -> dict[str, dict[str, Any]]:
    try:
        manifest = json.loads(manifest_text or "[]")
    except json.JSONDecodeError as exc:
        raise APIError(400, "invalid_datasheet_manifest", "Datasheet manifest must be valid JSON") from exc
    if not isinstance(manifest, list):
        raise APIError(400, "invalid_datasheet_manifest", "Datasheet manifest must be a list")
    if len(manifest) > MAX_DATASHEETS:
        raise APIError(400, "too_many_datasheets", f"An export can contain at most {MAX_DATASHEETS} datasheets")

    entries: dict[str, dict[str, Any]] = {}
    for entry in manifest:
        if not isinstance(entry, dict) or not entry.get("attachmentId"):
            raise APIError(400, "invalid_datasheet_manifest", "Every datasheet manifest entry requires an attachment ID")
        attachment_id = str(entry["attachmentId"])
        if attachment_id in entries:
            raise APIError(400, "invalid_datasheet_manifest", "Datasheet manifest contains duplicate attachment IDs")
        original_name = str(entry.get("originalFileName") or "")
        if Path(original_name).suffix.lower() != ".pdf":
            raise APIError(400, "invalid_datasheet_manifest", "Every datasheet must have a .pdf filename")
        if not isinstance(entry.get("owner"), dict):
            raise APIError(400, "invalid_datasheet_manifest", "Every datasheet manifest entry requires an owner")
        entries[attachment_id] = entry
    return entries


def _validate_pdf(content: bytes) -> None:
    if not content.startswith(b"%PDF-"):
        raise APIError(400, "invalid_datasheet", "Datasheets must be valid PDF files")

    try:
        reader = PdfReader(BytesIO(content), strict=True, root_object_recovery_limit=1_000)
        if reader.is_encrypted:
            raise APIError(400, "encrypted_datasheet", "Encrypted or password-protected datasheets are not allowed")
        page_count = len(reader.pages)
        if page_count < 1 or page_count > MAX_PDF_PAGES:
            raise APIError(400, "invalid_datasheet", f"Datasheets must contain between 1 and {MAX_PDF_PAGES} pages")

        stack: list[Any] = [reader.root_object]
        visited_indirect: set[tuple[int, int]] = set()
        visited_containers: set[int] = set()
        while stack:
            value = stack.pop()
            if isinstance(value, IndirectObject):
                identity = (value.idnum, value.generation)
                if identity in visited_indirect:
                    continue
                visited_indirect.add(identity)
                if len(visited_indirect) > MAX_PDF_STRUCTURES_INSPECTED:
                    raise APIError(400, "complex_datasheet", "Datasheet structure exceeds the configured safety limit")
                value = value.get_object()

            if not isinstance(value, (DictionaryObject, ArrayObject)):
                continue
            container_identity = id(value)
            if container_identity in visited_containers:
                continue
            visited_containers.add(container_identity)
            if len(visited_containers) > MAX_PDF_STRUCTURES_INSPECTED:
                raise APIError(400, "complex_datasheet", "Datasheet structure exceeds the configured safety limit")

            if isinstance(value, DictionaryObject):
                keys = {str(key) for key in value.keys()}
                blocked = sorted(keys.intersection(DISALLOWED_PDF_KEYS))
                if blocked or str(value.get("/Type")) == "/EmbeddedFile":
                    raise APIError(
                        400,
                        "active_datasheet_content",
                        "Datasheets containing active or embedded content are not allowed",
                        {"pdf_keys": blocked or ["/EmbeddedFile"]},
                    )
                if "/OpenAction" in keys:
                    open_action = value.get("/OpenAction")
                    while isinstance(open_action, IndirectObject):
                        open_action = open_action.get_object()
                    if isinstance(open_action, DictionaryObject):
                        action_type = str(open_action.get("/S") or "")
                        if action_type != "/GoTo":
                            raise APIError(
                                400,
                                "active_datasheet_content",
                                "Datasheets containing executable or external open actions are not allowed",
                                {"pdf_keys": ["/OpenAction"], "action_type": action_type or "unknown"},
                            )
                stack.extend(value.values())
            elif isinstance(value, ArrayObject):
                stack.extend(value)
    except APIError:
        raise
    except (PdfReadError, RecursionError, TypeError, ValueError, OSError) as exc:
        raise APIError(400, "invalid_datasheet", "Uploaded file is not a structurally valid PDF") from exc


async def _read_uploads(
    uploads: Iterable[UploadFile],
    manifest: dict[str, dict[str, Any]],
    max_upload_bytes: int,
) -> dict[str, bytes]:
    uploads = list(uploads)
    if len(uploads) > MAX_DATASHEETS:
        raise APIError(400, "too_many_datasheets", f"An export can contain at most {MAX_DATASHEETS} datasheets")

    files: dict[str, bytes] = {}
    total_bytes = 0
    per_file_limit = min(max_upload_bytes, MAX_DATASHEET_BYTES)
    total_limit = min(max_upload_bytes, MAX_TOTAL_DATASHEET_BYTES)
    for upload in uploads:
        attachment_id = Path(upload.filename or "").stem
        if not attachment_id or attachment_id not in manifest:
            raise APIError(400, "unexpected_datasheet", "Datasheet file does not match the attachment manifest")
        if attachment_id in files:
            raise APIError(400, "unexpected_datasheet", "A datasheet was uploaded more than once")
        if Path(upload.filename or "").suffix.lower() != ".pdf":
            raise APIError(400, "invalid_datasheet", "Datasheet uploads must use a .pdf filename")

        content = await read_upload_limited(
            upload,
            per_file_limit,
            error_code="datasheet_too_large",
            error_message="An uploaded datasheet exceeds the configured size limit",
        )
        total_bytes += len(content)
        if total_bytes > total_limit:
            raise APIError(413, "payload_too_large", "Uploaded datasheets exceed the configured size limit")
        try:
            await run_in_threadpool(_validate_pdf, content)
        except APIError as exc:
            details = exc.details if isinstance(exc.details, dict) else {}
            raise APIError(
                exc.status_code,
                exc.code,
                exc.message,
                {
                    "file_name": str(manifest[attachment_id].get("originalFileName") or upload.filename),
                    "attachment_id": attachment_id,
                    **details,
                },
            ) from exc
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
    used_ids: set[str] = set()

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
            used_ids.add(attachment_id)
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
            used_ids.add(attachment_id)
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
            used_ids.add(attachment_id)
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

    if set(manifest) != used_ids or set(files) != used_ids:
        raise APIError(
            400,
            "unexpected_datasheet",
            "Datasheet manifest contains a file not declared by the exported project",
        )

    return list(attachments.values())

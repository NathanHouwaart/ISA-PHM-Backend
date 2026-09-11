from __future__ import annotations

import json
import re
import warnings
from dataclasses import dataclass
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import Any

from fastapi import UploadFile
from PIL import Image, ImageOps, UnidentifiedImageError

from app.errors import APIError
from app.services.upload_validation import read_upload_limited

MAX_IMAGE_BYTES = 10 * 1024 * 1024
MAX_TOTAL_IMAGE_BYTES = 50 * 1024 * 1024
MAX_IMAGES = 10
MAX_IMAGE_PIXELS = 40_000_000
ALLOWED_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png"}


@dataclass(frozen=True)
class ImageAttachment:
    archive_path: str
    content: bytes


def _parse_manifest(manifest_text: str) -> dict[str, dict[str, Any]]:
    try:
        manifest = json.loads(manifest_text or "[]")
    except json.JSONDecodeError as exc:
        raise APIError(400, "invalid_image_manifest", "Image manifest must be valid JSON") from exc
    if not isinstance(manifest, list):
        raise APIError(400, "invalid_image_manifest", "Image manifest must be a list")
    if len(manifest) > MAX_IMAGES:
        raise APIError(400, "too_many_images", f"A test setup can contain at most {MAX_IMAGES} images")

    entries: dict[str, dict[str, Any]] = {}
    for entry in manifest:
        if not isinstance(entry, dict) or not entry.get("attachmentId"):
            raise APIError(400, "invalid_image_manifest", "Every image manifest entry requires an attachment ID")
        attachment_id = str(entry["attachmentId"])
        if attachment_id in entries:
            raise APIError(400, "invalid_image_manifest", "Image manifest contains duplicate attachment IDs")
        original_name = str(entry.get("originalFileName") or "")
        if Path(original_name).suffix.lower() not in ALLOWED_IMAGE_EXTENSIONS:
            raise APIError(400, "invalid_image_manifest", "Images must have a .png, .jpg, or .jpeg filename")
        if not isinstance(entry.get("owner"), dict):
            raise APIError(400, "invalid_image_manifest", "Every image manifest entry requires an owner")
        entries[attachment_id] = entry
    return entries


def _normalize_image(content: bytes) -> tuple[bytes, str]:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(BytesIO(content)) as opened:
                if opened.format not in {"JPEG", "PNG"}:
                    raise APIError(400, "invalid_image", "Only PNG and JPEG images are allowed")
                width, height = opened.size
                if width <= 0 or height <= 0 or width * height > MAX_IMAGE_PIXELS:
                    raise APIError(400, "invalid_image_dimensions", "Image dimensions exceed the configured limit")
                normalized = ImageOps.exif_transpose(opened)
                normalized.load()
                output = BytesIO()
                if opened.format == "JPEG":
                    if normalized.mode not in {"RGB", "L"}:
                        normalized = normalized.convert("RGB")
                    normalized.save(output, format="JPEG", quality=90, optimize=True)
                    return output.getvalue(), "jpg"
                if normalized.mode not in {"1", "L", "LA", "P", "RGB", "RGBA"}:
                    normalized = normalized.convert("RGBA")
                normalized.save(output, format="PNG", optimize=True)
                return output.getvalue(), "png"
    except APIError:
        raise
    except (
        Image.DecompressionBombError,
        Image.DecompressionBombWarning,
        UnidentifiedImageError,
        OSError,
        ValueError,
    ) as exc:
        raise APIError(400, "invalid_image", "Uploaded file is not a valid PNG or JPEG image") from exc


def _safe_archive_path(original_name: str, attachment_id: str, extension: str) -> str:
    stem = (re.sub(r"[^A-Za-z0-9._-]+", "-", Path(original_name).stem).strip(".-") or "test-setup")[:80]
    safe_id = re.sub(r"[^A-Za-z0-9_-]+", "", attachment_id)[:24] or "image"
    identity = sha256(attachment_id.encode("utf-8")).hexdigest()[:10]
    return f"Images/{stem}-{safe_id}-{identity}.{extension}"


async def prepare_images(
    payload: dict[str, Any],
    manifest_text: str,
    uploaded_files: list[UploadFile],
    max_total_bytes: int = MAX_TOTAL_IMAGE_BYTES,
) -> list[ImageAttachment]:
    manifest = _parse_manifest(manifest_text)
    test_setup = payload.get("test_setup") or {}
    declared_images = test_setup.get("images") or []
    if not isinstance(declared_images, list) or len(declared_images) > MAX_IMAGES:
        raise APIError(400, "too_many_images", f"A test setup can contain at most {MAX_IMAGES} images")

    files: dict[str, bytes] = {}
    total_bytes = 0
    effective_total_limit = min(max(0, max_total_bytes), MAX_TOTAL_IMAGE_BYTES)
    for upload in uploaded_files:
        attachment_id = Path(upload.filename or "").stem
        if not attachment_id or attachment_id not in manifest or attachment_id in files:
            raise APIError(400, "unexpected_image", "Image file does not match the image manifest")
        if Path(upload.filename or "").suffix.lower() not in ALLOWED_IMAGE_EXTENSIONS:
            raise APIError(400, "invalid_image", "Image uploads must use a .png, .jpg, or .jpeg filename")
        content = await read_upload_limited(
            upload,
            min(MAX_IMAGE_BYTES, effective_total_limit),
            error_code="images_too_large",
            error_message="An uploaded image exceeds the configured size limit",
        )
        total_bytes += len(content)
        if total_bytes > effective_total_limit:
            raise APIError(413, "images_too_large", "Uploaded images exceed the configured size limit")
        files[attachment_id] = content

    attachments: list[ImageAttachment] = []
    used_ids: set[str] = set()
    relative_paths: list[str] = []
    normalized_total_bytes = 0
    for image in declared_images:
        if not isinstance(image, dict) or not image.get("attachmentId"):
            raise APIError(400, "invalid_image_manifest", "Every declared image requires an attachment ID")
        attachment_id = str(image["attachmentId"])
        entry = manifest.get(attachment_id)
        owner = entry.get("owner") if entry else None
        if not isinstance(owner, dict) or owner.get("kind") != "test_setup" or owner.get("id") != test_setup.get("id"):
            raise APIError(400, "missing_image", "Image manifest does not match its test setup")
        content = files.get(attachment_id)
        if content is None:
            raise APIError(400, "missing_image", "A declared test setup image was not uploaded")

        normalized, extension = _normalize_image(content)
        normalized_total_bytes += len(normalized)
        if normalized_total_bytes > effective_total_limit:
            raise APIError(413, "images_too_large", "Normalized images exceed the configured size limit")
        archive_path = _safe_archive_path(
            str(entry.get("originalFileName") or image.get("fileName") or "test-setup"),
            attachment_id,
            extension,
        )
        attachments.append(ImageAttachment(archive_path, normalized))
        relative_paths.append(f"./{archive_path}")
        used_ids.add(attachment_id)

    if set(manifest) != used_ids or set(files) != used_ids:
        raise APIError(400, "unexpected_image", "Image manifest contains an image not declared by the test setup")

    if relative_paths:
        test_setup["imagePaths"] = relative_paths
        setup_id = test_setup.get("id")
        for study in payload.get("studies", []):
            used_setup = study.get("used_setup") if isinstance(study, dict) else None
            if isinstance(used_setup, dict) and used_setup.get("id") == setup_id:
                used_setup["imagePaths"] = list(relative_paths)

    return attachments

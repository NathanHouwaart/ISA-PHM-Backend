from __future__ import annotations

from fastapi import UploadFile

from app.errors import APIError


async def read_upload_limited(
    upload: UploadFile,
    max_bytes: int,
    *,
    error_code: str,
    error_message: str,
) -> bytes:
    """Read at most max_bytes while detecting an oversized upload."""
    content = await upload.read(max_bytes + 1)
    if len(content) > max_bytes:
        raise APIError(413, error_code, error_message)
    return content

from __future__ import annotations

import tempfile
import zipfile
from pathlib import Path

from fastapi.responses import FileResponse
from starlette.background import BackgroundTask

from app.services.datasheets import DatasheetAttachment


def create_export_archive(raw_json: str, datasheets: list[DatasheetAttachment]) -> str:
    archive_file = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    archive_path = archive_file.name
    archive_file.close()

    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("ISA-PHM-Out.json", raw_json)
        for datasheet in datasheets:
            archive.writestr(datasheet.archive_path, datasheet.content)
    return archive_path


def export_archive_response(archive_path: str) -> FileResponse:
    return FileResponse(
        archive_path,
        media_type="application/zip",
        filename="ISA-PHM-Out.zip",
        background=BackgroundTask(Path(archive_path).unlink, missing_ok=True),
    )

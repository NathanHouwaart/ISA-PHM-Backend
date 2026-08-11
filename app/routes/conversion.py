from __future__ import annotations

import logging
import time

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.concurrency import run_in_threadpool

from app.config import Settings
from app.services.archive import create_export_archive, export_archive_response
from app.services.conversion import convert_payload, read_and_validate_payload
from app.services.datasheets import prepare_datasheets
from app.services.images import prepare_images

router = APIRouter()
logger = logging.getLogger("isa_phm_backend")


@router.post("/convert")
async def convert_json(
    request: Request,
    file: UploadFile = File(...),
    datasheet_manifest: str = Form("[]"),
    datasheets: list[UploadFile] = File(default=[]),
    image_manifest: str = Form("[]"),
    images: list[UploadFile] = File(default=[]),
):
    started = time.perf_counter()
    settings: Settings = request.app.state.settings
    payload, input_size = await read_and_validate_payload(
        file, request.app.state.payload_schema, request.app.state.schema_path, settings,
    )
    remaining_upload_bytes = settings.max_upload_bytes - input_size
    attachments = await prepare_datasheets(
        payload, datasheet_manifest, datasheets, remaining_upload_bytes,
    )
    remaining_upload_bytes -= sum(len(attachment.content) for attachment in attachments)
    image_attachments = await prepare_images(
        payload, image_manifest, images, remaining_upload_bytes,
    )
    raw_json = await run_in_threadpool(convert_payload, payload, settings)
    archive_path = await run_in_threadpool(
        create_export_archive, raw_json, [*attachments, *image_attachments],
    )

    logger.info(
        "convert_success request_id=%s filename=%s size_bytes=%s duration_ms=%s",
        request.state.request_id, file.filename, input_size,
        int((time.perf_counter() - started) * 1000),
    )
    return export_archive_response(archive_path)

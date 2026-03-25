from __future__ import annotations

import json
import logging
import subprocess
import tempfile
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from uuid import uuid4

import jsonschema
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse

from app.config import Settings
from app.errors import APIError, ConverterFailedError, ConverterNotFoundError, ConverterTimeoutError
from app.semantic_validation import validate_payload_semantics

logger = logging.getLogger("isa_phm_backend")


def configure_logging() -> None:
    if logging.getLogger().handlers:
        return
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _schema_validation_error_details(exc: jsonschema.ValidationError) -> dict[str, Any]:
    if exc.path:
        path = "$" + "".join(
            f"[{segment}]" if isinstance(segment, int) else f".{segment}"
            for segment in exc.path
        )
    else:
        path = "$"
    return {"path": path, "validator": exc.validator, "message": exc.message}


def _error_payload(request_id: str, code: str, message: str, details: Any = None) -> dict[str, Any]:
    return {
        "error": {
            "code": code,
            "message": message,
            "details": details,
            "request_id": request_id,
        }
    }


def _request_id_from_request(request: Request) -> str:
    return getattr(request.state, "request_id", str(uuid4()))


def _json_error_response(
    request: Request,
    status_code: int,
    code: str,
    message: str,
    details: Any = None,
) -> JSONResponse:
    request_id = _request_id_from_request(request)
    response = JSONResponse(
        status_code=status_code,
        content=_error_payload(
            request_id=request_id,
            code=code,
            message=message,
            details=details,
        ),
    )
    response.headers["X-Request-ID"] = request_id
    return response


def _load_schema(settings: Settings) -> tuple[dict[str, Any] | None, Path, list[str]]:
    errors: list[str] = []
    schema_path = settings.strict_schema_path if settings.strict_schema else settings.schema_path

    if not schema_path.exists():
        errors.append(f"Schema file not found: {schema_path}")
        return None, schema_path, errors

    try:
        with schema_path.open("r", encoding="utf-8") as handle:
            schema = json.load(handle)
    except Exception as exc:
        errors.append(f"Failed to load schema {schema_path}: {exc}")
        return None, schema_path, errors

    return schema, schema_path, errors


def _check_converter_readiness(settings: Settings) -> list[str]:
    errors: list[str] = []

    if not settings.converter_script_path.exists():
        errors.append(f"Converter script not found: {settings.converter_script_path}")
        return errors

    try:
        result = subprocess.run(
            [settings.converter_python, "--version"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except FileNotFoundError:
        errors.append(f"Converter interpreter not found: {settings.converter_python}")
        return errors
    except subprocess.TimeoutExpired:
        errors.append(f"Converter interpreter timed out: {settings.converter_python}")
        return errors

    if result.returncode != 0:
        output = (result.stderr or result.stdout or "").strip()
        errors.append(
            f"Converter interpreter check failed (exit {result.returncode}): {output}"
        )

    return errors


def _run_converter_subprocess(settings: Settings, input_path: str, output_path: str) -> None:
    command = [
        settings.converter_python,
        str(settings.converter_script_path),
        input_path,
        output_path,
    ]

    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=settings.converter_timeout_seconds,
        )
    except FileNotFoundError as exc:
        raise ConverterNotFoundError(str(exc)) from exc
    except subprocess.TimeoutExpired as exc:
        raise ConverterTimeoutError(str(exc)) from exc

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        detail = stderr or stdout or "converter process exited with non-zero status"
        raise ConverterFailedError(detail)


def create_app(settings: Settings | None = None) -> FastAPI:
    configure_logging()
    runtime_settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        schema, schema_path, schema_errors = _load_schema(runtime_settings)
        converter_errors = _check_converter_readiness(runtime_settings)

        app.state.payload_schema = schema
        app.state.schema_path = str(schema_path)

        errors = [*schema_errors, *converter_errors]
        app.state.readiness = {
            "ready": len(errors) == 0,
            "schema_loaded": schema is not None,
            "schema_path": str(schema_path),
            "strict_schema": runtime_settings.strict_schema,
            "converter_ready": len(converter_errors) == 0,
            "converter_python": runtime_settings.converter_python,
            "converter_script_path": str(runtime_settings.converter_script_path),
            "errors": errors,
        }

        logger.info(
            "startup_readiness ready=%s schema_path=%s converter_python=%s strict_schema=%s errors=%s",
            app.state.readiness["ready"],
            app.state.readiness["schema_path"],
            app.state.readiness["converter_python"],
            app.state.readiness["strict_schema"],
            app.state.readiness["errors"],
        )
        yield

    app = FastAPI(lifespan=lifespan)
    app.state.settings = runtime_settings

    app.add_middleware(
        CORSMiddleware,
        allow_origins=runtime_settings.cors_allow_origins,
        allow_credentials=True,
        allow_methods=["POST", "GET", "OPTIONS"],
        allow_headers=["Content-Type", "X-Request-ID"],
    )

    @app.middleware("http")
    async def request_context_middleware(request: Request, call_next):
        request_id = request.headers.get("X-Request-ID") or str(uuid4())
        request.state.request_id = request_id

        started = time.perf_counter()
        response = await call_next(request)
        duration_ms = int((time.perf_counter() - started) * 1000)

        response.headers["X-Request-ID"] = request_id
        logger.info(
            "request_complete request_id=%s method=%s path=%s status=%s duration_ms=%s",
            request_id,
            request.method,
            request.url.path,
            response.status_code,
            duration_ms,
        )
        return response

    @app.exception_handler(APIError)
    async def api_error_handler(request: Request, exc: APIError):
        logger.warning(
            "api_error request_id=%s code=%s message=%s",
            _request_id_from_request(request),
            exc.code,
            exc.message,
        )
        return _json_error_response(
            request=request,
            status_code=exc.status_code,
            code=exc.code,
            message=exc.message,
            details=exc.details,
        )

    @app.exception_handler(RequestValidationError)
    async def request_validation_error_handler(request: Request, exc: RequestValidationError):
        return _json_error_response(
            request=request,
            status_code=422,
            code="invalid_request",
            message="Request validation failed",
            details=exc.errors(),
        )

    @app.exception_handler(HTTPException)
    async def http_error_handler(request: Request, exc: HTTPException):
        return _json_error_response(
            request=request,
            status_code=exc.status_code,
            code="http_error",
            message=str(exc.detail),
        )

    @app.exception_handler(Exception)
    async def unexpected_error_handler(request: Request, exc: Exception):
        logger.exception("unhandled_exception request_id=%s", _request_id_from_request(request))
        return _json_error_response(
            request=request,
            status_code=500,
            code="internal_error",
            message="Internal server error",
        )

    @app.get("/")
    async def root() -> dict[str, str]:
        return {"message": "API is running"}

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz(request: Request):
        readiness = request.app.state.readiness
        status_code = 200 if readiness.get("ready") else 503
        return JSONResponse(
            status_code=status_code,
            content={
                "status": "ready" if readiness.get("ready") else "not_ready",
                "readiness": readiness,
            },
        )

    @app.post("/convert")
    async def convert_json(request: Request, file: UploadFile = File(...)):
        request_id = _request_id_from_request(request)
        current_settings: Settings = request.app.state.settings

        if not file.filename or not file.filename.lower().endswith(".json"):
            raise APIError(status_code=400, code="invalid_file_extension", message="Only .json files are allowed")

        allowed_content_types = {"application/json", "text/json"}
        if file.content_type not in allowed_content_types:
            raise APIError(
                status_code=400,
                code="invalid_file_type",
                message="Invalid file type",
                details={"content_type": file.content_type, "allowed": sorted(allowed_content_types)},
            )

        input_path: str | None = None
        output_path: str | None = None
        started = time.perf_counter()

        try:
            raw_bytes = await file.read()
            if len(raw_bytes) > current_settings.max_upload_bytes:
                raise APIError(
                    status_code=413,
                    code="payload_too_large",
                    message=f"Uploaded file exceeds {current_settings.max_upload_mb} MB limit",
                )

            try:
                payload_text = raw_bytes.decode("utf-8-sig")
            except UnicodeDecodeError as exc:
                raise APIError(
                    status_code=400,
                    code="invalid_encoding",
                    message="Payload must be UTF-8 encoded JSON",
                    details={"message": str(exc)},
                ) from exc

            try:
                payload = json.loads(payload_text)
            except json.JSONDecodeError as exc:
                raise APIError(
                    status_code=400,
                    code="invalid_json",
                    message="Invalid JSON payload",
                    details={"line": exc.lineno, "column": exc.colno, "message": exc.msg},
                ) from exc

            schema = request.app.state.payload_schema
            if schema is None:
                raise APIError(
                    status_code=503,
                    code="schema_unavailable",
                    message="Payload schema is not available",
                    details={"schema_path": request.app.state.schema_path},
                )

            try:
                jsonschema.validate(instance=payload, schema=schema)
            except jsonschema.ValidationError as exc:
                raise APIError(
                    status_code=422,
                    code="schema_validation_failed",
                    message="Payload validation failed",
                    details=_schema_validation_error_details(exc),
                ) from exc

            semantic_issues = [issue.as_dict() for issue in validate_payload_semantics(payload)]
            if semantic_issues:
                raise APIError(
                    status_code=422,
                    code="semantic_validation_failed",
                    message="Payload semantic validation failed",
                    details=semantic_issues,
                )

            with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as input_file:
                input_path = input_file.name
                input_file.write(raw_bytes)

            output_file = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
            output_path = output_file.name
            output_file.close()

            try:
                await run_in_threadpool(_run_converter_subprocess, current_settings, input_path, output_path)
            except ConverterNotFoundError as exc:
                raise APIError(
                    status_code=503,
                    code="converter_not_found",
                    message="Converter runtime is not available",
                    details={"converter_python": current_settings.converter_python, "error": str(exc)},
                ) from exc
            except ConverterTimeoutError as exc:
                raise APIError(
                    status_code=504,
                    code="converter_timeout",
                    message="Converter process timed out",
                    details={"timeout_seconds": current_settings.converter_timeout_seconds, "error": str(exc)},
                ) from exc
            except ConverterFailedError as exc:
                raise APIError(
                    status_code=500,
                    code="converter_failed",
                    message="Conversion process failed",
                    details={"error": str(exc)},
                ) from exc

            with open(output_path, "r", encoding="utf-8") as output_handle:
                raw_json = output_handle.read()

            try:
                json.loads(raw_json)
            except json.JSONDecodeError as exc:
                raise APIError(
                    status_code=500,
                    code="invalid_converter_output",
                    message="Converter produced invalid JSON",
                    details={"line": exc.lineno, "column": exc.colno, "message": exc.msg},
                ) from exc

            duration_ms = int((time.perf_counter() - started) * 1000)
            logger.info(
                "convert_success request_id=%s filename=%s size_bytes=%s duration_ms=%s",
                request_id,
                file.filename,
                len(raw_bytes),
                duration_ms,
            )
            return PlainTextResponse(content=raw_json, media_type="application/json")

        finally:
            if input_path and Path(input_path).exists():
                Path(input_path).unlink(missing_ok=True)
            if output_path and Path(output_path).exists():
                Path(output_path).unlink(missing_ok=True)

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8080,
        proxy_headers=True,
    )

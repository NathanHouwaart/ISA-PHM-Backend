from __future__ import annotations

import json
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

from app.config import Settings
from app.errors import APIError
from app.middleware import RequestBodyLimitMiddleware
from app.routes.conversion import router as conversion_router
from app.services.conversion import check_converter_readiness

logger = logging.getLogger("isa_phm_backend")


def configure_logging() -> None:
    if logging.getLogger().handlers:
        return
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


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


def create_app(settings: Settings | None = None) -> FastAPI:
    configure_logging()
    runtime_settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        schema, schema_path, schema_errors = _load_schema(runtime_settings)
        converter_errors = check_converter_readiness(runtime_settings)

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

    app.add_middleware(GZipMiddleware, minimum_size=1024)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=runtime_settings.cors_allow_origins,
        allow_credentials=True,
        allow_methods=["POST", "GET", "OPTIONS"],
        allow_headers=["Content-Type", "X-Request-ID"],
    )
    app.add_middleware(
        RequestBodyLimitMiddleware,
        max_bytes=runtime_settings.max_upload_bytes + 1024 * 1024,
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

    app.include_router(conversion_router)

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

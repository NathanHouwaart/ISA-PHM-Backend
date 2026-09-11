from __future__ import annotations

from typing import Any
from uuid import uuid4

from starlette.responses import JSONResponse


class RequestBodyLimitMiddleware:
    def __init__(self, app: Any, max_bytes: int, path: str = "/convert") -> None:
        self.app = app
        self.max_bytes = max_bytes
        self.path = path

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http" or scope.get("path") != self.path:
            await self.app(scope, receive, send)
            return

        headers = {key.lower(): value for key, value in scope.get("headers", [])}
        request_id = headers.get(b"x-request-id", str(uuid4()).encode("ascii")).decode("ascii", errors="replace")
        content_length = headers.get(b"content-length")
        if content_length:
            try:
                if int(content_length) > self.max_bytes:
                    await self._reject(scope, receive, send, request_id)
                    return
            except ValueError:
                await self._reject(scope, receive, send, request_id)
                return

        received = 0
        response_started = False

        async def limited_receive() -> dict[str, Any]:
            nonlocal received
            message = await receive()
            if message.get("type") == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_bytes:
                    raise _RequestBodyTooLarge
            return message

        async def tracked_send(message: dict[str, Any]) -> None:
            nonlocal response_started
            if message.get("type") == "http.response.start":
                response_started = True
            await send(message)

        try:
            await self.app(scope, limited_receive, tracked_send)
        except _RequestBodyTooLarge:
            if response_started:
                raise
            await self._reject(scope, receive, send, request_id)

    async def _reject(self, scope: dict[str, Any], receive: Any, send: Any, request_id: str) -> None:
        response = JSONResponse(
            status_code=413,
            content={
                "error": {
                    "code": "request_too_large",
                    "message": "Request body exceeds the configured size limit",
                    "details": {"max_bytes": self.max_bytes},
                    "request_id": request_id,
                }
            },
            headers={
                "X-Request-ID": request_id,
                "X-Content-Type-Options": "nosniff",
            },
        )
        await response(scope, receive, send)


class _RequestBodyTooLarge(Exception):
    pass

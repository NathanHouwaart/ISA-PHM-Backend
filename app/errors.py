from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class APIError(Exception):
    status_code: int
    code: str
    message: str
    details: Any = None

    def as_payload(self, request_id: str) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
                "request_id": request_id,
            }
        }


class ConverterNotFoundError(RuntimeError):
    pass


class ConverterTimeoutError(RuntimeError):
    pass


class ConverterFailedError(RuntimeError):
    pass

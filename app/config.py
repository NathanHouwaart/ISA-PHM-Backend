from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List


DEFAULT_CORS_ORIGINS = [
    "https://nathanhouwaart.github.io",
    "http://localhost:5173",
]


@dataclass(frozen=True)
class Settings:
    converter_python: str
    converter_timeout_seconds: int
    max_upload_mb: int
    cors_allow_origins: List[str]
    strict_schema: bool
    schema_path: Path
    strict_schema_path: Path
    converter_script_path: Path

    @property
    def max_upload_bytes(self) -> int:
        return self.max_upload_mb * 1024 * 1024

    @classmethod
    def from_env(cls) -> "Settings":
        base_dir = Path(__file__).resolve().parent
        schema_path = base_dir.parent / "schema" / "IsaPhmInfo.schema.json"
        strict_schema_path = base_dir.parent / "schema" / "IsaPhmInfo.strict.schema.json"
        converter_script_path = base_dir / "web-to-isa-phm.py"

        converter_python = os.getenv("CONVERTER_PYTHON", sys.executable)
        timeout_value = os.getenv("CONVERTER_TIMEOUT_SECONDS", "120")
        max_upload_value = os.getenv("MAX_UPLOAD_MB", "50")
        strict_schema = os.getenv("STRICT_SCHEMA", "false").strip().lower() in {"1", "true", "yes", "on"}

        try:
            converter_timeout_seconds = max(1, int(timeout_value))
        except ValueError:
            converter_timeout_seconds = 120

        try:
            max_upload_mb = max(1, int(max_upload_value))
        except ValueError:
            max_upload_mb = 50

        raw_origins = os.getenv("CORS_ALLOW_ORIGINS", "")
        if raw_origins.strip():
            cors_allow_origins = [origin.strip() for origin in raw_origins.split(",") if origin.strip()]
        else:
            cors_allow_origins = DEFAULT_CORS_ORIGINS.copy()

        return cls(
            converter_python=converter_python,
            converter_timeout_seconds=converter_timeout_seconds,
            max_upload_mb=max_upload_mb,
            cors_allow_origins=cors_allow_origins,
            strict_schema=strict_schema,
            schema_path=schema_path,
            strict_schema_path=strict_schema_path,
            converter_script_path=converter_script_path,
        )

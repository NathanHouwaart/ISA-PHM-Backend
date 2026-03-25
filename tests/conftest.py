from __future__ import annotations

import json
import sys
from dataclasses import replace
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


@pytest.fixture()
def test_settings() -> Settings:
    base = Settings.from_env()
    return replace(
        base,
        converter_python=sys.executable,
        converter_timeout_seconds=20,
        max_upload_mb=5,
        strict_schema=False,
        cors_allow_origins=["http://localhost:5173"],
    )


@pytest.fixture()
def client(test_settings: Settings):
    app = create_app(test_settings)
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture()
def minimal_payload() -> dict:
    payload_path = Path("tests/fixtures/minimal_payload.json")
    return json.loads(payload_path.read_text(encoding="utf-8-sig"))

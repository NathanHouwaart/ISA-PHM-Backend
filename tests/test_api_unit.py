from __future__ import annotations

import copy
import json
from dataclasses import replace

from fastapi.testclient import TestClient

import app.main as main_module
from app.config import Settings
from app.errors import ConverterNotFoundError, ConverterTimeoutError
from app.main import create_app


def _post_payload(client: TestClient, payload: dict, filename: str = "input.json", content_type: str = "application/json"):
    body = json.dumps(payload)
    return client.post("/convert", files={"file": (filename, body, content_type)})


def test_convert_rejects_non_json_extension(client: TestClient, minimal_payload: dict):
    response = _post_payload(client, minimal_payload, filename="input.txt")
    assert response.status_code == 400
    body = response.json()
    assert body["error"]["code"] == "invalid_file_extension"
    assert body["error"]["request_id"]


def test_convert_rejects_invalid_content_type(client: TestClient, minimal_payload: dict):
    response = _post_payload(client, minimal_payload, content_type="text/plain")
    assert response.status_code == 400
    body = response.json()
    assert body["error"]["code"] == "invalid_file_type"


def test_convert_rejects_malformed_json(client: TestClient):
    response = client.post("/convert", files={"file": ("input.json", "{ bad", "application/json")})
    assert response.status_code == 400
    body = response.json()
    assert body["error"]["code"] == "invalid_json"


def test_convert_rejects_semantic_mismatch(client: TestClient, minimal_payload: dict):
    broken = copy.deepcopy(minimal_payload)
    broken["studies"][0]["study_to_study_variable_mapping"][0]["studyVariableId"] = "missing-variable"

    response = _post_payload(client, broken)
    assert response.status_code == 422
    body = response.json()
    assert body["error"]["code"] == "semantic_validation_failed"
    detail_paths = [detail["path"] for detail in body["error"]["details"]]
    assert "$.studies[0].study_to_study_variable_mapping[0].studyVariableId" in detail_paths


def test_convert_reports_converter_not_found(client: TestClient, minimal_payload: dict, monkeypatch):
    def _raise(*_args, **_kwargs):
        raise ConverterNotFoundError("missing converter")

    monkeypatch.setattr(main_module, "_run_converter_subprocess", _raise)
    response = _post_payload(client, minimal_payload)
    assert response.status_code == 503
    body = response.json()
    assert body["error"]["code"] == "converter_not_found"


def test_convert_reports_converter_timeout(client: TestClient, minimal_payload: dict, monkeypatch):
    def _raise(*_args, **_kwargs):
        raise ConverterTimeoutError("timed out")

    monkeypatch.setattr(main_module, "_run_converter_subprocess", _raise)
    response = _post_payload(client, minimal_payload)
    assert response.status_code == 504
    body = response.json()
    assert body["error"]["code"] == "converter_timeout"


def test_healthz_and_readyz(client: TestClient):
    health = client.get("/healthz")
    assert health.status_code == 200
    assert health.json()["status"] == "ok"

    ready = client.get("/readyz")
    assert ready.status_code == 200
    assert ready.json()["status"] == "ready"


def test_readyz_degraded_when_converter_missing(test_settings: Settings):
    degraded_settings = replace(test_settings, converter_python="definitely-not-a-real-python")
    app = create_app(degraded_settings)
    with TestClient(app) as degraded_client:
        response = degraded_client.get("/readyz")
        assert response.status_code == 503
        body = response.json()
        assert body["status"] == "not_ready"
        assert body["readiness"]["converter_ready"] is False

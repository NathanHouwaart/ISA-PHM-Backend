from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile

from fastapi.testclient import TestClient


def _post_payload(client: TestClient, payload: dict):
    return client.post("/convert", files={"file": ("input.json", json.dumps(payload), "application/json")})


def test_convert_integration_returns_parsable_isa_json(client: TestClient, minimal_payload: dict):
    response = _post_payload(client, minimal_payload)
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")

    output = json.loads(response.text)
    assert isinstance(output, dict)
    assert output.get("title") == minimal_payload["title"]
    assert isinstance(output.get("studies"), list)
    assert len(output["studies"]) == 1
    assert len(output["studies"][0].get("assays", [])) >= 1

    with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as handle:
        handle.write(response.text.encode("utf-8"))
        path = handle.name

    try:
        verify = subprocess.run(
            [sys.executable, "tools/verify-isa-json.py", path],
            check=False,
            capture_output=True,
            text=True,
        )
    finally:
        os.unlink(path)

    assert verify.returncode == 0, verify.stdout + "\n" + verify.stderr

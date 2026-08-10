from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import zipfile
from io import BytesIO

from fastapi.testclient import TestClient


def _post_payload(client: TestClient, payload: dict):
    return client.post("/convert", files={"file": ("input.json", json.dumps(payload), "application/json")})


def test_convert_integration_returns_parsable_isa_json(client: TestClient, minimal_payload: dict):
    response = _post_payload(client, minimal_payload)
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/zip")

    with zipfile.ZipFile(BytesIO(response.content)) as archive:
        assert archive.namelist() == ["ISA-PHM-Out.json"]
        output_bytes = archive.read("ISA-PHM-Out.json")
    output = json.loads(output_bytes)
    assert isinstance(output, dict)
    assert output.get("title") == minimal_payload["title"]
    assert isinstance(output.get("studies"), list)
    assert len(output["studies"]) == 1
    assert len(output["studies"][0].get("assays", [])) >= 1

    with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as handle:
        handle.write(output_bytes)
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


def test_convert_packages_nonreplaceable_characteristic_datasheet(client: TestClient, minimal_payload: dict):
    setup = minimal_payload["studies"][0]["used_setup"]
    setup["characteristics"][0].update({
        "id": "characteristic-machine",
        "datasheet": {"attachmentId": "datasheet-1", "fileName": "Rig Manual.pdf"},
    })
    minimal_payload["test_setup"] = setup
    manifest = [{
        "attachmentId": "datasheet-1",
        "originalFileName": "Rig Manual.pdf",
        "owner": {"kind": "test_setup_characteristic", "id": "characteristic-machine"},
    }]
    response = client.post(
        "/convert",
        data={"datasheet_manifest": json.dumps(manifest)},
        files=[
            ("file", ("input.json", json.dumps(minimal_payload), "application/json")),
            ("datasheets", ("datasheet-1.pdf", b"%PDF-1.7\nexample", "application/pdf")),
        ],
    )

    assert response.status_code == 200
    with zipfile.ZipFile(BytesIO(response.content)) as archive:
        datasheet_path = "Datasheets/Rig-Manual-datasheet-1.pdf"
        assert datasheet_path in archive.namelist()
        output = json.loads(archive.read("ISA-PHM-Out.json"))
    serialized_output = json.dumps(output)
    assert '"name": "datasheet"' in serialized_output
    assert f'"value": "./{datasheet_path}"' in serialized_output


def test_convert_packages_sensor_type_datasheet(client: TestClient, minimal_payload: dict):
    setup = minimal_payload["studies"][0]["used_setup"]
    setup["sensorTypes"] = [{
        "id": "sensor-type-1",
        "name": "Accelerometer Type",
        "datasheet": {"attachmentId": "sensor-sheet", "fileName": "Accelerometer.pdf"},
    }]
    setup["sensors"][0]["sensorTypeId"] = "sensor-type-1"
    minimal_payload["test_setup"] = setup
    manifest = [{
        "attachmentId": "sensor-sheet",
        "originalFileName": "Accelerometer.pdf",
        "owner": {"kind": "sensor_type", "id": "sensor-type-1"},
    }]
    response = client.post(
        "/convert",
        data={"datasheet_manifest": json.dumps(manifest)},
        files=[
            ("file", ("input.json", json.dumps(minimal_payload), "application/json")),
            ("datasheets", ("sensor-sheet.pdf", b"%PDF-1.7\nexample", "application/pdf")),
        ],
    )

    assert response.status_code == 200
    with zipfile.ZipFile(BytesIO(response.content)) as archive:
        datasheet_path = "Datasheets/Accelerometer-sensor-sheet.pdf"
        assert datasheet_path in archive.namelist()
        output = json.loads(archive.read("ISA-PHM-Out.json"))
    serialized_output = json.dumps(output)
    assert f'"value": "./{datasheet_path}"' in serialized_output

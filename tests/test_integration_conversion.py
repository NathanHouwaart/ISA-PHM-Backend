from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import zipfile
from io import BytesIO

from fastapi.testclient import TestClient
from PIL import Image
from pypdf import PdfWriter
from pypdf.generic import ArrayObject, NameObject, NumberObject


def _post_payload(client: TestClient, payload: dict):
    return client.post("/convert", files={"file": ("input.json", json.dumps(payload), "application/json")})


def _png_bytes() -> bytes:
    output = BytesIO()
    Image.new("RGB", (8, 6), color=(30, 90, 150)).save(output, format="PNG")
    return output.getvalue()


def _pdf_bytes(*, encrypted: bool = False, javascript: bool = False) -> bytes:
    output = BytesIO()
    writer = PdfWriter()
    writer.add_blank_page(width=72, height=72)
    if javascript:
        writer.add_js("app.alert('not allowed')")
    if encrypted:
        writer.encrypt("secret")
    writer.write(output)
    return output.getvalue()


def _pdf_with_many_primitive_values() -> bytes:
    output = BytesIO()
    writer = PdfWriter()
    writer.add_blank_page(width=72, height=72)
    writer.root_object[NameObject("/BenignData")] = ArrayObject(
        NumberObject(value) for value in range(12_000)
    )
    writer.write(output)
    return output.getvalue()


def _pdf_with_initial_page_destination() -> bytes:
    output = BytesIO()
    writer = PdfWriter()
    page = writer.add_blank_page(width=72, height=72)
    writer.root_object[NameObject("/OpenAction")] = ArrayObject([
        page.indirect_reference,
        NameObject("/Fit"),
    ])
    writer.write(output)
    return output.getvalue()


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
            ("datasheets", ("datasheet-1.pdf", _pdf_bytes(), "application/pdf")),
        ],
    )

    assert response.status_code == 200
    with zipfile.ZipFile(BytesIO(response.content)) as archive:
        datasheet_path = next(name for name in archive.namelist() if name.startswith("Datasheets/Rig-Manual-"))
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
            ("datasheets", ("sensor-sheet.pdf", _pdf_bytes(), "application/pdf")),
        ],
    )

    assert response.status_code == 200
    with zipfile.ZipFile(BytesIO(response.content)) as archive:
        datasheet_path = next(name for name in archive.namelist() if name.startswith("Datasheets/Accelerometer-"))
        assert datasheet_path in archive.namelist()
        output = json.loads(archive.read("ISA-PHM-Out.json"))
    serialized_output = json.dumps(output)
    assert f'"value": "./{datasheet_path}"' in serialized_output


def test_convert_normalizes_and_packages_test_setup_image(client: TestClient, minimal_payload: dict):
    setup = minimal_payload["studies"][0]["used_setup"]
    setup["images"] = [{
        "attachmentId": "setup-image-1",
        "fileName": "Motor Rig.png",
        "mimeType": "image/png",
        "size": len(_png_bytes()),
    }]
    minimal_payload["test_setup"] = setup
    manifest = [{
        "attachmentId": "setup-image-1",
        "originalFileName": "Motor Rig.png",
        "owner": {"kind": "test_setup", "id": setup["id"]},
    }]
    response = client.post(
        "/convert",
        data={"image_manifest": json.dumps(manifest)},
        files=[
            ("file", ("input.json", json.dumps(minimal_payload), "application/json")),
            ("images", ("setup-image-1.png", _png_bytes(), "image/png")),
        ],
    )

    assert response.status_code == 200
    with zipfile.ZipFile(BytesIO(response.content)) as archive:
        image_path = next(name for name in archive.namelist() if name.startswith("Images/Motor-Rig-"))
        assert image_path in archive.namelist()
        with Image.open(BytesIO(archive.read(image_path))) as packaged_image:
            assert packaged_image.format == "PNG"
            assert packaged_image.size == (8, 6)
        output = json.loads(archive.read("ISA-PHM-Out.json"))
    serialized_output = json.dumps(output)
    assert '"name": "image"' in serialized_output
    assert f'"value": "./{image_path}"' in serialized_output


def test_convert_rejects_disguised_test_setup_image(client: TestClient, minimal_payload: dict):
    setup = minimal_payload["studies"][0]["used_setup"]
    setup["images"] = [{"attachmentId": "bad-image", "fileName": "bad.png"}]
    minimal_payload["test_setup"] = setup
    manifest = [{
        "attachmentId": "bad-image",
        "originalFileName": "bad.png",
        "owner": {"kind": "test_setup", "id": setup["id"]},
    }]
    response = client.post(
        "/convert",
        data={"image_manifest": json.dumps(manifest)},
        files=[
            ("file", ("input.json", json.dumps(minimal_payload), "application/json")),
            ("images", ("bad-image.png", b"not really an image", "image/png")),
        ],
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_image"


def _post_characteristic_datasheet(client: TestClient, payload: dict, content: bytes):
    setup = payload["studies"][0]["used_setup"]
    setup["characteristics"][0].update({
        "id": "characteristic-machine",
        "datasheet": {"attachmentId": "datasheet-1", "fileName": "Manual.pdf"},
    })
    payload["test_setup"] = setup
    manifest = [{
        "attachmentId": "datasheet-1",
        "originalFileName": "Manual.pdf",
        "owner": {"kind": "test_setup_characteristic", "id": "characteristic-machine"},
    }]
    return client.post(
        "/convert",
        data={"datasheet_manifest": json.dumps(manifest)},
        files=[
            ("file", ("input.json", json.dumps(payload), "application/json")),
            ("datasheets", ("datasheet-1.pdf", content, "application/pdf")),
        ],
    )


def test_convert_rejects_malformed_pdf_with_valid_header(client: TestClient, minimal_payload: dict):
    response = _post_characteristic_datasheet(client, minimal_payload, b"%PDF-1.7\nnot a PDF")

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_datasheet"


def test_convert_rejects_encrypted_pdf(client: TestClient, minimal_payload: dict):
    response = _post_characteristic_datasheet(client, minimal_payload, _pdf_bytes(encrypted=True))

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "encrypted_datasheet"


def test_convert_rejects_pdf_javascript(client: TestClient, minimal_payload: dict):
    response = _post_characteristic_datasheet(client, minimal_payload, _pdf_bytes(javascript=True))

    assert response.status_code == 400
    error = response.json()["error"]
    assert error["code"] == "active_datasheet_content"
    assert error["details"]["file_name"] == "Manual.pdf"
    assert error["details"]["attachment_id"] == "datasheet-1"


def test_convert_accepts_pdf_with_initial_page_destination(client: TestClient, minimal_payload: dict):
    response = _post_characteristic_datasheet(client, minimal_payload, _pdf_with_initial_page_destination())

    assert response.status_code == 200


def test_convert_accepts_pdf_with_many_benign_primitive_values(client: TestClient, minimal_payload: dict):
    response = _post_characteristic_datasheet(client, minimal_payload, _pdf_with_many_primitive_values())

    assert response.status_code == 200


def test_convert_rejects_undeclared_datasheet(client: TestClient, minimal_payload: dict):
    manifest = [{
        "attachmentId": "datasheet-1",
        "originalFileName": "Manual.pdf",
        "owner": {"kind": "sensor_type", "id": "not-declared"},
    }]
    response = client.post(
        "/convert",
        data={"datasheet_manifest": json.dumps(manifest)},
        files=[
            ("file", ("input.json", json.dumps(minimal_payload), "application/json")),
            ("datasheets", ("datasheet-1.pdf", _pdf_bytes(), "application/pdf")),
        ],
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "unexpected_datasheet"

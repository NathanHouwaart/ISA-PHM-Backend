import json
import sys
import zipfile
from io import BytesIO

import pytest

from app.services.specification_validation import CHECKS, validate_export


def test_unavailable_validator_is_explicit(monkeypatch):
    monkeypatch.setitem(sys.modules, "validator", None)
    report = validate_export('{}')
    assert report['status'] == 'unavailable'
    assert report['conformant'] is None
    assert 'not uploaded' in report['limitations'][-1]


def test_shared_check_excludes_dataset_file_checks():
    pytest.importorskip('validator.api')
    report = validate_export(json.dumps({'studies': []}))
    assert set(report['checks']) == set(CHECKS)
    assert {'layout', 'phm_sensor_csv'} <= set(report['not_selected'])
    assert any('not uploaded' in item for item in report['limitations'])


def test_conversion_archive_contains_report(client, minimal_payload):
    response = client.post('/convert', files={'file': ('input.json', json.dumps(minimal_payload), 'application/json')})
    assert response.status_code == 200
    with zipfile.ZipFile(BytesIO(response.content)) as archive:
        report = json.loads(archive.read('isa-phm-validation-report.json'))
        assert report['status'] in ('pass', 'fail', 'incomplete', 'unavailable')
        assert 'not uploaded' in report['limitations'][-1]

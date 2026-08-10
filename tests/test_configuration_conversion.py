from __future__ import annotations

import copy

from app.converter.entrypoint import create_isa_data


def test_project_scoped_configuration_becomes_sample_characteristics(minimal_payload: dict):
    payload = copy.deepcopy(minimal_payload)
    setup = payload["studies"][0]["used_setup"]
    setup["characteristics"].append(
        {
            "id": "component-bearing",
            "category": "Bearing",
            "description": "Replaceable drive-end bearing",
            "isReplaceable": True,
        }
    )
    setup["configurations"] = [
        {
            "id": "cfg-1",
            "name": "SKF 6205 configuration",
            "testSetupId": setup["id"],
            "typeAssignments": [
                {
                    "replaceableCharacteristicId": "component-bearing",
                    "typeId": "type-skf-6205",
                }
            ],
        }
    ]
    setup["configurationTypes"] = [
        {
            "id": "type-skf-6205",
            "name": "SKF 6205",
            "replaceableCharacteristicId": "component-bearing",
            "characteristics": [
                {"id": "type-detail-1", "name": "Material", "value": "Steel"},
                {"id": "type-detail-2", "name": "Clearance", "value": "C3"},
            ],
        }
    ]

    investigation = create_isa_data(payload)
    sample = investigation.studies[0].samples[0]
    characteristics = {
        characteristic.category.term: characteristic.value
        for characteristic in sample.characteristics
    }

    assert characteristics["Configuration Name"] == "SKF 6205 configuration"
    assert characteristics["Bearing"] == "SKF 6205"
    assert characteristics["Material"] == "Steel"
    assert characteristics["Clearance"] == "C3"
    bearing_characteristic = next(
        characteristic
        for characteristic in sample.characteristics
        if characteristic.category.term == "Bearing"
    )
    assert bearing_characteristic.comments == []
    assert [(comment.name, comment.value) for comment in bearing_characteristic.category.comments] == [
        ("description", "Replaceable drive-end bearing")
    ]


def test_replaceable_components_are_excluded_from_source_and_comments_are_preserved(minimal_payload: dict):
    payload = copy.deepcopy(minimal_payload)
    setup = payload["studies"][0]["used_setup"]
    setup["characteristics"] = [
        {
            "id": "component-bearing",
            "category": "Motor Bearing",
            "value": "",
            "isReplaceable": True,
            "description": "Drive-end bearing",
        },
        {
            "id": "speed-accuracy",
            "category": "Motor Speed Accuracy",
            "value": "5",
            "unit": "RPM",
            "comments": [
                {"name": "Test Comment title", "value": "Test Comment text"},
            ],
            "isReplaceable": False,
        },
    ]

    investigation = create_isa_data(payload)
    source_characteristics = investigation.studies[0].sources[0].characteristics

    assert [characteristic.category.term for characteristic in source_characteristics] == [
        "Motor Speed Accuracy"
    ]
    assert source_characteristics[0].comments == []
    assert [
        (comment.name, comment.value)
        for comment in source_characteristics[0].category.comments
    ] == [("Test Comment title", "Test Comment text")]


def test_samples_use_readable_one_based_run_names(minimal_payload: dict):
    payload = copy.deepcopy(minimal_payload)
    payload["studies"][0]["total_runs"] = 2
    payload["studies"][0]["used_setup"]["configurations"][0]["name"] = "Configuration 1"

    investigation = create_isa_data(payload)

    assert [sample.name for sample in investigation.studies[0].samples] == [
        "Rig A - Configuration 1 - Run 1",
        "Rig A - Configuration 1 - Run 2",
    ]

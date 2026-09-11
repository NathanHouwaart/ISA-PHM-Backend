from __future__ import annotations

import copy

from app.converter.entrypoint import create_isa_data


def test_project_scoped_configuration_becomes_study_source_characteristics(minimal_payload: dict):
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
            "datasheetPath": "./Datasheets/bearing.pdf",
            "characteristics": [
                {"id": "type-detail-1", "name": "Material", "value": "Steel"},
                {"id": "type-detail-2", "name": "Clearance", "value": "C3"},
            ],
        }
    ]

    investigation = create_isa_data(payload)
    source = investigation.studies[0].sources[0]
    characteristics = {
        characteristic.category.term: characteristic.value
        for characteristic in source.characteristics
    }

    assert characteristics["Configuration Name"] == "SKF 6205 configuration"
    assert characteristics["Bearing"] == "SKF 6205"
    assert "Material" not in characteristics
    assert "Clearance" not in characteristics
    bearing_characteristic = next(
        characteristic
        for characteristic in source.characteristics
        if characteristic.category.term == "Bearing"
    )
    assert bearing_characteristic.category.comments == []
    assert [(comment.name, comment.value) for comment in bearing_characteristic.comments] == [
        ("description", "Replaceable drive-end bearing"),
        ("datasheet", "./Datasheets/bearing.pdf"),
        ("Material", "Steel"),
        ("Clearance", "C3"),
    ]


def test_replaceable_components_are_stored_once_on_source_and_comments_are_preserved(minimal_payload: dict):
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
        "Motor Speed Accuracy", "Configuration Name", "Replaceable Component", "Bearing type"
    ]
    assert source_characteristics[0].category.comments == []
    assert [
        (comment.name, comment.value)
        for comment in source_characteristics[0].comments
    ] == [("Test Comment title", "Test Comment text")]


def test_component_instances_become_study_source_characteristics_with_datasheet(minimal_payload: dict):
    payload = copy.deepcopy(minimal_payload)
    setup = payload["studies"][0]["used_setup"]
    setup["characteristics"] = [{
        "id": "component-bearing", "category": "Bearing", "description": "Drive-end bearing", "isReplaceable": True,
    }]
    setup["configurations"] = [{
        "id": "bearing-1", "replaceableCharacteristicId": "component-bearing", "componentId": "1.1", "typeId": "type-6205",
    }]
    setup["configurationTypes"] = [{
        "id": "type-6205", "name": "SKF 6205", "replaceableCharacteristicId": "component-bearing", "datasheetPath": "./Datasheets/skf-6205.pdf",
        "characteristics": [{"id": "material", "name": "Material", "value": "Steel"}],
    }]
    payload["studies"][0]["componentAssignments"] = [{
        "replaceableCharacteristicId": "component-bearing", "componentInstanceId": "bearing-1",
    }]
    payload["studies"][0].pop("configurationId", None)

    investigation = create_isa_data(payload)
    bearing = next(characteristic for characteristic in investigation.studies[0].sources[0].characteristics if characteristic.category.term == "Bearing")
    assert bearing.value == "SKF 6205"
    assert bearing.category.comments == []
    assert [(comment.name, comment.value) for comment in bearing.comments] == [
        ("description", "Drive-end bearing"),
        ("component ID", "1.1"),
        ("datasheet", "./Datasheets/skf-6205.pdf"),
        ("Material", "Steel"),
    ]
    assert "Material" not in {
        category.term for category in investigation.studies[0].characteristic_categories
    }


def test_replaceable_component_is_not_copied_to_each_run_sample(minimal_payload: dict):
    payload = copy.deepcopy(minimal_payload)
    payload["studies"][0]["total_runs"] = 3
    setup = payload["studies"][0]["used_setup"]
    setup["characteristics"].append({
        "id": "component-bearing", "category": "Bearing", "description": "Drive-end bearing", "isReplaceable": True,
    })
    setup["configurations"] = [{
        "id": "bearing-1", "replaceableCharacteristicId": "component-bearing", "componentId": "1.1", "typeId": "type-6205",
    }]
    setup["configurationTypes"] = [{
        "id": "type-6205", "name": "SKF 6205", "replaceableCharacteristicId": "component-bearing",
    }]
    payload["studies"][0]["componentAssignments"] = [{
        "replaceableCharacteristicId": "component-bearing", "componentInstanceId": "bearing-1",
    }]
    payload["studies"][0].pop("configurationId", None)

    investigation = create_isa_data(payload)
    study = investigation.studies[0]

    assert [characteristic.category.term for characteristic in study.sources[0].characteristics].count("Bearing") == 1
    assert all(not sample.characteristics for sample in study.samples)
    assert all(study.sources[0] in sample.derives_from for sample in study.samples)


def test_study_factor_does_not_export_arbitrary_range_or_unit_comments(minimal_payload: dict):
    payload = copy.deepcopy(minimal_payload)
    payload["study_variables"][0].update({"unit": "N", "min": "0", "max": "2000", "step": "100"})

    investigation = create_isa_data(payload)
    factor = investigation.studies[0].factors[0]

    assert [(comment.name, comment.value) for comment in factor.comments] == [
        ("description", "Applied load"),
    ]
    assert investigation.studies[0].samples[0].factor_values[0].unit.term == "N"


def test_samples_use_readable_one_based_run_names(minimal_payload: dict):
    payload = copy.deepcopy(minimal_payload)
    payload["studies"][0]["total_runs"] = 2
    payload["studies"][0]["used_setup"]["configurations"][0]["name"] = "Configuration 1"

    investigation = create_isa_data(payload)

    assert [sample.name for sample in investigation.studies[0].samples] == [
        "Rig A - Configuration 1 - Run 1",
        "Rig A - Configuration 1 - Run 2",
    ]


def test_test_setup_image_paths_become_source_comments(minimal_payload: dict):
    payload = copy.deepcopy(minimal_payload)
    payload["studies"][0]["used_setup"]["imagePaths"] = [
        "./Images/motor-rig.png",
        "./Images/detail.jpg",
    ]

    investigation = create_isa_data(payload)

    assert [
        (comment.name, comment.value)
        for comment in investigation.studies[0].sources[0].comments
        if comment.name == "image"
    ] == [
        ("image", "./Images/motor-rig.png"),
        ("image", "./Images/detail.jpg"),
    ]

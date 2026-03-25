from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from uuid import uuid4

from isatools.model import (
    Characteristic,
    Comment,
    Investigation,
    OntologyAnnotation,
    Person,
    Process,
    Protocol,
    Publication,
    Sample,
    Source,
    Study,
    batch_create_materials,
)

from .assay_graph import append_assays_to_study
from .context import ConversionContext
from .factor_mapping import add_study_factors, assign_factor_values
from .normalization import as_comment_value
from .protocol_mapping import (
    build_measurement_parameters_for_sensor,
    build_processing_parameters_for_sensor,
)


def create_isa_data(
    isa_phm_info: Dict[str, Any],
    output_path: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> Investigation:
    logger = logger or logging.getLogger("isa_phm_converter")
    context = ConversionContext()

    investigation = Investigation()
    investigation.filename = output_path if output_path else "isa_phm.json"
    investigation.identifier = str(uuid4())
    investigation.title = isa_phm_info.get("title", "")
    investigation.description = isa_phm_info.get("description", "")
    investigation.submission_date = isa_phm_info.get("submission_date", "")
    investigation.public_release_date = isa_phm_info.get("public_release_date", "")
    investigation.comments.append(Comment(name="ud_identifier", value=as_comment_value(isa_phm_info.get("identifier", ""))))
    investigation.comments.append(Comment(name="experiment_type", value=as_comment_value(isa_phm_info.get("experiment_type", ""))))
    investigation.comments.append(Comment(name="license", value=as_comment_value(isa_phm_info.get("license", ""))))

    contacts: List[Dict[str, Any]] = isa_phm_info.get("contacts", [])
    for contact in contacts:
        person = Person()
        person.first_name = contact.get("firstName", "")
        person.mid_initials = contact.get("midInitials", "")
        person.last_name = contact.get("lastName", "")
        person.email = contact.get("email", "")
        person.phone = contact.get("phone", "")
        person.fax = contact.get("fax", "")
        person.address = contact.get("address", "")
        person.affiliation = "; ".join(contact.get("affiliations", []))
        person.roles.extend(
            [
                role
                for role in (context.get_or_create_role(role_name) for role_name in contact.get("roles", []))
                if role is not None
            ]
        )
        person.comments.append(Comment(name="orcid", value=as_comment_value(contact.get("orcid", ""))))
        person.comments.append(Comment(name="author_id", value=as_comment_value(contact.get("id", ""))))
        investigation.contacts.append(person)

    publications: List[Dict[str, Any]] = isa_phm_info.get("publications", [])
    for publication in publications:
        publication_obj = Publication()
        publication_obj.title = publication.get("title", "")
        publication_obj.author_list = "; ".join(["#" + author for author in publication.get("contactList", [])])
        publication_obj.status = OntologyAnnotation(publication.get("publicationStatus", "unknown"))
        publication_obj.doi = publication.get("doi", "")
        publication_obj.comments.append(
            Comment(
                name="Corresponding author ID",
                value=as_comment_value(publication.get("correspondingContactId", "")),
            )
        )
        investigation.publications.append(publication_obj)

    global_measurement_protocol_defs = (
        {protocol.get("id"): protocol for protocol in isa_phm_info.get("measurement_protocols", [])}
        if isa_phm_info.get("measurement_protocols")
        else {}
    )
    global_processing_defs = (
        {protocol.get("id"): protocol for protocol in isa_phm_info.get("processing_protocols", [])}
        if isa_phm_info.get("processing_protocols")
        else {}
    )

    studies: List[Dict[str, Any]] = isa_phm_info.get("studies", [])
    for study_index, study in enumerate(studies, start=1):
        study_obj = Study()
        study_obj.filename = f"s{study_index:02d}_.txt"
        study_obj.identifier = study.get("id", "")
        study_obj.title = study.get("name", "")
        study_obj.description = study.get("description", "")
        study_obj.submission_date = study.get("submissionDate", "")
        study_obj.public_release_date = study.get("publicationDate", "")
        study_obj.publications.extend(investigation.publications)
        study_obj.contacts.extend(investigation.contacts)
        study_obj.design_descriptors.append(OntologyAnnotation(study.get("experimentType", "Diagnostics")))

        study_total_runs = study.get("total_runs", 1)
        study_obj.comments.append(Comment(name="total_runs", value=as_comment_value(study_total_runs)))

        test_setup = study.get("used_setup", {})
        measurement_protocol_variants = test_setup.get("measurementProtocols", []) or []
        processing_protocol_variants = test_setup.get("processingProtocols", []) or []

        selected_measurement_protocol_id = (
            study.get("selectedMeasurementProtocolId")
            or study.get("selected_measurement_protocol_id")
            or ""
        )
        selected_processing_protocol_id = (
            study.get("selectedProcessingProtocolId")
            or study.get("selected_processing_protocol_id")
            or ""
        )

        selected_measurement_protocol = next(
            (
                protocol
                for protocol in measurement_protocol_variants
                if protocol.get("id") == selected_measurement_protocol_id
            ),
            None,
        )
        selected_processing_protocol = next(
            (
                protocol
                for protocol in processing_protocol_variants
                if protocol.get("id") == selected_processing_protocol_id
            ),
            None,
        )

        selected_measurement_parameters = (
            selected_measurement_protocol.get("parameters", [])
            if selected_measurement_protocol
            else ([] if measurement_protocol_variants else isa_phm_info.get("measurement_protocols", []))
        )
        selected_processing_parameters = (
            selected_processing_protocol.get("parameters", [])
            if selected_processing_protocol
            else ([] if processing_protocol_variants else isa_phm_info.get("processing_protocols", []))
        )

        measurement_protocol_defs = (
            {parameter.get("id"): parameter for parameter in selected_measurement_parameters if parameter.get("id")}
            if selected_measurement_parameters
            else (global_measurement_protocol_defs if not measurement_protocol_variants else {})
        )

        processing_defs = (
            {parameter.get("id"): parameter for parameter in selected_processing_parameters if parameter.get("id")}
            if selected_processing_parameters
            else (global_processing_defs if not processing_protocol_variants else {})
        )

        experiment_prep_protocol = Protocol(
            name=test_setup.get("experimentPreparationProtocolName", "Experiment Preparation")
        )
        experiment_prep_protocol.protocol_type = OntologyAnnotation("Experiment Preparation Protocol")
        study_obj.protocols.append(experiment_prep_protocol)

        for sensor in test_setup.get("sensors", []):
            sensor_id = (
                sensor.get("id", "")
                or sensor.get("name", "")
                or sensor.get("sensorLocation", "")
                or f"sensor_{len(study_obj.protocols)}"
            )
            measurement_type = sensor.get("measurementType", "") or "Unknown"

            measurement_protocol = Protocol(
                name=f"{measurement_type} measurement ({sensor_id})",
                description=sensor.get("description", "no description provided"),
            )
            measurement_protocol.protocol_type = OntologyAnnotation("Measurement Protocol")
            measurement_protocol.comments.append(Comment(name="Sensor id", value=as_comment_value(sensor.get("id", ""))))
            measurement_protocol.comments.append(
                Comment(
                    name="selected_measurement_protocol_id",
                    value=as_comment_value(selected_measurement_protocol_id),
                )
            )
            measurement_protocol.parameters.extend(
                build_measurement_parameters_for_sensor(
                    study,
                    sensor,
                    measurement_protocol_defs,
                    selected_protocol_id=selected_measurement_protocol_id,
                )
            )
            study_obj.protocols.append(measurement_protocol)

        for sensor in test_setup.get("sensors", []):
            sensor_id = (
                sensor.get("id", "")
                or sensor.get("name", "")
                or sensor.get("sensorLocation", "")
                or f"sensor_{len(study_obj.protocols)}"
            )
            measurement_type = sensor.get("measurementType", "") or "Unknown"

            processing_protocol = Protocol(
                name=f"{measurement_type} processing ({sensor_id})",
                description=sensor.get("description", ""),
            )
            processing_protocol.protocol_type = OntologyAnnotation("Processing Protocol")
            processing_protocol.comments.append(Comment(name="Sensor id", value=as_comment_value(sensor.get("id", ""))))
            processing_protocol.comments.append(
                Comment(
                    name="selected_processing_protocol_id",
                    value=as_comment_value(selected_processing_protocol_id),
                )
            )
            processing_protocol.parameters.extend(
                build_processing_parameters_for_sensor(
                    study,
                    sensor,
                    processing_defs,
                    selected_protocol_id=selected_processing_protocol_id,
                )
            )
            study_obj.protocols.append(processing_protocol)

        source = Source(name=test_setup.get("name", "Test Setup"))
        source.comments.append(Comment(name="description", value=as_comment_value(test_setup.get("description", ""))))
        for characteristic in test_setup.get("characteristics", []):
            category = OntologyAnnotation(term=characteristic.get("category", "unknown"))
            study_obj.characteristic_categories.append(category)

            characteristic_obj = Characteristic()
            characteristic_obj.category = category
            characteristic_obj.value = characteristic.get("value", "")
            characteristic_obj.unit = context.add_unit_to_study(study_obj, characteristic.get("unit", ""))
            source.characteristics.append(characteristic_obj)

        study_obj.sources.append(source)

        configuration_id = study.get("configurationId")
        active_config = next(
            (configuration for configuration in test_setup.get("configurations", []) if configuration.get("id") == configuration_id),
            None,
        )

        if active_config:
            sample_name = f"{test_setup.get('name', 'Test Setup')} - {active_config.get('name', 'Configuration')}"
        else:
            sample_name = f"{test_setup.get('name', 'Test Setup')} - No Configuration"
        dummy_sample = Sample(name=sample_name, derives_from=[source])

        if active_config:
            for config_category, config_value in [
                ("Configuration Name", active_config.get("name", "")),
                ("Replaceable Component", active_config.get("replaceableComponentId", "")),
            ]:
                annotation = OntologyAnnotation(term=config_category)
                study_obj.characteristic_categories.append(annotation)
                dummy_sample.characteristics.append(Characteristic(category=annotation, value=config_value))

            for detail in active_config.get("details", []):
                detail_category = OntologyAnnotation(term=detail.get("name", "Configuration Detail"))
                study_obj.characteristic_categories.append(detail_category)
                dummy_sample.characteristics.append(
                    Characteristic(category=detail_category, value=detail.get("value", ""))
                )

        study_variables = isa_phm_info.get("study_variables", [])
        add_study_factors(study_obj, study_variables)

        study_obj.samples = batch_create_materials(dummy_sample, n=study_total_runs)
        for sample in study_obj.samples:
            sample.id = ""

        assign_factor_values(
            study_obj=study_obj,
            study_payload=study,
            study_variables=study_variables,
            study_total_runs=study_total_runs,
            add_unit_to_study=context.add_unit_to_study,
            logger=logger,
        )

        experiment_preparation_process = Process(executes_protocol=experiment_prep_protocol)
        experiment_preparation_process.inputs.append(source)
        for sample in study_obj.samples:
            experiment_preparation_process.outputs.append(sample)
        study_obj.process_sequence.append(experiment_preparation_process)

        append_assays_to_study(
            study_obj=study_obj,
            study_payload=study,
            dummy_sample=dummy_sample,
            selected_measurement_protocol_id=selected_measurement_protocol_id,
            selected_processing_protocol_id=selected_processing_protocol_id,
            measurement_protocol_variants=measurement_protocol_variants,
            processing_protocol_variants=processing_protocol_variants,
            measurement_protocol_defs=measurement_protocol_defs,
            processing_defs=processing_defs,
            add_unit_to_study=context.add_unit_to_study,
            logger=logger,
        )

        investigation.studies.append(study_obj)

    return investigation

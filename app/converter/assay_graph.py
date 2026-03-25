from __future__ import annotations

from logging import Logger
from typing import Any, Dict, List

from isatools.model import (
    Assay,
    Comment,
    DataFile,
    OntologyAnnotation,
    ParameterValue,
    Process,
    Protocol,
    ProtocolParameter,
    Sample,
    Study,
    plink,
)

from .normalization import normalize_unit, parse_numeric_if_possible
from .protocol_mapping import parse_protocol_entry


def append_assays_to_study(
    study_obj: Study,
    study_payload: Dict[str, Any],
    dummy_sample: Sample,
    selected_measurement_protocol_id: str,
    selected_processing_protocol_id: str,
    measurement_protocol_variants: List[Dict[str, Any]],
    processing_protocol_variants: List[Dict[str, Any]],
    measurement_protocol_defs: Dict[str, Dict[str, Any]],
    processing_defs: Dict[str, Dict[str, Any]],
    add_unit_to_study,
    logger: Logger,
) -> None:
    for assay in study_payload.get("assay_details", []):
        assay_obj = Assay(filename=assay.get("assay_file_name", "unknown"))

        assay_sensor = assay.get("used_sensor", {})
        assay_measurement_type = assay_sensor.get("measurementType", "") or "Unknown"

        assay_obj.measurement_type = OntologyAnnotation(assay_measurement_type)
        assay_obj.technology_type = OntologyAnnotation(assay_sensor.get("technologyType", "unknown"))
        assay_obj.technology_platform = assay_sensor.get("technologyPlatform", "unknown")
        sensor_alias_value = assay_sensor.get("alias", "") or assay_sensor.get("id", "")
        if sensor_alias_value:
            assay_obj.comments.append(Comment(name="sensor alias", value=sensor_alias_value))

        for sample in study_obj.samples:
            assay_obj.samples.append(sample)

        runs = assay.get("runs", [])
        run_has_raw: List[bool] = []
        run_has_processed: List[bool] = []
        run_raw_df_index: Dict[int, int] = {}
        run_processed_df_index: Dict[int, int] = {}

        for run_index, run in enumerate(runs):
            raw_name = (run.get("raw_file_name") or "").strip()
            proc_name = (run.get("processed_file_name") or "").strip()
            has_raw = bool(raw_name)
            has_proc = bool(proc_name)
            run_has_raw.append(has_raw)
            run_has_processed.append(has_proc)

            if has_raw:
                run_raw_df_index[run_index] = len(assay_obj.data_files)
                assay_obj.data_files.append(
                    DataFile(filename=raw_name, label="Raw Data File", generated_from=dummy_sample)
                )

            if has_proc:
                run_processed_df_index[run_index] = len(assay_obj.data_files)
                assay_obj.data_files.append(
                    DataFile(filename=proc_name, label="Derived Data File", generated_from=dummy_sample)
                )

        assay_sensor_id = assay_sensor.get("id", "") or assay_sensor.get("name", "") or assay_sensor.get("sensorLocation", "")
        sensor_alias = assay_sensor.get("alias", "") or assay_sensor.get("id", "sensor")
        expected_measurement_name = (
            f"{assay_measurement_type} measurement ({assay_sensor_id})"
            if assay_sensor_id
            else f"{assay_measurement_type} measurement"
        )
        expected_processing_name = (
            f"{assay_measurement_type} processing ({assay_sensor_id})"
            if assay_sensor_id
            else f"{assay_measurement_type} processing"
        )

        measurement_params: List[ParameterValue] = []
        processing_params: List[ParameterValue] = []
        measurement_protocol_obj: Protocol | None = None
        processing_protocol_obj: Protocol | None = None

        for protocol in study_obj.protocols:
            is_measurement = (
                protocol.name == expected_measurement_name
                or (not assay_sensor_id and protocol.name.startswith(f"{assay_measurement_type} measurement"))
            )
            is_processing = (
                protocol.name == expected_processing_name
                or (not assay_sensor_id and protocol.name.startswith(f"{assay_measurement_type} processing"))
            )

            if is_measurement:
                measurement_protocol_obj = protocol
                use_measurement_entries = bool(selected_measurement_protocol_id) or not measurement_protocol_variants
                if not use_measurement_entries:
                    continue

                for entry in assay.get("measurement_protocols", []):
                    parsed = parse_protocol_entry(
                        entry,
                        assay_sensor_id,
                        measurement_protocol_defs,
                        expected_protocol_id=selected_measurement_protocol_id,
                    )
                    if not parsed:
                        continue

                    target_id, parameter_name, raw_value, raw_unit = parsed
                    matching_param = next(
                        (
                            p
                            for p in protocol.parameters
                            if getattr(p.parameter_name, "term", None) in (parameter_name, target_id)
                        ),
                        None,
                    )
                    category = matching_param or ProtocolParameter(parameter_name=OntologyAnnotation(parameter_name))
                    parsed_value, is_numeric = parse_numeric_if_possible(raw_value)
                    clean_unit = normalize_unit(raw_unit)
                    unit = add_unit_to_study(study_obj, clean_unit) if clean_unit and is_numeric else None
                    if clean_unit and not is_numeric:
                        logger.warning(
                            "Skipping unit for non-numeric measurement value",
                            extra={"parameter": parameter_name, "value": raw_value, "unit": clean_unit},
                        )

                    measurement_params.append(ParameterValue(category=category, value=parsed_value, unit=unit))

            if is_processing:
                processing_protocol_obj = protocol
                use_processing_entries = bool(selected_processing_protocol_id) or not processing_protocol_variants
                if not use_processing_entries:
                    continue

                for entry in assay.get("processing_protocols", []):
                    parsed = parse_protocol_entry(
                        entry,
                        assay_sensor_id,
                        processing_defs,
                        expected_protocol_id=selected_processing_protocol_id,
                    )
                    if not parsed:
                        continue

                    target_id, parameter_name, raw_value, raw_unit = parsed
                    matching_param = next(
                        (
                            p
                            for p in protocol.parameters
                            if getattr(p.parameter_name, "term", None) in (parameter_name, target_id)
                        ),
                        None,
                    )
                    category = matching_param or ProtocolParameter(parameter_name=OntologyAnnotation(parameter_name))
                    parsed_value, is_numeric = parse_numeric_if_possible(raw_value)
                    clean_unit = normalize_unit(raw_unit)
                    unit = add_unit_to_study(study_obj, clean_unit) if clean_unit and is_numeric else None
                    if clean_unit and not is_numeric:
                        logger.warning(
                            "Skipping unit for non-numeric processing value",
                            extra={"parameter": parameter_name, "value": raw_value, "unit": clean_unit},
                        )

                    processing_params.append(ParameterValue(category=category, value=parsed_value, unit=unit))

        for index, run in enumerate(runs):
            run_number = run.get("run_number", index + 1)
            has_raw = run_has_raw[index]
            has_proc = run_has_processed[index]

            if not has_raw and not has_proc:
                logger.warning("Skipping run without output files", extra={"run_number": run_number})
                continue

            raw_df = assay_obj.data_files[run_raw_df_index[index]] if has_raw else None
            proc_df = assay_obj.data_files[run_processed_df_index[index]] if has_proc else None

            measurement_process = None
            processing_process = None

            if measurement_protocol_obj:
                measurement_output = raw_df if has_raw else proc_df
                measurement_process = Process(
                    executes_protocol=measurement_protocol_obj,
                    parameter_values=measurement_params,
                )
                measurement_process.name = f"{sensor_alias}_run_{run_number}_measurement"
                measurement_process.inputs.append(study_obj.samples[index])
                measurement_process.outputs.append(measurement_output)
                assay_obj.process_sequence.append(measurement_process)

            if processing_protocol_obj and has_proc:
                processing_input = raw_df if has_raw else proc_df
                processing_process = Process(
                    executes_protocol=processing_protocol_obj,
                    parameter_values=processing_params,
                )
                processing_process.name = f"{sensor_alias}_run_{run_number}_processing"
                processing_process.inputs.append(processing_input)
                processing_process.outputs.append(proc_df)
                assay_obj.process_sequence.append(processing_process)

            if measurement_process and processing_process:
                plink(measurement_process, processing_process)

        study_obj.assays.append(assay_obj)

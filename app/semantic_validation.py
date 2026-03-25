from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


@dataclass(frozen=True)
class SemanticIssue:
    path: str
    code: str
    message: str

    def as_dict(self) -> dict[str, str]:
        return {"path": self.path, "code": self.code, "message": self.message}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _as_object(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _collect_protocol_by_id(protocols: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for protocol in protocols:
        protocol_id = protocol.get("id")
        if isinstance(protocol_id, str) and protocol_id:
            result[protocol_id] = protocol
    return result


def _collect_parameter_ids(protocol: dict[str, Any] | None) -> set[str]:
    if not protocol:
        return set()
    param_ids: set[str] = set()
    for parameter in _as_list(protocol.get("parameters")):
        parameter_id = _as_object(parameter).get("id")
        if isinstance(parameter_id, str) and parameter_id:
            param_ids.add(parameter_id)
    return param_ids


def validate_payload_semantics(payload: dict[str, Any]) -> list[SemanticIssue]:
    issues: list[SemanticIssue] = []

    def add(path: str, code: str, message: str) -> None:
        issues.append(SemanticIssue(path=path, code=code, message=message))

    top_study_variables = _collect_protocol_by_id(_as_list(payload.get("study_variables")))

    studies = _as_list(payload.get("studies"))
    for study_index, study_value in enumerate(studies):
        study = _as_object(study_value)
        study_path = f"$.studies[{study_index}]"
        total_runs = study.get("total_runs")
        if not isinstance(total_runs, int) or total_runs < 1:
            add(
                f"{study_path}.total_runs",
                "invalid_total_runs",
                "total_runs must be an integer >= 1",
            )
            total_runs = None

        used_setup = _as_object(study.get("used_setup"))
        sensors = _as_list(used_setup.get("sensors"))
        sensor_ids = {
            str(sensor.get("id"))
            for sensor in sensors
            if isinstance(sensor, dict) and sensor.get("id") not in (None, "")
        }

        measurement_protocols = _as_list(used_setup.get("measurementProtocols"))
        processing_protocols = _as_list(used_setup.get("processingProtocols"))
        if not measurement_protocols:
            measurement_protocols = _as_list(payload.get("measurement_protocols"))
        if not processing_protocols:
            processing_protocols = _as_list(payload.get("processing_protocols"))

        measurement_protocol_by_id = _collect_protocol_by_id(measurement_protocols)
        processing_protocol_by_id = _collect_protocol_by_id(processing_protocols)

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

        if selected_measurement_protocol_id and selected_measurement_protocol_id not in measurement_protocol_by_id:
            add(
                f"{study_path}.selectedMeasurementProtocolId",
                "unknown_measurement_protocol",
                f"Unknown measurement protocol id '{selected_measurement_protocol_id}'",
            )

        if selected_processing_protocol_id and selected_processing_protocol_id not in processing_protocol_by_id:
            add(
                f"{study_path}.selectedProcessingProtocolId",
                "unknown_processing_protocol",
                f"Unknown processing protocol id '{selected_processing_protocol_id}'",
            )

        study_variable_mappings = _as_list(study.get("study_to_study_variable_mapping"))
        for mapping_index, mapping_value in enumerate(study_variable_mappings):
            mapping = _as_object(mapping_value)
            mapping_path = f"{study_path}.study_to_study_variable_mapping[{mapping_index}]"
            variable_id = mapping.get("studyVariableId")
            if isinstance(variable_id, str) and variable_id and variable_id not in top_study_variables:
                add(
                    f"{mapping_path}.studyVariableId",
                    "unknown_study_variable",
                    f"Unknown study variable id '{variable_id}'",
                )

            run_number = mapping.get("runNumber")
            if run_number is not None:
                if not isinstance(run_number, int):
                    add(
                        f"{mapping_path}.runNumber",
                        "invalid_run_number",
                        "runNumber must be an integer",
                    )
                elif total_runs is not None and (run_number < 1 or run_number > total_runs):
                    add(
                        f"{mapping_path}.runNumber",
                        "run_number_out_of_range",
                        f"runNumber {run_number} is outside 1..{total_runs}",
                    )

        assay_details = _as_list(study.get("assay_details"))
        processed_outputs_present = False
        if measurement_protocol_by_id and assay_details and not selected_measurement_protocol_id:
            add(
                f"{study_path}.selectedMeasurementProtocolId",
                "missing_measurement_protocol_selection",
                "selectedMeasurementProtocolId is required when measurement protocol variants are present",
            )

        selected_measurement_protocol = measurement_protocol_by_id.get(selected_measurement_protocol_id)
        selected_processing_protocol = processing_protocol_by_id.get(selected_processing_protocol_id)

        all_measurement_target_ids: set[str] = set()
        all_processing_target_ids: set[str] = set()
        for protocol in measurement_protocol_by_id.values():
            all_measurement_target_ids.update(_collect_parameter_ids(protocol))
        for protocol in processing_protocol_by_id.values():
            all_processing_target_ids.update(_collect_parameter_ids(protocol))

        selected_measurement_target_ids = _collect_parameter_ids(selected_measurement_protocol)
        selected_processing_target_ids = _collect_parameter_ids(selected_processing_protocol)

        for assay_index, assay_value in enumerate(assay_details):
            assay = _as_object(assay_value)
            assay_path = f"{study_path}.assay_details[{assay_index}]"

            used_sensor = _as_object(assay.get("used_sensor"))
            used_sensor_id = used_sensor.get("id")
            if sensor_ids and isinstance(used_sensor_id, str) and used_sensor_id and used_sensor_id not in sensor_ids:
                add(
                    f"{assay_path}.used_sensor.id",
                    "unknown_sensor",
                    f"Unknown sensor id '{used_sensor_id}'",
                )

            runs = _as_list(assay.get("runs"))
            if total_runs is not None and len(runs) != total_runs:
                add(
                    f"{assay_path}.runs",
                    "run_count_mismatch",
                    f"Expected {total_runs} runs, found {len(runs)}",
                )

            run_numbers: list[int] = []
            for run_index, run_value in enumerate(runs):
                run = _as_object(run_value)
                run_path = f"{assay_path}.runs[{run_index}]"
                run_number = run.get("run_number")
                if not isinstance(run_number, int) or run_number < 1:
                    add(
                        f"{run_path}.run_number",
                        "invalid_run_number",
                        "run_number must be an integer >= 1",
                    )
                else:
                    run_numbers.append(run_number)

                processed_file_name = run.get("processed_file_name")
                if isinstance(processed_file_name, str) and processed_file_name.strip():
                    processed_outputs_present = True

            if total_runs is not None and run_numbers:
                expected_run_numbers = list(range(1, total_runs + 1))
                if sorted(run_numbers) != expected_run_numbers:
                    add(
                        f"{assay_path}.runs",
                        "invalid_run_sequence",
                        f"run_number values must match {expected_run_numbers}",
                    )

            measurement_entries = _as_list(assay.get("measurement_protocols"))
            for entry_index, entry_value in enumerate(measurement_entries):
                entry = _as_object(entry_value)
                entry_path = f"{assay_path}.measurement_protocols[{entry_index}]"

                source_id = entry.get("sourceId")
                if sensor_ids and isinstance(source_id, str) and source_id and source_id not in sensor_ids:
                    add(
                        f"{entry_path}.sourceId",
                        "unknown_sensor",
                        f"Unknown sourceId '{source_id}'",
                    )

                protocol_id = entry.get("protocolId")
                if isinstance(protocol_id, str) and protocol_id:
                    if protocol_id not in measurement_protocol_by_id:
                        add(
                            f"{entry_path}.protocolId",
                            "unknown_measurement_protocol",
                            f"Unknown measurement protocol id '{protocol_id}'",
                        )
                    if selected_measurement_protocol_id and protocol_id != selected_measurement_protocol_id:
                        add(
                            f"{entry_path}.protocolId",
                            "measurement_protocol_selection_mismatch",
                            "protocolId does not match selectedMeasurementProtocolId",
                        )

                target_id = entry.get("targetId")
                allowed_targets = selected_measurement_target_ids or all_measurement_target_ids
                if isinstance(target_id, str) and target_id and allowed_targets and target_id not in allowed_targets:
                    add(
                        f"{entry_path}.targetId",
                        "unknown_measurement_target",
                        f"Unknown measurement target id '{target_id}'",
                    )

            processing_entries = _as_list(assay.get("processing_protocols"))
            for entry_index, entry_value in enumerate(processing_entries):
                entry = _as_object(entry_value)
                entry_path = f"{assay_path}.processing_protocols[{entry_index}]"

                source_id = entry.get("sourceId")
                if sensor_ids and isinstance(source_id, str) and source_id and source_id not in sensor_ids:
                    add(
                        f"{entry_path}.sourceId",
                        "unknown_sensor",
                        f"Unknown sourceId '{source_id}'",
                    )

                protocol_id = entry.get("protocolId")
                if isinstance(protocol_id, str) and protocol_id:
                    if protocol_id not in processing_protocol_by_id:
                        add(
                            f"{entry_path}.protocolId",
                            "unknown_processing_protocol",
                            f"Unknown processing protocol id '{protocol_id}'",
                        )
                    if selected_processing_protocol_id and protocol_id != selected_processing_protocol_id:
                        add(
                            f"{entry_path}.protocolId",
                            "processing_protocol_selection_mismatch",
                            "protocolId does not match selectedProcessingProtocolId",
                        )

                target_id = entry.get("targetId")
                allowed_targets = selected_processing_target_ids or all_processing_target_ids
                if isinstance(target_id, str) and target_id and allowed_targets and target_id not in allowed_targets:
                    add(
                        f"{entry_path}.targetId",
                        "unknown_processing_target",
                        f"Unknown processing target id '{target_id}'",
                    )

        if processed_outputs_present and processing_protocol_by_id and not selected_processing_protocol_id:
            add(
                f"{study_path}.selectedProcessingProtocolId",
                "missing_processing_protocol_selection",
                "selectedProcessingProtocolId is required when processed outputs are present",
            )

    return issues

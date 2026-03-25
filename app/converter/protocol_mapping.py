from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from isatools.model import OntologyAnnotation, ProtocolParameter


def parse_protocol_entry(
    protocol_entry: Dict[str, Any],
    expected_source_id: Optional[str],
    protocol_defs: Dict[str, Dict[str, Any]],
    expected_protocol_id: Optional[str] = None,
) -> Optional[Tuple[Optional[str], str, Any, str]]:
    if expected_source_id and protocol_entry.get("sourceId") != expected_source_id:
        return None

    if expected_protocol_id and protocol_entry.get("protocolId") not in (expected_protocol_id, "", None):
        return None

    value_list = protocol_entry.get("value", []) or []
    if not value_list or str(value_list[0]).strip() == "":
        return None

    target_id = protocol_entry.get("targetId") or protocol_entry.get("id") or None
    resolved_name = target_id or ""
    if protocol_defs and target_id in protocol_defs:
        protocol_def = protocol_defs.get(target_id, {})
        resolved_name = protocol_def.get("name") or protocol_def.get("title") or target_id

    raw_value = value_list[0]
    raw_unit = value_list[1] if len(value_list) > 1 else ""
    return (target_id, resolved_name, raw_value, raw_unit)


def build_processing_parameters_for_sensor(
    study: Dict[str, Any],
    sensor: Dict[str, Any],
    processing_defs: Dict[str, Dict[str, Any]],
    selected_protocol_id: Optional[str] = None,
) -> List[ProtocolParameter]:
    params: List[ProtocolParameter] = []
    target_ids = set()

    sensor_id = sensor.get("id")
    for assay in study.get("assay_details", []):
        for entry in assay.get("processing_protocols", []):
            parsed = parse_protocol_entry(
                entry,
                sensor_id,
                processing_defs,
                expected_protocol_id=selected_protocol_id,
            )
            if not parsed:
                continue
            target_id, _, _, _ = parsed
            if target_id:
                target_ids.add(target_id)

    if processing_defs:
        for parameter_id in processing_defs.keys():
            if parameter_id in target_ids:
                pdef = processing_defs.get(parameter_id, {})
                parameter_name = pdef.get("name") or pdef.get("title") or parameter_id
                params.append(ProtocolParameter(parameter_name=OntologyAnnotation(parameter_name)))
    else:
        for parameter_id in target_ids:
            params.append(ProtocolParameter(parameter_name=OntologyAnnotation(parameter_id)))

    return params


def build_measurement_parameters_for_sensor(
    study: Dict[str, Any],
    sensor: Dict[str, Any],
    measurement_defs: Dict[str, Dict[str, Any]],
    selected_protocol_id: Optional[str] = None,
) -> List[ProtocolParameter]:
    params: List[ProtocolParameter] = []
    target_ids = set()

    sensor_id = sensor.get("id")
    for assay in study.get("assay_details", []):
        for entry in assay.get("measurement_protocols", []):
            parsed = parse_protocol_entry(
                entry,
                sensor_id,
                measurement_defs,
                expected_protocol_id=selected_protocol_id,
            )
            if not parsed:
                continue
            target_id, _, _, _ = parsed
            if target_id:
                target_ids.add(target_id)

    if measurement_defs:
        for parameter_id in measurement_defs.keys():
            if parameter_id in target_ids:
                pdef = measurement_defs.get(parameter_id, {})
                parameter_name = pdef.get("name") or pdef.get("title") or parameter_id
                params.append(ProtocolParameter(parameter_name=OntologyAnnotation(parameter_name)))
    else:
        for parameter_id in target_ids:
            params.append(ProtocolParameter(parameter_name=OntologyAnnotation(parameter_id)))

    return params

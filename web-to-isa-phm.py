from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple
from isatools.model import *
from isatools import isatab
# from isatools.isatab.dump.write import *
import argparse
import json
from isatools.isajson import ISAJSONEncoder
from copy import copy, deepcopy
import pandas as pd

# %%
#The following function creates units based on specified units in the input file. It prevents generation of duplicate units when the same unit occurs multiple times.
unit_list = [] #Initilize unit list
def get_or_create_unit(unit_term):
    if pd.notna(unit_term):
        # Check if the unit already exists
        for existing_unit in unit_list:
            if existing_unit.term == unit_term:
                return existing_unit  # Reuse
        # Create and store new unit
        new_unit = OntologyAnnotation(term=unit_term)
        unit_list.append(new_unit)
        return new_unit
    else:
        return None  # Nothing to assign


def add_unit_to_study(study_obj: Study, unit_term: str) -> OntologyAnnotation:
    """
    Adds a unit to the study's unit categories if it doesn't already exist.
    Returns the unit annotation object.
    
    Args:
        study_obj: The Study object to add the unit to
        unit_term: The unit term string (e.g., "RPM", "bar")
    
    Returns:
        OntologyAnnotation object for the unit, or None if unit_term is empty
    """
    if not unit_term:
        return None
        
    unit = get_or_create_unit(unit_term)
    if unit and unit not in study_obj.units:
        study_obj.units.append(unit)
    
    return unit


def parse_numeric_if_possible(raw_value: Any) -> Tuple[Any, bool]:
    """
    Try to parse raw_value into an int or float.
    Returns a tuple (parsed_value, is_numeric).
    If parsing fails, returns (original_value, False).
    """
    if raw_value is None:
        return raw_value, False
    # If it's already a numeric type, keep it
    if isinstance(raw_value, (int, float)):
        return raw_value, True

    s = str(raw_value).strip()
    # Try integer
    try:
        iv = int(s)
        return iv, True
    except Exception:
        pass

    # Try float
    try:
        fv = float(s)
        return fv, True
    except Exception:
        pass

    return raw_value, False


def parse_processing_protocol_entry(processing_entry: Dict[str, Any], expected_source_id: Optional[str], processing_defs: Dict[str, Dict[str, Any]]) -> Optional[Tuple[Optional[str], str, Any, str]]:
    """
    Parse a single processing_protocol entry and return:
      (target_id, resolved_name, raw_value, raw_unit)
    Returns None if the entry should be skipped (wrong sourceId or empty value).
    """
    # If a specific sourceId is expected and this entry doesn't match, skip it
    if expected_source_id and processing_entry.get("sourceId") != expected_source_id:
        return None

    value_list = processing_entry.get("value", []) or []
    if not value_list or str(value_list[0]).strip() == "":
        return None

    target_id = processing_entry.get("targetId") or processing_entry.get("id") or None

    resolved_name = target_id
    if processing_defs and target_id in processing_defs:
        param_def = processing_defs.get(target_id, {})
        resolved_name = param_def.get("name") or param_def.get("title") or target_id

    raw_value = value_list[0]
    raw_unit = value_list[1] if len(value_list) > 1 else ""

    return (target_id, resolved_name, raw_value, raw_unit)


def build_processing_parameters_for_sensor(
    study: Dict[str, Any],
    sensor: Dict[str, Any],
    processing_defs: Dict[str, Dict[str, Any]],
) -> List[ProtocolParameter]:
    """
    Build a list of ProtocolParameter objects for a sensor by scanning all assays
    in the provided study for processing_protocols where value[0] is non-empty.

    processing_defs is a mapping id -> definition (may contain 'name' or 'title').
    Returns parameters in the order of processing_defs when available; otherwise arbitrary order.
    """
    params: List[ProtocolParameter] = []
    target_ids = set()

    sensor_id = sensor.get("id")
    # collect referenced targetIds where the first value is non-empty
    for assay in study.get("assay_details", []):
        for processing_entry in assay.get("processing_protocols", []):
            parsed_entry = parse_processing_protocol_entry(processing_entry, sensor_id, processing_defs)
            if not parsed_entry:
                continue
            target_id, _, _, _ = parsed_entry
            if target_id:
                target_ids.add(target_id)

    # Preserve a stable ordering: prefer the order from processing_defs when present
    if processing_defs:
        for pid in processing_defs.keys():
            if pid in target_ids:
                pdef = processing_defs.get(pid, {})
                pname = pdef.get("name") or pdef.get("title") or pid
                params.append(ProtocolParameter(parameter_name=OntologyAnnotation(pname)))
    else:
        for pid in target_ids:
            params.append(ProtocolParameter(parameter_name=OntologyAnnotation(pid)))

    return params

@dataclass
class IsaPhmInfo:
    """
    Top-level container for investigation metadata,
    including studies, contacts, and publications.
    Any dates should be in ISO 8601 format.
    """
    identifier: str = "i0"
    title: str = ""
    description: str = ""
    submission_date: str = ""
    public_release_date: str = ""
    publication: Publication = None
    contacts: List[Person] = field(default_factory=list)
    # study_details: List[StudyInfo] = field(default_factory=list)

def create_isa_data(IsaPhmInfo: dict, output_path: str = None) -> Investigation:
    """
    Builds the full ISA investigation object from the IsaPhmInfo metadata.
    """
    investigation = Investigation()
    investigation.filename = output_path if output_path else "isa_phm.json"
    investigation.identifier = IsaPhmInfo.get("identifier", "i0")
    investigation.title = IsaPhmInfo.get("title", "")
    investigation.description = IsaPhmInfo.get("description", "")
    investigation.submission_date = IsaPhmInfo.get("submission_date", "")
    investigation.public_release_date = IsaPhmInfo.get("public_release_date", "")
    investigation.comments.append(Comment(name="License", value="MIT License"))
    
    # INVESTIGATION CONTACTS
    authors: List[Dict[str, Any]] = IsaPhmInfo.get("authors", [])
    for contact in authors:
        person = Person()
        person.first_name   = contact.get("firstName", "")
        person.mid_initials = contact.get("midInitials", "")
        person.last_name    = contact.get("lastName", "")
        person.email        = contact.get("email", "")
        person.phone        = contact.get("phone", "")
        person.fax          = contact.get("fax", "")
        person.address      = contact.get("address", "")
        person.affiliation  = "; ".join(contact.get("affiliations", []))
        person.roles.extend([OntologyAnnotation(role) for role in contact.get("roles", [])])
        person.comments.append(Comment(name="orcid", value=contact.get("orcid", "")))
        person.comments.append(Comment(name="subroles", value="; ".join(contact.get("subroles", []))))
        person.comments.append(Comment(name="author_id", value=contact.get("id", "")))
        
        investigation.contacts.append(person)

    # INVESTIGATION PUBLICATIONS
    publications: List[Dict[str, Any]] = IsaPhmInfo.get("publications", [])
    for publication in publications:
        publication_obj = Publication()
        publication_obj.title = publication.get("title", "")
        publication_obj.author_list = "; ".join(["#" + author for author in publication.get("authorList", [])])
        publication_obj.status = OntologyAnnotation(publication.get("publicationStatus", "unknown"))
        publication_obj.doi = publication.get("doi", "")
        investigation.publications.append(publication_obj)


    # Precompute processing definitions lookup (id -> def) for the whole investigation
    processing_defs = {p.get("id"): p for p in IsaPhmInfo.get("processing_protocols", [])} if IsaPhmInfo.get("processing_protocols") else {}

    # INVESTIGATION STUDIES
    studies: List[Dict[str, Any]] = IsaPhmInfo.get("studies", [])
    for study_index, study in enumerate(studies, start=1):
        study_obj = Study()
        study_obj.filename = f"a_s{study_index:02d}_.txt"
        study_obj.identifier = study.get("id", "")
        study_obj.title = study.get("name", "")
        study_obj.description = study.get("description", "")
        study_obj.submission_date = study.get("submissionDate", "")
        study_obj.public_release_date = study.get("publicationDate", "")
        study_obj.publications.extend(investigation.publications)   # ID REFERENCE OR FULL REFERENCE?
        study_obj.contacts.extend(investigation.contacts)           # ID REFERENCE OR FULL REFERENCE?
        #TODO: Add experiment type in online form
        study_obj.design_descriptors.append(OntologyAnnotation(study.get("experimentType", "Diagnostics")))

    # (processing_defs is precomputed for the whole investigation)

        # Experiment preparation Protocol
        experiment_prep_protocol = Protocol(name="Experiment Preparation")
        experiment_prep_protocol.protocol_type = OntologyAnnotation("Experiment Preparation Protocol")
        study_obj.protocols.append(experiment_prep_protocol)

        # Measurement Protocols
        test_setup_obj: Dict[str, Any] = study.get("used_setup", {})
        for sensor in test_setup_obj.get("sensors", []):
            # Include sensor ID or name to make protocol names unique
            sensor_id = sensor.get("id", "") or sensor.get("name", "") or sensor.get("sensorLocation", "") or f"sensor_{len(study_obj.protocols)}"
            
            protocol = Protocol(
                name=f'{sensor.get("measurementType", "")} measurement ({sensor_id})', 
                description=sensor.get("description", "no description provided")
            )
            protocol.protocol_type = OntologyAnnotation("Measurement Protocol")
            protocol.comments.append(Comment(name="Sensor id", value=sensor.get("id", "")))
            
            # Add parameters only if they exist in the sensor definition
            if sensor.get("sensorLocation", ""):
                protocol.parameters.append(ProtocolParameter(parameter_name=OntologyAnnotation("Sensor Location")))
            if sensor.get("sensorOrientation", ""):
                protocol.parameters.append(ProtocolParameter(parameter_name=OntologyAnnotation("Sensor Orientation")))
            if sensor.get("samplingRate", ""):
                protocol.parameters.append(ProtocolParameter(parameter_name=OntologyAnnotation("Sampling Rate")))
            if sensor.get("measurementUnit", ""):
                protocol.parameters.append(ProtocolParameter(parameter_name=OntologyAnnotation("Measured Unit")))
            if sensor.get("phase", ""):
                protocol.parameters.append(ProtocolParameter(parameter_name=OntologyAnnotation("Phase")))

            # Add protocol to study
            study_obj.protocols.append(protocol)
        
        # Processing Protocols
        for sensor in test_setup_obj.get("sensors", []):
            # Use the same sensor ID logic
            sensor_id = sensor.get("id", "") or sensor.get("name", "") or sensor.get("sensorLocation", "") or f"sensor_{len(study_obj.protocols)}"
            
            protocol = Protocol(
                name=f'{sensor.get("measurementType", "")} processing ({sensor_id})', 
                description=sensor.get("description", "")
            )
            protocol.protocol_type = OntologyAnnotation("Processing Protocol")
            protocol.comments.append(Comment(name="Sensor id", value=sensor.get("id", "")))
            
            # Dynamically build processing parameters for this sensor (helper handles ordering)
            protocol.parameters.extend(build_processing_parameters_for_sensor(study, sensor, processing_defs))
       
            study_obj.protocols.append(protocol)
        
        # Adds Material -> Source to ISA
        source = Source(name=test_setup_obj.get("name", "Test Setup"))
        study_obj.sources.append(source)
        
        # Adds Material -> Sample to ISA
        dummy_sample = Sample(name="Test Setup Characteristics", derives_from=[source])
        for characteristic in test_setup_obj.get("characteristics", []):           
            category = OntologyAnnotation(term=characteristic.get("category", "unknown"))
            study_obj.characteristic_categories.append(category)
            
            # Extract unit (if exists) and add to study units
            unit = add_unit_to_study(study_obj, characteristic.get("unit", ""))
            
            characteristic_obj = Characteristic()
            characteristic_obj.category = category
            characteristic_obj.value = characteristic.get("value", "")
            characteristic_obj.unit = unit  # will be None if no unit specified and will not be added
            
            ## TODO: May delete these two lines if they are redundant
            characteristic_obj.comments.append(Comment(name="name", value=characteristic.get("category", "")))
            characteristic_obj.comments.append(Comment(name="unit", value=characteristic.get("unit", "")))
            
            dummy_sample.characteristics.append(characteristic_obj)
        
        
        # Study Factors and Factor Values
        for variable in IsaPhmInfo.get("study_variables", []):
            
            # Create Study Factor for each study variable
            study_factor_obj = StudyFactor(name=variable.get("name", ""), factor_type=OntologyAnnotation(variable.get("type", "unknown")))

            study_factor_obj.comments.append(Comment(name="description", value=variable.get("description", "")))
            study_factor_obj.comments.append(Comment(name="unit", value=variable.get("unit", "")))
            study_factor_obj.comments.append(Comment(name="min", value=variable.get("min", "")))
            study_factor_obj.comments.append(Comment(name="max", value=variable.get("max", "")))
            study_factor_obj.comments.append(Comment(name="step", value=variable.get("step", "")))
            
            study_obj.factors.append(study_factor_obj)
            
            
            # Give value to each factor (study variable)
            factor_value = FactorValue()
            factor_value.factor_name = study_factor_obj
            
            # Find the corresponding variable in the study variables
            mapping = list(filter(lambda x: x.get("variableName") == study_factor_obj.name, study.get("study_to_study_variable_mapping", [])))
            if mapping:
                mapping = mapping[0]
                factor_value.value = mapping.get("value", "unknown")
                factor_value.unit = add_unit_to_study(study_obj, variable.get("unit", ""))
            else:
                # If no mapping found, set a placeholder value
                print(f"Warning: No mapping found for factor {study_factor_obj.name}. Setting placeholder value.")
                factor_value.value = "unknown"
         
            dummy_sample.factor_values.append(factor_value)
        
        study_obj.samples.append(dummy_sample)
        
        # Study Process Sequence to get from source (test setup) to sample (test setup characteristics)
        experiment_prerparation_process = Process(executes_protocol=experiment_prep_protocol)
        experiment_prerparation_process.inputs.append(source)
        experiment_prerparation_process.outputs.append(dummy_sample)
        study_obj.process_sequence.append(experiment_prerparation_process)
        
        
        # Assays
        for assay in study.get("assay_details", []):
            assay_obj = Assay(filename=assay.get("assay_file_name", "unknown.txt"))
            
            # Get the sensor for THIS specific assay
            assay_sensor = assay.get("used_sensor", {})
            assay_measurement_type = assay_sensor.get("measurementType", "")
            
            assay_obj.measurement_type = OntologyAnnotation(assay_measurement_type)
            assay_obj.technology_type = OntologyAnnotation(assay_sensor.get("technologyType", "unknown"))
            assay_obj.technology_platform = assay_sensor.get("technologyPlatform", "unknown")
            assay_obj.samples.append(dummy_sample)
            
            # Each assay will have a raw and processed data file
            raw_data_file = DataFile(
                filename=assay.get("raw_file_name", "unknown.txt"),
                label="Raw Data File",
                generated_from=dummy_sample
            )
            assay_obj.data_files.append(raw_data_file)
            
            processed_data_file = DataFile(
                filename=assay.get("processed_file_name", "") if assay.get("processed_file_name") else "not-used.txt",
                label="Processed Data File",
                generated_from=dummy_sample
            )
            assay_obj.data_files.append(processed_data_file)

            # Raw data and Processed data files are generated by processes
            collect_raw_data_process = Process(name="Collect Raw Data", executes_protocol=experiment_prep_protocol, parameter_values=[])
            collect_raw_data_process.inputs.append(dummy_sample)
            collect_raw_data_process.outputs.append(raw_data_file)
            assay_obj.process_sequence.append(collect_raw_data_process)

            # Now find and use ONLY the protocols for THIS specific sensor/measurement type
            for protocol in study_obj.protocols:
                if protocol.name == "Experiment Preparation":
                    continue
                
                # Get the sensor ID from the assay
                assay_sensor_id = assay_sensor.get("id", "") or assay_sensor.get("name", "") or assay_sensor.get("sensorLocation", "")
                if not assay_sensor_id:
                    # If no ID found, try to match by measurement type only (fallback)
                    assay_sensor_id = ""
                
                # Check if this protocol belongs to THIS specific sensor
                expected_measurement_name = f'{assay_measurement_type} measurement ({assay_sensor_id})' if assay_sensor_id else f'{assay_measurement_type} measurement'
                expected_processing_name = f'{assay_measurement_type} processing ({assay_sensor_id})' if assay_sensor_id else f'{assay_measurement_type} processing'
                
                # Match protocols with exact names or fallback to partial matching
                if (protocol.name == expected_measurement_name or 
                    (not assay_sensor_id and protocol.name.startswith(f'{assay_measurement_type} measurement'))):
                    
                    parameter_map = {
                        "Sensor Location": [assay_sensor.get("sensorLocation", ""), assay_sensor.get("locationUnit", "")],
                        "Sensor Orientation": [assay_sensor.get("sensorOrientation", ""), assay_sensor.get("orientationUnit", "")],
                        "Sampling Rate": [assay_sensor.get("samplingRate", ""), assay_sensor.get("samplingUnit", "")],
                        "Measured Unit": [assay_sensor.get("measurementUnit", ""), ""]
                    }
                    parameter_list = []
                    
                    for parameter in protocol.parameters:
                        if parameter.parameter_name.term in parameter_map:
                            value = parameter_map[parameter.parameter_name.term]
                            parsed_value, is_numeric = parse_numeric_if_possible(value[0])
                            if value[1] and is_numeric:
                                unit_obj = add_unit_to_study(study_obj, value[1])
                            else:
                                # If a unit was provided but the value is not numeric, skip attaching the unit
                                if value[1]:
                                    print(f"Warning: not attaching unit '{value[1]}' to non-numeric value '{value[0]}' for parameter '{parameter.parameter_name.term}'")
                                unit_obj = None

                            parameter_list.append(ParameterValue(category=parameter, value=parsed_value, unit=unit_obj))

                    process = Process(executes_protocol=protocol, parameter_values=parameter_list)
                    process.inputs.append(dummy_sample)
                    process.outputs.append(raw_data_file)
                    assay_obj.process_sequence.append(process)
                    
                # Use per-assay processing_protocols entries (from front-end) as ParameterValue(s)
                elif (protocol.name == expected_processing_name or 
                      (not assay_sensor_id and protocol.name.startswith(f'{assay_measurement_type} processing'))):

                    parameter_list = []

                    # Each processing protocol entry is parsed via helper; it returns (target_id, pname, raw_value, raw_unit)
                    for processing_entry in assay.get("processing_protocols", []):
                        parsed_entry = parse_processing_protocol_entry(processing_entry, assay_sensor_id, processing_defs)
                        if not parsed_entry:
                            continue

                        target_id, pname, raw_value, raw_unit = parsed_entry

                        # Try to match existing ProtocolParameter in the protocol by term or id
                        matching_param = next((p for p in protocol.parameters if getattr(p.parameter_name, "term", None) in (pname, target_id)), None)
                        category_param = matching_param if matching_param is not None else ProtocolParameter(parameter_name=OntologyAnnotation(pname))

                        parsed_value, is_numeric = parse_numeric_if_possible(raw_value)
                        if raw_unit and is_numeric:
                            unit_obj = add_unit_to_study(study_obj, raw_unit)
                        else:
                            if raw_unit and not is_numeric:
                                print(f"Warning: not attaching unit '{raw_unit}' to non-numeric processing value '{raw_value}' (parameter {pname})")
                            unit_obj = None

                        parameter_list.append(ParameterValue(category=category_param, value=parsed_value, unit=unit_obj))

                    process = Process(executes_protocol=protocol, parameter_values=parameter_list)
                    process.inputs.append(raw_data_file)
                    process.outputs.append(processed_data_file)
                    assay_obj.process_sequence.append(process)

            # Link the processes in sequence
            for i in range(len(assay_obj.process_sequence) - 1):
                process1 = assay_obj.process_sequence[i]
                process2 = assay_obj.process_sequence[i + 1]
                plink(process1, process2)

            study_obj.assays.append(assay_obj)
        
        
        investigation.studies.append(study_obj)
        
        

    return investigation

def main(args):
    json_data = json.load(open(args.file, "r"))
    print(f"Loading ISA-PhM JSON file: {args.file}")
    print(type(json_data))
    investigation = create_isa_data(IsaPhmInfo=json_data, output_path=args.outfile)

    with open(investigation.filename, "w") as f:
        json.dump(investigation, f, cls=ISAJSONEncoder, sort_keys=False,
                  indent=4, separators=(',', ': '))

    print(f"ISA-PhM JSON file created: {investigation.filename}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("file",
                        help="input a json file that contains the necessary" +
                        " information to create the isa-phm")
    parser.add_argument("outfile", default="isa_phm.json",
                        help="output file name for the isa-phm json file")
    args = parser.parse_args()
    main(args)

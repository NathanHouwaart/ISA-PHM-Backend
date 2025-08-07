from dataclasses import dataclass, field
from typing import List, Dict, Any
from isatools.model import *
from isatools import isatab
# from isatools.isatab.dump.write import *
import argparse
import json
from isatools.isajson import ISAJSONEncoder
from copy import copy, deepcopy

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
        person.affiliation  = contact.get("affiliation", "")
        person.comments.append(Comment(name="orcid", value=contact.get("orcid", "")))
        person.roles.append(OntologyAnnotation(contact.get("role", "unknown")))
        
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


    # INVESTIGATION STUDIES
    studies: List[Dict[str, Any]] = IsaPhmInfo.get("studies", [])
    for study in studies:
        study_obj = Study()
        study_obj.identifier = study.get("id", "")
        study_obj.title = study.get("name", "")
        study_obj.description = study.get("description", "")
        study_obj.submission_date = study.get("submissionDate", "")
        study_obj.public_release_date = study.get("publicationDate", "")
        study_obj.publications.extend(investigation.publications)   # ID REFERENCE OR FULL REFERENCE?
        study_obj.contacts.extend(investigation.contacts)           # ID REFERENCE OR FULL REFERENCE?
        #TODO: Add experiment type in online form
        study_obj.design_descriptors.append(OntologyAnnotation(study.get("experimentType", "Diagnostics")))

        # Experiment prepartaion Protocol
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
                description=sensor.get("description", "")
            )
            protocol.protocol_type = OntologyAnnotation("Measurement Protocol")
            
            protocol.parameters.append(ProtocolParameter(parameter_name=OntologyAnnotation("Sensor Location")))
            protocol.parameters.append(ProtocolParameter(parameter_name=OntologyAnnotation("Sensor Orientation")))
            protocol.parameters.append(ProtocolParameter(parameter_name=OntologyAnnotation("Sampling Rate")))
            protocol.parameters.append(ProtocolParameter(parameter_name=OntologyAnnotation("Measured Unit")))
       
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
            
            protocol.parameters.append(ProtocolParameter(parameter_name=OntologyAnnotation("Filter Type")))
            protocol.parameters.append(ProtocolParameter(parameter_name=OntologyAnnotation("Chunk Size")))
            protocol.parameters.append(ProtocolParameter(parameter_name=OntologyAnnotation("Scaling Range")))
            protocol.parameters.append(ProtocolParameter(parameter_name=OntologyAnnotation("Scaling Resolution")))
       
            study_obj.protocols.append(protocol)
            
        source = Source(name=test_setup_obj.get("name", "Test Setup"))
        study_obj.sources.append(source)
        
        dummy_sample = Sample(name="Test Setup Characteristics", derives_from=[source])
        for characteristic in test_setup_obj.get("characteristics", []):            
            characteristic_obj = Characteristic()
            characteristic_obj.category = characteristic.get("category", "")        #TODO: DOES NOT GET INJECTED IN JSON
            characteristic_obj.value = characteristic.get("value", "")            
            characteristic_obj.unit = characteristic.get("unit", "")                #TODO: DOES NOT GET INJECTED IN JSON
            characteristic_obj.comments.append(Comment(name="name", value=characteristic.get("category", "")))
            characteristic_obj.comments.append(Comment(name="unit", value=characteristic.get("unit", "")))

            dummy_sample.characteristics.append(characteristic_obj)
        
        for variable in IsaPhmInfo.get("study_variables", []):
            study_factor_obj = StudyFactor(name=variable.get("name", ""), factor_type=OntologyAnnotation(variable.get("type", "unknown")))
            study_factor_obj.comments.append(Comment(name="description", value=variable.get("description", "")))
            study_factor_obj.comments.append(Comment(name="unit", value=variable.get("unit", "")))
            study_factor_obj.comments.append(Comment(name="min", value=variable.get("min", "")))
            study_factor_obj.comments.append(Comment(name="max", value=variable.get("max", "")))
            study_factor_obj.comments.append(Comment(name="step", value=variable.get("step", "")))
            
            study_obj.factors.append(study_factor_obj)
            # dummy_sample.factors.append(factor_name)
            # study_obj.factors.append(factor_name)
        
        # Give value to each factor (study variable)
        for factor in study_obj.factors:
            factor_value = FactorValue()
            factor_value.factor_name = factor
            
            # Find the corresponding variable in the study variables
            variable = list(filter(lambda x: x.get("variableName") == factor.name, study.get("study_to_study_variable_mapping", [])))
            if variable:
                variable = variable[0]
                factor_value.value = variable.get("value", "unknown")
                # factor_value.unit = OntologyAnnotation(variable.get("unit", "unknown"))
            else:
                # If no mapping found, set a placeholder value
                print(f"Warning: No mapping found for factor {factor.name}. Setting placeholder value.")
                factor_value.value = "unknown"
                # factor_value.unit = OntologyAnnotation("unknown")
                
            # factor_value.value = "placeholder_value"  # Placeholder value, replace with actual logic
            # factor_value.unit = OntologyAnnotation(test_setup_obj.get("testunit", "testunit"))
            dummy_sample.factor_values.append(factor_value)
        
        study_obj.samples.append(dummy_sample)
        
        # Study Process Sequence to get from source (test setup) to sample (test setup characteristics)
        experiment_prerparation_process = Process(executes_protocol=experiment_prep_protocol)
        experiment_prerparation_process.inputs.append(source)
        experiment_prerparation_process.outputs.append(dummy_sample)
        study_obj.process_sequence.append(experiment_prerparation_process)
        
        
        # Assays
        for assay in study.get("assay_details", []):
            assay_obj = Assay(filename=assay.get("raw_file_name", "unknown.txt"))
            
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
                            try:
                                # Try to convert to int if it looks like a number and has a unit
                                param_value = int(value[0]) if value[1] and value[0].isdigit() else value[0]
                            except (ValueError, TypeError):
                                param_value = value[0]
                                
                            parameter_list.append(
                                ParameterValue(
                                    category=parameter,
                                    value=param_value,
                                    unit=OntologyAnnotation(value[1]) if value[1] else None
                                )
                            )

                    process = Process(executes_protocol=protocol, parameter_values=parameter_list)
                    process.inputs.append(dummy_sample)
                    process.outputs.append(raw_data_file)
                    assay_obj.process_sequence.append(process)

                elif (protocol.name == expected_processing_name or 
                      (not assay_sensor_id and protocol.name.startswith(f'{assay_measurement_type} processing'))):
                    
                    file_parameters_list = assay.get("file_details", {}).get("file_parameters", [])
                    file_parameters = {param.get("parameter", ""): param.get("value", "") for param in file_parameters_list}

                    parameter_map = {
                        "Filter Type": [file_parameters.get("processingProtocolFilterTypeSpecification", ""), ""],
                        "Chunk Size": [file_parameters.get("processingProtocolChunkSizeSpecification", ""), ""],
                        "Scaling Range": [file_parameters.get("processingProtocolScalingRangeSpecification", ""), ""],
                        "Scaling Resolution": [file_parameters.get("processingProtocolScalingResolutionSpecification", ""), file_parameters.get("processingProtocolScalingResolutionUnit", "")]
                    }
                    parameter_list = []
                                            
                    for parameter in protocol.parameters:
                        if parameter.parameter_name.term in parameter_map:
                            value = parameter_map[parameter.parameter_name.term]
                            try:
                                # Try to convert to int if it looks like a number and has a unit
                                param_value = int(value[0]) if value[1] and value[0].isdigit() else value[0]
                            except (ValueError, TypeError):
                                param_value = value[0]
                                
                            parameter_list.append(
                                ParameterValue(
                                    category=parameter,
                                    value=param_value,
                                    unit=OntologyAnnotation(value[1]) if value[1] else None
                                )
                            )

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

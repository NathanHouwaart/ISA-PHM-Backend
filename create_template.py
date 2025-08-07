"""
ISA-PHM Template Generator
--------------------------

This script generates ISA-PHM (Investigation/Study/Assay for
Prognostics and Health Management) templates from a provided JSON file
containing the necessary metadata. It leverages the isatools library to
construct ISA-Tab or ISA-JSON representations of experimental investigations,
studies, assays, and associated metadata such as sensors, test setups,
file details, and fault/degradation protocols.

--------
- FileParameter: Represents a protocol parameter and its value for a file.
- FileDetails: Holds information about a data file, including names, locations,
    parameters, and labels.
- Sensor: Describes a sensor used in the experiment, including its type, unit,
    and technical details.
- TestSetUp: Contains details about the experimental setup, including sensors
    and their characteristics.
- AssayInfo: Holds information about an assay, including the sensor used and
    file details.
- RunInfo: Holds information about a run, including run conditions and assay
    details.
- StudyInfo: Aggregates all metadata for a study, including title, contacts,
    setup, faults, and assays.
- IsaPhmInfo: Top-level container for investigation metadata,
    including studies, contacts, and publications.

----------
- create_info(filename) -> IsaPhmInfo:
    Parses the input JSON file and returns an IsaPhmInfo object with all
    relevant metadata.

- create_study_descriptor(experiment_type):
    Creates an ontology annotation for the study's experiment type.

- add_test_setup(study: Study, study_info: StudyInfo):
    Adds test setup sources and samples to the study based on provided
    setup information.

- create_assay_data(assay_info: AssayInfo, sample: Sample, study: Study,
                    s_index: int, r_index: int, test_setup: TestSetUp):
    Constructs assay data, including protocols, processes, and data files,
    and links them to the sample.

- create_fault_preparation_protocol(study: Study, study_info: StudyInfo):
    Adds a fault/degradation preparation protocol and process to the study.

- create_study_data(study_info: StudyInfo, index: int):
    Assembles all study-level metadata, including samples, factors, protocols,
    and assays.

- create_isa_data(IsaPhmInfo: IsaPhmInfo):
    Builds the full ISA investigation object from the IsaPhmInfo metadata.

- main(args):
    Entry point for the script. Parses arguments, loads metadata,
    generates ISA structures, and writes output files.

------

Dependencies:
    - isatools
    - argparse
    - dataclasses
    - typing
    - json

Note:
    The script expects the input JSON file to conform to the structure
    required by the IsaPhmInfo class and its nested components.
    If imported to another python file and an IsaPhmInfo object is made you can
    use the function create_isa_data to create the investigation object.
"""

from dataclasses import dataclass, field
from typing import List
from isatools.model import *
from isatools import isatab
# from isatools.isatab.dump.write import *
import argparse
import json
from isatools.isajson import ISAJSONEncoder

TXT_ending = False

@dataclass
class FileParameter:
    """Represents a protocol parameter and its value for a file."""
    parameter: ProtocolParameter
    value: ParameterValue


@dataclass
class FileDetails:
    """
    Holds information about a data file, including names, locations,
    parameters, and labels.
    """
    raw_file_name: str = ""  # Name of the raw data file
    raw_file_location: str = ""  # Location/path of the raw data file
    proccesed_file_name: str = ""  # Name of the processed data file
    proccesed_file_location: str = ""  # Location of the processed data file
    file_parameters: List[FileParameter] = field(default_factory=list)
    number_of_columns: int = 0  # Number of columns in the data file
    labels: List[str] = field(default_factory=list)  # Column labels


@dataclass
class Sensor:
    """
    Describes a sensor used in the experiment, including its type, unit,
    and technical details.
    """
    identifier: str = "s0"  # Unique sensor identifier
    measurement_type: str = ""
    measurement_unit: str = ""
    description: str = ""
    technology_type: str = ""
    technology_platform: str = ""
    data_acquisition_unit: str = ""
    sampling_rate: str = ""
    sampling_unit: str = ""
    sensor_location: str = ""
    location_unit: str = ""
    sensor_orientation: str = ""
    orientation_unit: str = ""


@dataclass
class TestSetUp:
    """
    Contains details about the experimental setup, including sensors
    and their characteristics.
    """
    name: str = ""
    location: str = ""
    characteristics: List[Characteristic] = field(default_factory=list)
    number_of_sensors: int = 0
    sensors: List[Sensor] = field(default_factory=list)


@dataclass
class AssayInfo:
    """
    Holds information about an assay,
    including the sensor used and file details.
    """
    used_sensor: Sensor
    file_details: FileDetails


@dataclass
class RunInfo:
    """
    Holds information about a run, including run conditions and assay details.
    """
    run_conditions: List[FactorValue] = field(default_factory=list)
    assay_details: List[AssayInfo] = field(default_factory=list)


@dataclass
class StudyInfo:
    """
    Aggregates all metadata for a study, including title, contacts,
    setup, faults, and assays.
    """
    title: str = ""
    description: str = ""
    submission_date: str = ""
    public_release_date: str = ""
    detail_preparation: str = ""
    publication: Publication = None
    contacts: List[Person] = field(default_factory=list)
    experiment_type: str = ""
    used_setup: TestSetUp = None
    fault_type: str = ""  # Fault, Degradation
    fault_position: str = ""  # Position of the fault
    fault_severity: str = ""  # Severity of the fault
    runs: List[RunInfo] = field(default_factory=list)


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
    study_details: List[StudyInfo] = field(default_factory=list)


def json_to_comment(data):
    """
    Converts a JSON comment object to an ISA comment object.
    """
    return Comment(**data)


def json_to_person(data):
    """
    Converts a JSON person object to an ISA Person object.
    """
    data["comments"] = [json_to_comment(c) for c in data.get("comments", [])]
    data["roles"] = [OntologyAnnotation(term=r) for r in data.get("roles", [])]
    return Person(**data)


def json_to_publication(data):
    """
    Converts a JSON publication object to an ISA Publication object.
    """
    if data is None:
        return None
    data["comments"] = [json_to_comment(c) for c in data.get("comments", [])]
    data["status"] = OntologyAnnotation(data.get("status", ""))
    return Publication(**data)


def json_to_characteristic(data):
    """
    Converts a JSON characteristic object to an ISA Characteristic object.
    """
    data["comments"] = [json_to_comment(c) for c in data.get("comments", [])]
    return Characteristic(**data)


def json_to_file_parameter(data):
    """
    Converts a JSON file parameter object to a FileParameter object.
    Handles conversion of value to float if possible and unit is present.
    """
    pdata = data["parameter"]
    pdata["comments"] = [json_to_comment(c) for c in data.get("comments", [])]
    # Convert parameter_name string to OntologyAnnotation if it's a string
    if "parameter_name" in pdata and isinstance(pdata["parameter_name"], str):
        pdata["parameter_name"] = OntologyAnnotation(term=pdata["parameter_name"])
    vdata = data["value"]
    vdata["comments"] = [json_to_comment(c) for c in data.get("comments", [])]
    p = ProtocolParameter(**pdata)
    # Only convert to float if value is numeric and unit is present
    if "unit" in vdata:
        try:
            vdata["value"] = float(vdata["value"])
        except (ValueError, TypeError):
            # Leave as string if not convertible
            pass
        vdata["unit"] = OntologyAnnotation(vdata["unit"])
    v = ParameterValue(**vdata, category=p)
    return FileParameter(parameter=p, value=v)


def json_to_file_details(data):
    """
    Converts a JSON object to a FileDetails object.
    """
    data["file_parameters"] = [json_to_file_parameter(fp) for fp in
                               data.get("file_parameters", [])]
    return FileDetails(**data)


def json_to_sensor(data):
    """
    Converts a JSON object to a Sensor object.
    """
    return Sensor(**data)


def json_to_assay_info(data):
    """
    Converts a JSON object to an AssayInfo object.
    """
    data["file_details"] = json_to_file_details(data.get("file_details", {}))
    data["used_sensor"] = json_to_sensor(data.get("used_sensor", {}))
    return AssayInfo(**data)


def json_to_test_setup(data):
    """
    Converts a JSON object to a TestSetUp object.
    """
    data["characteristics"] = [json_to_characteristic(c) for c in
                               data.get("characteristics", [])]
    data["sensors"] = [json_to_sensor(s) for s in data.get("sensors", [])]
    return TestSetUp(**data)


def json_to_factor_value(data):
    """
    Converts a JSON object to a FactorValue object.
    """
    sf = StudyFactor(name=data["name"])
    if "factor_type" in data:
        sf.factor_type = OntologyAnnotation(data["factor_type"])
    fv = FactorValue(factor_name=sf, value=data["value"])
    if "unit" in data:
        fv.unit = OntologyAnnotation(data["unit"])
    return fv


def json_to_run_info(data):
    """
    Converts a JSON object to a RunInfo object.
    """
    data["run_conditions"] = [json_to_factor_value(c) for
                              c in data.get("run_conditions", [])]
    data["assay_details"] = [json_to_assay_info(a) for
                             a in data.get("assay_details", [])]
    return RunInfo(**data)


def json_to_study_info(data):
    """
    Converts a JSON object to a StudyInfo object.
    """
    data["publication"] = json_to_publication(data.get("publication", None))
    data["contacts"] = [json_to_person(c) for c in data.get("contacts", [])]
    data["used_setup"] = json_to_test_setup(data.get("used_setup", None))
    data["runs"] = [json_to_run_info(r) for r in data.get("runs", [])]
    return StudyInfo(**data)


def json_to_isa_phm_info(data):
    """
    Converts a JSON object to an IsaPhmInfo object.
    """
    data["publication"] = json_to_publication(data.get("publication", None))
    data["contacts"] = [json_to_person(c) for c in data.get("contacts", [])]
    data["study_details"] = [json_to_study_info(s) for
                             s in data.get("study_details", [])]
    return IsaPhmInfo(**data)


def create_info(filename) -> IsaPhmInfo:
    """
    Parses the input JSON file and returns an IsaPhmInfo object with all
    relevant metadata.
    """
    data = None
    with open(filename, 'r') as file:
        data = json.load(file)
    return json_to_isa_phm_info(data)


def create_study_descriptor(experiment_type):
    """
    Creates an ontology annotation for the study's experiment type.
    """
    annotation = OntologyAnnotation(term=experiment_type)
    return annotation


def add_test_setup(study: Study, study_info: StudyInfo):
    """
    Adds test setup sources and samples to the study based on provided
    setup information.
    """
    source = Source(name=study_info.used_setup.name)
    study.sources.append(source)
    sample = Sample(
        name=f"{study_info.fault_type}_" +
             f"{study_info.fault_position}_{study_info.fault_severity}",
        derives_from=[source])
    sample.characteristics = study_info.used_setup.characteristics
    # Add fault/degradation factors if not diagnostic
    if study_info.experiment_type != "Diagnostic":
        FT = StudyFactor(name="Fault Type",
                         factor_type=study_info.experiment_type)
        FP = StudyFactor(name="Fault Position",
                         factor_type=study_info.experiment_type)
        FS = StudyFactor(name="Fault Severity",
                         factor_type=study_info.experiment_type)
        FT1 = FactorValue(factor_name=FT, value=study_info.fault_type)
        FP1 = FactorValue(factor_name=FP, value=study_info.fault_position)
        FS1 = FactorValue(factor_name=FS, value=study_info.fault_severity)
        sample.factor_values.append(FT1)
        sample.factor_values.append(FP1)
        sample.factor_values.append(FS1)

    # Create one sample per run
    study.samples = batch_create_materials(sample, n=len(study_info.runs))
    for i, run in enumerate(study_info.runs):
        study.samples[i].factor_values.extend(run.run_conditions)


def create_assay_data(assay_info: AssayInfo, sample: Sample, study: Study,
                      s_index: int, r_index: int, test_setup: TestSetUp):
    """
    Constructs assay data, including protocols, processes, and data files,
    and links them to the sample.
    """
    assay = Assay()
    # Sensor Details
    sensor = assay_info.used_sensor
    try:
        si = test_setup.sensors.index(sensor)
    except ValueError:
        si = -1
    assay.filename = (
        f"a_assay_st{s_index}_se{si}_"
        f"run{r_index}."
    ) + ("txt" if TXT_ending else "json")
    assay.measurement_type = OntologyAnnotation(sensor.measurement_type)
    assay.technology_type = OntologyAnnotation(sensor.technology_type)
    assay.technology_platform = sensor.technology_platform

    # Define protocol parameters
    SR = ProtocolParameter(parameter_name=OntologyAnnotation("sampling rate"))
    SL = ProtocolParameter(
        parameter_name=OntologyAnnotation("Sensor location"))
    SO = ProtocolParameter(
        parameter_name=OntologyAnnotation("Sensor orientation"))
    MU = ProtocolParameter(parameter_name=OntologyAnnotation("Measured unit"))
    DataAcquisition = ProtocolParameter(
        parameter_name=OntologyAnnotation("Data Acquisition Unit"))

    # Create parameter values, converting sampling_rate to float if possible
    try:
        sr_value = float(sensor.sampling_rate)
        SR_value = ParameterValue(category=SR, value=sr_value,
                                  unit=OntologyAnnotation(sensor.sampling_unit))
    except (ValueError, TypeError):
        print(f"Warning: Invalid sampling rate '{sensor.sampling_rate}' " +
              f"for sensor {sensor.identifier}. Using as string.")
        sr_value = sensor.sampling_rate
        SR_value = ParameterValue(category=SR, value=sr_value)
    SL_value = ParameterValue(category=SL, value=sensor.sensor_location)
    SO_value = ParameterValue(category=SO, value=sensor.sensor_orientation)
    MU_value = ParameterValue(category=MU, value=sensor.measurement_unit)
    DataAcquisition_value = ParameterValue(category=DataAcquisition,
                                           value=sensor.data_acquisition_unit)

    file_details = assay_info.file_details
    data_collection_protocol = Protocol(
        name="data collection",
        protocol_type="data collection",
        parameters=[SR, SL, SO, MU, DataAcquisition])

    study.protocols.append(data_collection_protocol)

    data_collection_process = Process(
        executes_protocol=data_collection_protocol,
        parameter_values=[SR_value, SL_value, SO_value,
                          MU_value, DataAcquisition_value]
    )

    # Add file details as comments for column headers
    datafile_raw = DataFile(
        filename=file_details.raw_file_name,
        label="Raw Data File",
        generated_from=[sample],
        comments=[Comment(name=f"Column Header {i+1}",
                          value=file_details.labels[i])
                  for i in range(file_details.number_of_columns)]
    )

    data_collection_process.inputs.append(sample)
    data_collection_process.outputs.append(datafile_raw)
    assay.process_sequence.append(data_collection_process)

    # Handle data transformation if file parameters are present
    if file_details.file_parameters:
        data_transformation_protocol = Protocol(
            name="data transformation",
            protocol_type="data transformation",
            parameters=[file_parameter.parameter for file_parameter
                        in file_details.file_parameters])
        study.protocols.append(data_transformation_protocol)

        data_transformation_process = Process(
            executes_protocol=data_transformation_protocol,
            parameter_values=[file_parameter.value for file_parameter in
                              file_details.file_parameters])
        data_transformation_process.inputs.append(
            data_collection_process.outputs[0])
        datafile = DataFile(
            filename=file_details.proccesed_file_name,
            label="Derived Array Data File",
            generated_from=[sample])
        data_transformation_process.outputs.append(datafile)
        plink(data_collection_process, data_transformation_process)
        assay.process_sequence.append(data_transformation_process)

    assay.samples.append(sample)

    return assay


def create_fault_preparation_protocol(study: Study, study_info: StudyInfo):
    """
    Adds a fault/degradation preparation protocol and process to the study.
    """
    # Define ProtocolParameters for factors
    f_params = []
    f_params_val = []
    if study_info.experiment_type == "Diagnostic":
        FT_param = ProtocolParameter(
            parameter_name=OntologyAnnotation(term="Fault Type"))
        FP_param = ProtocolParameter(
            parameter_name=OntologyAnnotation(term="Fault Position"))
        FS_param = ProtocolParameter(
            parameter_name=OntologyAnnotation(term="Fault Severity"))
        FT_value = ParameterValue(category=FT_param,
                                  value=study_info.fault_type)
        FP_value = ParameterValue(category=FP_param,
                                  value=study_info.fault_position)
        FS_value = ParameterValue(category=FS_param,
                                  value=study_info.fault_severity)
        f_params = [FT_param, FP_param, FS_param]
        f_params_val = [FT_value, FP_value, FS_value]

    experiment_preparation_protocol = Protocol(
        name=study_info.detail_preparation,
        protocol_type=OntologyAnnotation(term="Fault/degradation protocol"),
        parameters=f_params
    )

    study.protocols.append(experiment_preparation_protocol)

    # Define ParameterValues for the process
    experiment_preparation_process = Process(
        executes_protocol=experiment_preparation_protocol,
        parameter_values=f_params_val
    )

    for src in study.sources:
        experiment_preparation_process.inputs.append(src)
    for sam in study.samples:
        experiment_preparation_process.outputs.append(sam)

    study.process_sequence.append(experiment_preparation_process)


def create_study_data(study_info: StudyInfo, index: int):
    """
    Assembles all study-level metadata, including samples, factors,
    protocols, and assays.
    """
    study = Study(filename=f"s_study_s{index}.")
    study.filename += ("txt" if TXT_ending else "json")
    study.title = study_info.title
    study.identifier = f"s{index}"
    study.description = study_info.description
    study.submission_date = study_info.submission_date
    study.public_release_date = study_info.public_release_date
    # STUDY PUBLICATIONS
    if study_info.publication:
        study.publications.append(study_info.publication)
    # STUDY CONTACTS
    study.contacts = study_info.contacts
    # Experiment type
    study.design_descriptors.append(
        create_study_descriptor(study_info.experiment_type))
    # Study design
    add_test_setup(study, study_info)

    f_type = "Fault" if study_info.experiment_type == "Diagnostic" else "Load"
    study.add_factor(name="Fault Type", factor_type=f_type)
    study.add_factor(name="Fault Position", factor_type=f_type)
    study.add_factor(name="Fault Severity", factor_type=f_type)

    # Protocol reference
    create_fault_preparation_protocol(study, study_info)
    for r_i, run in enumerate(study_info.runs):
        for assay in run.assay_details:
            assay_data = create_assay_data(assay, study.samples[r_i], study,
                                           index, r_i, study_info.used_setup)
            study.assays.append(assay_data)

    return study


def create_isa_data(IsaPhmInfo: IsaPhmInfo):
    """
    Builds the full ISA investigation object from the IsaPhmInfo metadata.
    """
    investigation = Investigation()
    investigation.filename = "i_investigation." + \
                             ("txt" if TXT_ending else "json")
    investigation.identifier = IsaPhmInfo.identifier
    investigation.title = IsaPhmInfo.title
    investigation.description = IsaPhmInfo.description
    investigation.submission_date = IsaPhmInfo.submission_date
    investigation.public_release_date = IsaPhmInfo.public_release_date
    # INVESTIGATION PUBLICATIONS
    if IsaPhmInfo.publication:
        investigation.publications.append(IsaPhmInfo.publication)
    # INVESTIGATION CONTACTS
    investigation.contacts = IsaPhmInfo.contacts
    for i, study in enumerate(IsaPhmInfo.study_details):
        s = create_study_data(study, i)
        investigation.studies.append(s)
    return investigation


def main(args):
    """
    Entry point for the script. Parses arguments, loads metadata,
    generates ISA structures, and writes output files.
    """
    if args.tab:
        global TXT_ending
        TXT_ending = True
    info = create_info(args.file)
    inv_obj = create_isa_data(info)

    # if args.tab:
    #     write_study_table_files(inv_obj, "")
    #     write_assay_table_files(inv_obj, "", write_factor_values=False)
    #     isatab.dump(inv_obj, ".")
    if args.json:
        with open(inv_obj.filename, "w") as f:
            json.dump(inv_obj, f, cls=ISAJSONEncoder, sort_keys=True,
                      indent=4, separators=(',', ': '))
        for study in inv_obj.studies:
            with open(study.filename, "w") as f:
                json.dump(study, f, cls=ISAJSONEncoder, sort_keys=True,
                          indent=4, separators=(',', ': '))

            # Write each assay in the study as a separate JSON file
            for assay in study.assays:
                with open(assay.filename, "w") as f:
                    json.dump(assay, f, cls=ISAJSONEncoder, sort_keys=True,
                              indent=4, separators=(',', ': '))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--tab", action="store_true", default=False,
                        help="when this is flag is enabled outputs isa-tab" +
                        " format, default is False")
    parser.add_argument("-j", "--json", action="store_true", default=True,
                        help="when this is flag is enabled outputs" +
                        " isa-json format, default is True")
    parser.add_argument("file",
                        help="input a json file that contains the necessary" +
                        " information to create the isa-phm")
    args = parser.parse_args()
    main(args)

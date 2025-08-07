import uuid
from copy import deepcopy
from datetime import datetime
import json

# Global lists to store persons, publications, test setups, and sensor file details
PERSONS = []
PUBLICATIONS = []
TEST_SETUPS = []
SENSOR_TO_FILE_DETAIL = {}


def yes_no_question(to_ask):
    """
    Ask a yes/no question via input() and return the answer as True/False.
    Keeps prompting until a valid answer is given.
    """
    while True:
        answer = input(to_ask + " (y/n): ").strip().lower()
        if answer in ['y', 'yes']:
            return True
        elif answer in ['n', 'no']:
            return False
        else:
            print("Please respond with 'y' or 'n'.")


def get_date_question(to_ask):
    """
    Prompt the user for a date in ISO8601 format and validate the input.
    Keeps prompting until a valid date is entered.
    """
    while True:
        anwser = input(to_ask).strip().lower()
        try:
            # Try to parse the answer as an ISO8601 date
            datetime.fromisoformat(anwser)
            return anwser
        except ValueError:
            print("Please enter the date in ISO8601 format (e.g., 2023-06-01" +
                  " or 2023-06-01T12:00:00).")


def ask_for_publication_details():
    """
    Prompt the user for publication details, including title, DOI, status, author list, and comments.
    Returns a dictionary with the publication information.
    """
    tiltle = input("what is the title of the publication?:")
    doi = ""
    while True:
        doi = input("what is the DOI of the publication?:").strip()
        # Basic DOI validation: starts with "10." and contains a "/"
        if (doi.startswith("10.") and "/" in doi) or doi == "":
            break
        else:
            print("Please enter a valid DOI (e.g., 10.xxxx/xxxxx).")
    status = input("what is the status of the publication?:")
    author_list = input("what is the author list of the publication? " +
                        "(every name should be comma seperated):")

    publication = {
        "title": tiltle,
        "doi": doi,
        "status": status,
        "author_list": author_list,
        "comments": []
    }

    temp = 'a'
    # Allow adding multiple comments to the publication
    while yes_no_question(f"Do you want to add {temp} comment to " +
                          "the publication?"):
        name = input("what is the name of the comment?:")
        value = input("what is the value of the comment?:")
        comment = {
            "name": name,
            "value": value
        }
        temp = 'another'
        publication["comments"].append(comment)

    PUBLICATIONS.append(publication)
    return publication


def ask_for_contact_details():
    """
    Prompt the user for contact details, including name, email, phone, address, affiliation, roles, and comments.
    Returns a dictionary with the contact information.
    """
    first_name = input("What is the contact's first name?: ").strip()
    last_name = input("What is the contact's last name?: ").strip()
    mid_initials = input("What are the " +
                         "contact's middle initials (if any)?: ").strip()
    email = input("What is the contact's email address?: ").strip()
    phone = input("What is the contact's phone number?: ").strip()
    address = input("What is the contact's address?: ").strip()
    affiliation = input("What is the contact's affiliation?: ").strip()

    roles = []
    # Allow adding multiple roles
    while True:
        role = input("Enter a role for the contact" +
                     " (leave blank to finish): ").strip()
        if role:
            roles.append(role)
        else:
            break

    comments = []
    # Allow adding multiple comments
    while yes_no_question("Do you want to add a comment to the contact?"):
        name = input("What is the name of the comment?: ").strip()
        value = input("What is the value of the comment?: ").strip()
        comments.append({"name": name, "value": value})

    contact = {
        "first_name": first_name,
        "last_name": last_name,
        "mid_initials": mid_initials,
        "email": email,
        "phone": phone,
        "address": address,
        "affiliation": affiliation,
        "roles": roles,
        "comments": comments
    }

    PERSONS.append(contact)
    return contact


def describe_investigation():
    """
    Collects and returns the details of the investigation, including identifier, title, description,
    submission/public release dates, contacts, and optional publication.
    """
    i_identifier = "i-"+str(uuid.uuid4())
    i_title = input("what is the title of the investigation?:")
    i_description = input("what is the description of the investigation?:")
    i_submission_date = get_date_question("what is the submission date of " +
                                          "the investigation?:")
    i_public_release_date = get_date_question("what is the public release" +
                                              " date of the investigation?:")
    IsaPhmInfo = {"identifier": i_identifier,
                  "title": i_title,
                  "description": i_description,
                  "submission_date": i_submission_date,
                  "public_release_date": i_public_release_date,
                  "contacts": []
                  }

    # Allow changing the auto-generated identifier
    if yes_no_question("The current indentifier is " + i_identifier +
                       " do you want to change it?"):
        i_identifier = input("what is the identifier of the investigation?:")
        IsaPhmInfo["identifier"] = i_identifier

    # Optionally add a publication
    if yes_no_question("Do you want to add a publication?"):
        i_publication = ask_for_publication_details()
        IsaPhmInfo["publication"] = i_publication

    # Allow adding multiple contacts
    temp = "a"
    while yes_no_question(f"Do you want to add {temp} contact?"):
        i_contact = ask_for_contact_details()
        IsaPhmInfo["contacts"].append(i_contact)
        temp = "another"

    return IsaPhmInfo


def ask_for_characteristic():
    """
    Prompt the user for a characteristic's category, value, unit, and comments.
    Returns a dictionary with the characteristic information.
    """
    category = input("What is the category of the characteristic?: ").strip()
    value = input("What is the value/name of the characteristic?: ").strip()
    unit = input("What is the unit of the characteristic? " +
                 "(leave blank if none): ").strip()

    comments = []
    # Allow adding multiple comments
    while yes_no_question("Do you want to add a comment" +
                          " to the characteristic?"):
        name = input("What is the name of the comment?: ").strip()
        value = input("What is the value of the comment?: ").strip()
        comments.append({"name": name, "value": value})

    return {
        "category": category,
        "value": value,
        "unit": unit,
        "comments": comments
    }


def file_details():
    """
    Prompt the user for file details related to a sensor, including number of columns, labels,
    and optionally data processing steps.
    Returns a dictionary with the file details.
    """
    while True:
        try:
            num_columns = int(input("How many columns does the sensor" +
                                    " output have?: "))
            break
        except ValueError:
            print("Please enter a valid number.")
    labels = []
    for i in range(num_columns):
        label = input(f"What is the label of column {i + 1}?: ").strip()
        labels.append(label)

    f_parameters = []
    # Optionally add data processing steps
    if yes_no_question("Is the data of the sensor processed?"):
        description = input("what is the description how" +
                            " the data is processed?: ").strip()
        # Ask for associated data processing steps

        while yes_no_question("Do you want to add a data processing step" +
                              " (e.g., filter type, chunk size, etc.)?"):
            step_name = input("What is the name of the processing step (e.g." +
                              ", 'filter type', 'chunk size')?: ").strip()
            step_value = input("What is the " +
                               f"value for '{step_name}'?: ").strip()
            step_unit = input(f"What is the unit for '{step_name}' " +
                              "(leave blank if none)?: ").strip()
            file_parameter = {
                "parameter": {
                    "parameter_name": step_name,
                    "comments": []
                },
                "value": {
                    "value": step_value,
                    "unit": step_unit,
                    "comments": []
                }
            }
            f_parameters.append(file_parameter)
    return {
        "file_parameters": f_parameters,
        "number_of_columns": num_columns,
        "labels": labels
    }


def ask_for_sensor_details(index: int = 0, name: str = ""):
    """
    Prompt the user for sensor details, including measurement type, unit, description,
    technology, sampling, location, and orientation.
    Also collects file details for the sensor.
    Returns a dictionary with the sensor information.
    """
    identifier = f"Sensor_{index}_{name}"
    measurement_type = input("What is the measurement " +
                             "type of the sensor?: ").strip()
    measurement_unit = input("What is the measurement unit" +
                             " of the sensor?: ").strip()
    description = input("What is the description of the sensor?: ").strip()
    technology_type = input("What is the technology type" +
                            " of the sensor?: ").strip()
    technology_platform = input("What is the technology platform" +
                                " of the sensor?: ").strip()
    data_acquisition_unit = input("What is the data acquisition unit" +
                                  " of the sensor?: ").strip()
    sampling_rate = input("What is the sampling rate of the sensor?: ").strip()
    sampling_unit = input("What is the sampling unit of the sensor?: ").strip()
    sensor_location = input("What is the sensor location" +
                            " of the sensor?: ").strip()
    location_unit = input("What is the location unit of the sensor?: ").strip()
    sensor_orientation = input("What is the sensor orientation" +
                               " of the sensor?: ").strip()
    orientation_unit = input("What is the orientation unit" +
                             " of the sensor?: ").strip()

    sensor = {
        "identifier": identifier,
        "measurement_type": measurement_type,
        "measurement_unit": measurement_unit,
        "description": description,
        "technology_type": technology_type,
        "technology_platform": technology_platform,
        "data_acquisition_unit": data_acquisition_unit,
        "sampling_rate": sampling_rate,
        "sensor_location": sensor_location,
        "sensor_orientation": sensor_orientation,
    }

    # Add optional units if provided
    if sampling_unit != "":
        sensor["sampling_unit"] = sampling_unit
    elif location_unit != "":
        sensor["location_unit"] = location_unit
    elif orientation_unit != "":
        sensor["orientation_unit"] = orientation_unit

    # Store file details for this sensor
    SENSOR_TO_FILE_DETAIL[identifier] = file_details()

    return sensor


def describe_test_setup():
    """
    Prompt the user for test setup details, including name, location, characteristics, and sensors.
    Appends the setup to the TEST_SETUPS list.
    """
    name = input("What is the name of the test setup?: ").strip()
    location = input("What is the location of the test setup?: ").strip()

    characteristics = []
    temp = "a"
    # Allow adding multiple characteristics
    while yes_no_question(f"Do you want to add {temp}" +
                          " characteristic to the test setup?"):
        temp = "another"
        characteristic = ask_for_characteristic()
        characteristics.append(characteristic)

    sensors = []
    temp = "a"
    index = 0
    # Allow adding multiple sensors
    while yes_no_question(f"Do you want to add {temp} sensor?"):
        temp = "another"
        sensor = ask_for_sensor_details(index, name)
        index += 1
        sensors.append(sensor)

    TEST_SETUPS.append({
        "name": name,
        "location": location,
        "characteristics": characteristics,
        "number_of_sensors": len(sensors),
        "sensors": sensors
    })


def get_contacts_for_experiment():
    """
    Display the list of contacts and allow the user to confirm or add more.
    Returns the list of contacts for the experiment.
    """
    print("is this the list of the contacts for the experiment?")
    for i, contact in enumerate(PERSONS):
        print(f"\t{i + 1}. {contact['first_name']} {contact['last_name']}")
    if yes_no_question(""):
        return PERSONS
    else:
        while yes_no_question("Do you want to add a contact?"):
            contact = ask_for_contact_details()
            PERSONS.append(contact)


def get_test_setup_for_experiment():
    """
    Allow the user to select a test setup from the available setups.
    Returns the selected test setup.
    """
    used_setup = None
    if len(TEST_SETUPS) == 1:
        used_setup = TEST_SETUPS[0]
    else:
        print("Please select a test setup from the following list:")
        for i, setup in enumerate(TEST_SETUPS):
            print(f"{i + 1}. {setup['name']}")

        while True:
            try:
                choice = int(input("Enter the number of the" +
                                   " test setup you want to use: "))
                if 1 <= choice <= len(TEST_SETUPS):
                    used_setup = TEST_SETUPS[choice - 1]
                    break
                else:
                    print("Invalid choice. Please try again.")
            except ValueError:
                print("Invalid input. Please enter a number.")
    return used_setup


def get_experiment_type():
    """
    Prompt the user to select the experiment type from a predefined list.
    Returns the selected experiment type.
    """
    print("Select the experiment type:")
    experiment_types = {"Diagnostic": "Diagnostic",
                        "Constant degradation": "Degradation-c",
                        "Time varying degradation": "Degradation-tv"}
    keys = list(experiment_types.keys())
    for i, key in enumerate(keys):
        print(f"{i + 1}. {key}")
    while True:
        try:
            exp_type_choice = int(input("Enter the number" +
                                        " of the experiment type: "))
            if 1 <= exp_type_choice <= len(keys):
                chosen_key = keys[exp_type_choice - 1]
                return experiment_types[chosen_key]
            else:
                print("Invalid choice. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a number.")


def get_fault_info():
    """
    Prompt the user for fault type, position, and severity.
    Returns a tuple with the fault information.
    """
    fault_type = input("What is the fault type?: ").strip().lower()
    fault_position = input("What is the fault position?: ").strip()
    fault_severity = input("What is the fault severity?: ").strip()
    return fault_type, fault_position, fault_severity


def ask_for_operating_condition():
    """
    Prompt the user for an operating condition's name, value, and unit.
    Returns a dictionary with the operating condition information.
    """
    name = input("What is the name of the operating" +
                 " condition (indepent variable)?: ").strip()
    value = input("What is the value of the operating condition?: ").strip()
    unit = input("What is the unit of the operating condition" +
                 " (leave blank if none)?: ").strip()
    return {
        "name": name,
        "value": value,
        "unit": unit
    }


def ask_for_run(setup, operating_conditions):
    """
    Prompt the user for run details, including file details for each sensor in the setup.
    Returns a dictionary with the run conditions and assay details.
    """
    run_conditions = operating_conditions
    assay_details = []
    for i, sensor in enumerate(setup["sensors"]):
        a_file = deepcopy(SENSOR_TO_FILE_DETAIL[sensor["identifier"]])
        print("\nSensor details:")
        for key, value in sensor.items():
            if key != "identifier":
                print(f"  {key.replace('_', ' ').capitalize()}: {value}")
        print()
        raw_file_name = input("What is the raw file name?: ").strip()
        raw_file_location = input("What is the raw file location?: ").strip()
        a_file["raw_file_name"] = raw_file_name
        a_file["raw_file_location"] = raw_file_location
        if a_file["file_parameters"] != []:
            proccesed_file_name = input("What is the" +
                                        " processed file name?: ").strip()
            proccesed_file_location = input("What is the processed" +
                                            " file location?: ").strip()
            a_file["proccesed_file_name"] = proccesed_file_name
            a_file["proccesed_file_location"] = proccesed_file_location

        assay_details.append({
            "used_sensor": sensor,
            "file_details": a_file
        })

    return {
        "run_conditions": run_conditions,
        "assay_details": assay_details
    }


def describe_experiment():
    """
    Collects and returns the details of an experiment, including title, description, dates,
    preparation, contacts, experiment type, fault info, operating conditions, and runs.
    Optionally adds a publication.
    """
    if len(TEST_SETUPS) == 0:
        print("No test setup has been added yet.")
        return
    used_setup = get_test_setup_for_experiment()
    title = input("What is the title of the experiment?: ")
    description = input("What is the description of the experiment?: ")
    submission_date = get_date_question("What is the submission date of" +
                                        " the experiment?: ")
    public_release_date = get_date_question("What is the public release" +
                                            " date of the experiment?: ")
    detail_preparation = input("What is the detail preparation of " +
                               "the experiment?: ")
    s_publication = None
    if yes_no_question("Do you want to add a publication?"):
        s_publication = ask_for_publication_details()
    contacts = get_contacts_for_experiment()
    experiment_type = get_experiment_type()
    fault_type, fault_position, fault_severity = get_fault_info()
    operating_conditions = []
    temp = "an"
    # Allow adding multiple operating conditions
    while yes_no_question(f"Do you want to add {temp}" +
                          " operating condition to the test setup?"):
        temp = "another"
        operating_conditions.append(ask_for_operating_condition())
    temp = "a"
    runs = []
    # Allow adding multiple runs
    while yes_no_question(f"Do you want to add {temp} run?"):
        temp = "another"
        runs.append(ask_for_run(used_setup, operating_conditions))

    StudyInfo = {
        "title": title,
        "description": description,
        "submission_date": submission_date,
        "public_release_date": public_release_date,
        "detail_preparation": detail_preparation,
        "contacts": contacts,
        "experiment_type": experiment_type,
        "fault_type": fault_type,
        "fault_position": fault_position,
        "fault_severity": fault_severity,
        "runs": runs,
        "used_setup": used_setup
    }

    if s_publication:
        StudyInfo["publication"] = s_publication
    return StudyInfo


# Main script execution
print("### INVESTIGATON DETAILS ###\n")
isa_phm_info = describe_investigation()
print("\n### TEST SETUP DETAILS ###\n")
describe_test_setup()
while yes_no_question("Do you want to add another test setup?"):
    describe_test_setup()
print("\n### EXPERIMENT DETAILS ###\n")
study_objects = []
study_objects.append(describe_experiment())
while yes_no_question("Do you want to add another experiment?"):
    study_objects.append(describe_experiment())
print("### DONE ###")

# Add study details to the investigation info and save to JSON
isa_phm_info["study_details"] = study_objects

with open("experiment.json", "w") as f:
    json.dump(isa_phm_info, f, indent=4)

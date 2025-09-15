# -*- coding: utf-8 -*-
"""
Created on Fri Apr 18 08:53:14 2025

@author: KeizersLS
"""
import pandas as pd
import os
import isatools
from isatools.model import *
from isatools.isatab.dump.write import *
from isatools import isatab
import json
from isatools.isajson import ISAJSONEncoder
import shutil

#Get current directory
current_dir = os.getcwd()

#Get input file
input_file = str(current_dir+"\\Input_template.xlsx")

#Read the data 
df = pd.read_excel(input_file, sheet_name=None,header=None)  # Use row 2 (index 1) as the header



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
    



#The following part of the code maps given names in the input template to required input names for the isa script. 
#This enables more intuitive naming in the input file.
class Sheetnames:
    INVESTIGATIONDETAILS = 'Investigation details'
    SETUPDETAILS = 'Set-up details'
    STUDYDETAILS = 'Study details'
    TESTMATRIX = 'Test matrix'

column_mapping = {
    Sheetnames.INVESTIGATIONDETAILS: {
        'identifier':'identifier',
        'Description': 'description',
        'Submission date': 'submission date',
        'Public release date': 'public_release_date',
        'Comments':'comments',
        'Last name': 'last_name',
        'First name': 'first_name',
        'Mid initials': 'mid_initials',
        'Email': 'email',
        'Phone': 'phone',
        'Fax': 'fax',
        'Address': 'address',
        'Affiliation': 'affiliation',
        'Roles': 'roles',
        'Investigation Publication Title': 'title',
        'Authors': 'author_list',
        'DOI': 'doi',
        'Investigation Publication Status': 'status',
        },
 
    Sheetnames.SETUPDETAILS:{
            'Detail type': 'category',
            'Specification': 'value',
            'Unit':'unit'
            },
    Sheetnames.STUDYDETAILS:{
        'Identifier': 'identifier',
        'Title':'title',
        'Description':'description',
        'submission date':'submission_date',
        'Publication date':'public_release_date'
        } 
    }
# %%
###Input the general investigation details
###These details consist of 1) General info, 2) Contact details and 3) Publication details. The columns for this part are case-independent (i.e. no variation between cases))

#Read data and generate investigation
df_inv = pd.read_excel(input_file, sheet_name='Investigation details',header=1)                                  #Read data
df_inv = df_inv.rename(columns=column_mapping[Sheetnames.INVESTIGATIONDETAILS])                                  #Map given names -> isa names
df_inv = df_inv.dropna(axis=1,how='all')                                                                         #Drop empty columns
investigation = Investigation()                                                                                  #Create investigation

#GEneral info                                                                                 
general_columns = ['identifier', 'Title', 'description', 'submission data', 'public_release_date', 'comments']   #Columns referring to general information
for index, row in df_inv.iterrows():                                                                             #For every row
    for key in [col for col in general_columns if col in df_inv.columns]:                                        #For every column 
        value = row[key]                                                                                         #Obtain the data  
        if pd.notna(value):                                                                                      #If there's data
            setattr(investigation, key, str(value))                                                              #Add general info to the investigation                                                                            

#Contact details
contact_columns=['last_name','first_name','mid_initials','email','affiliation','roles']                          #Columns referring to contacts
for index, row in df_inv.iterrows():                                                                             #For every row
    contact = Person()                                                                                           #Create a contact 
    for key in [col for col in contact_columns if col in df_inv.columns]:                                        #For every column
        value = row[key]                                                                                         #Obtain the data
        if pd.notna(value):                                                                                      #If there's data (so there's a person defined)
            if key == 'roles':                                                                                   #If column is roles
                roles = [role.strip() for role in value.split(',')]                                              #Create a role for each column-separated role
                setattr(contact, key, [OntologyAnnotation(term=role) for role in roles])                         #Set roles to person
            else:
                setattr(contact, key, value)                                                                     #For other columns, just add their values
    investigation.contacts.append(contact)                                                                       #Add person to investigation

#Publication details
publication_columns = ['title','author_list','doi','status']                                                     #Columns referring to publication
pub = Publication()                                                                                              #Create a publication
for key in [col for col in publication_columns if col in df_inv.columns]:                                        #For every column
    value = df_inv.iloc[0][key]                                                                                  #Get the data
    if pd.notna(value):                                                                                          #If there are data defined 
        if key == 'author_list':                                                                                 #If authors defined
            authors = '; '.join(df_inv['author_list'].dropna().astype(str))                                      #Concatenate all rows' author_list values into a single string       
            setattr(pub, key, authors)
        elif key == 'status':                                                                                    #If status, add status as ontologyannotation 
            setattr(pub, key, OntologyAnnotation(value))
        else:                                                                                                    #For other columns, just add value 
            setattr(pub, key, value)
investigation.publications.append(pub)                                                                           #Add publication to investigation

# %% Add the set-up details
df_setup = pd.read_excel(input_file, sheet_name='Set-up details',header=0)                                       #Read the data
df_setup = df_setup.rename(columns=column_mapping[Sheetnames.SETUPDETAILS])                                      #Map given names -> isa names
df_setup = df_setup.dropna(axis=1,how='all')                                                                     #Drop empty columns

sourcename = df_setup[df_setup['category']=='Location or lab name']['value'].values[0]                           #Set the name of the test location / lab
source = Source(name=sourcename)                                                                                 #Create source
samplename = df_setup[df_setup['category']=='Set-up or test specimen name']['value'].values[0]                   #Set sample name
dummy_sample = Sample(name=samplename, derives_from=[source])                                                    #Create a dummy sample. If >1 sample, it can be used to assign multiple samples with the same characteristics
characteristic_category_list = []                                                                                #Prepare a list to store characteristics. This will be used to assign all characteristics at once to the studies.

# Process each row
for i, row in df_setup[3:].iterrows():                                                                           # The first three rows contain location/lab name, set-up/specimen name and name of exp. prep. protocol, already processed above.
    if 'category' in row and 'value' in row:                                                                     #If there's data
        cat = OntologyAnnotation(term=row['category'])                                                           #Create the category as ontologyannotation
        characteristic_category_list.append(cat)                                                                 #Add characteristic category to list
        char = Characteristic(                                                                                   #Create the characteristic by assigning the value to the category
            category=cat,
            value=row['value']
        )
        
        if 'unit' in row and pd.notna(row['unit']):                                                              #If there's a unit, add it 
            unit = get_or_create_unit(row['unit'])
            char.unit = unit
        dummy_sample.characteristics.append(char)                                                                #Add characteristic to dummy sample 
        
# %% Create the studies. First, the general info of the studies are defined. Then the test matrix is used to specify the test details

df_studydetails = pd.read_excel(input_file, sheet_name='Study details',header=0)                                 #Read study detail data
df_studydetails = df_studydetails.rename(columns=column_mapping[Sheetnames.STUDYDETAILS])                        #Map given names -> isa names   
df_studydetails= df_studydetails.dropna(axis=1,how='all')                                                        #Drop empty columns
df_testmatrix = pd.read_excel(input_file, sheet_name='Test matrix',header=0)                                     #Read the test matrix 

studies = {}                                                                                                     #Initialize studies
for i,row in df_studydetails.iterrows():                                                                         #For every study 
    study = Study()                                                                                              #Create study object
    for column in df_studydetails.columns[1:]:                                                                   #For every column (after identifier column)
        setattr(study,column,str(row[column]))                                                                   #Assign data to study
    study.sources.append(source)                                                                                 #Add source to study
    identifier = row['identifier']                                                                               #Define identifier 
    study.filename=identifier+'.txt'                                                                             #Set study name as identifier.txt
    studies[identifier] = study                                                                                  #Add study to study list 
    investigation.studies.append(study)                                                                          #Add study to investigation

study_factors = {}                                                                                               #Initialize study factor dict
 ###First create the study factors
for _,row in df_testmatrix.iterrows():                                                                           #For every row in the test matrix 
    factorname = row['Variable']                                                                                 #Find the study factor name 
    factortype = OntologyAnnotation(row['Variable type'])                                                        #Find the study factor type                                                                      
    study_factors[factorname] = StudyFactor(name=factorname, factor_type=factortype)                             #Create the study factor

###Second cretae the parameter values based on the created study factors
for study in studies:                                                                                            #For every study
    studies[study].samples.extend(batch_create_materials(dummy_sample, n=1))                                     #Create a sample (based on the predefined dummy sample with given characteristics)
    studies[study].factors.extend(list(study_factors.values()))                                                  #Set the study factors based on the predefined study factor dict 
    studies[study].characteristic_categories.extend(characteristic_category_list)                                #Add the characteristics categories to the study based on the predefined characteristics list (these need to be added explicitly)
    for factor in df_testmatrix.Variable:                                                                        #For every study factor
        factorvalue = df_testmatrix[df_testmatrix.Variable==factor][study].values[0]                             #Get the value
        factorunit = get_or_create_unit(df_testmatrix[df_testmatrix.Variable==factor]['Unit'].values[0])         #Get the unit
        
        for sample in studies[study].samples:                                                                    #For each sample in the study (which is only 1)
            FV = FactorValue(factor_name=study_factors[factor],value=factorvalue,unit=factorunit)                #set the factor value
            sample.factor_values.append(FV)                                                                      #add it to the sample


#%% Define the experiment preparation protocol
experiment_preparation_protocol = Protocol(name=df_setup[df_setup['category'] == 'Name of experiment preparation protocol']['value'].iloc[0],protocol_type="Experiment preparation") #Set the name of the experiment preparation protocol

for study in studies:                                                                             #For every study
    studies[study].protocols.append(experiment_preparation_protocol)                              #Add the experiment preparation protocol
    experiment_preparation_process = Process(executes_protocol=experiment_preparation_protocol)   #Create the experiment preparation process
    for src in studies[study].sources:                                                            #For every source (which is one) 
        experiment_preparation_process.inputs.append(src)                                         #Add the source as process input 
    for sam in studies[study].samples:                                                            #For every sample  (which is one)
        experiment_preparation_process.outputs.append(sam)                                        #Add the sample as process output
    studies[study].process_sequence.append(experiment_preparation_process)                        #Add the process to the process sequence
    
#%% Now setthe measurement details
protocol_parameters = {}                                                                          #Initialize a dict for protocol parameters 
protocols = {}                                                                                    #Initializea dict for the protocols 
df_measurement_details = pd.read_excel(input_file, sheet_name='Measurement Details',header=0)     #Read the data     
df_measurement_details.index=df_measurement_details.iloc[:,0]                                     #Set index
df_measurement_details = df_measurement_details.iloc[:,1:]                                        #Set header 

df_dict = pd.read_excel(input_file, sheet_name=None,header=None)                                  #Create a dict of all sheets. This is done to find all "protocols"
 
for df_name, df_data in df_dict.items():                                                          #Go through all sheets
    if df_name.lower().endswith("protocols"):                                                     #If it is a protocol
       
        df_protocol=df_data                                                                       #Get the data
        df_protocol.index = df_protocol.iloc[:,0]                                                 #Set index
        df_protocol.columns = df_protocol.iloc[0]                                                 #Set header
        df_protocol = df_protocol.iloc[1:, 1:]                                                    #Remove index and header from data (as they are already in header and index now)
        
        for sensor in df_measurement_details['Name']:                                                                           #For every sensor
            measurement_type = df_measurement_details[df_measurement_details['Name']==sensor]['Measurement type'].values[0]     #Get the measurement type
            protname = str(measurement_type+" "+df_name[: -len("protocols")].strip())                                           #Set the protocol name (as measurement type + protocol type (e.g. vibration measurement / current processing))
            findprot = df_protocol[sensor]                                                                                      #Find the column corresponding to the sensor from the df
     
            protocol_parameters_temp = {}                                                         #Initialize protocol parameters of the sensor 
            for protocol_parameter,data in df_protocol[1:].iterrows():                            #Loop over the protocol parameters
                parametername = protocol_parameter                                                #Select the parameter name
                
                protocol_parameters[parametername] = ProtocolParameter(parameter_name=parametername) #Create a protocol parameter
                if not pd.isna(data[sensor]):                                                        #If there is a value given in the cell (not all rows may have data for all sensors)
                    protocol_parameters_temp[protocol_parameter] = protocol_parameters[parametername] #Include the protocol parameter in the sensor-specific protocol parameter
            
            protocol = Protocol(name=protname,protocol_type=df_name[: -len("protocols")].strip(),parameters=list(protocol_parameters_temp.values())) #Create the protocol with the protocol parameters
            protocols[protname] = protocol                                                        #Add the protocol to the list of protocols   
            for study in studies:                                                                 #Add the protocol to all studies
                studies[study].protocols.append(protocol)
  
#%% Create the assays
                                                                                     
df_assays = df['Assays']                                                       #df with assay names
df_assays.index=df_assays.iloc[:,0]                                            #Set index
df_assays.columns=df_assays.iloc[0]                                            #Seat header
df_assays = df_assays.iloc[1:,1:]                                              #Remove index and header from df (as they are already included in the index and header)

sensors = df_protocol.columns.unique().dropna()                                #Get all sensor names
 
###This try-excepts are there with the idea that there might not always be both a measurement- and processing protocol. For now it is.
# They read the data, set indices, set headers and remove the indices and headers from the df itself.
try:
    df_raw = df['Measurement output']
    df_raw.index=df_raw.iloc[:,0]
    df_raw.columns=df_raw.iloc[0]
    df_raw = df_raw.iloc[1:,1:]
except:
    df_raw = None
    
try:
    df_processed = df['Processing output']
    df_processed.index=df_processed.iloc[:,0]
    df_processed.columns=df_processed.iloc[0]
    df_processed = df_processed.iloc[1:,1:]
except:
    df_processed = None



for study in studies:                                                          # For each study
    for sensor in sensors:                                                     #For each sensor
        assay = Assay(filename=df_assays.loc[study, sensor] + '.txt')          #Select the corresponding assay
        measurement_outputs = {}                                               #Initialize measurement outputs dict 
        for sample in studies[study].samples:                                  #For each sample (which is only one)
            assay.samples.append(sample)                                       #Add the sample to the assay
            
            ###Loop through the protocols to assign inputs, outputs and parameter values to them
            
            for df_name, df_data in df_dict.items():                           # For each sheet
                if df_name.lower().endswith("protocols"):                      # If it describes a protocol
                    df_protocol = df_data                                      # Select the sheet
                    df_protocol.index = df_protocol.iloc[:, 0]                 # Set index
                    df_protocol.columns = df_protocol.iloc[0]                  # Set columns
                    df_protocol = df_protocol.iloc[1:, 1:]                     # Remove index and col from rest of df

                    ### Determine process input and outputs of the protocols and add them to the assays
                    
                    if "Measurement" in df_name:                               #If it is the measurement protocol
                        datafile_name = df_raw.loc[study, sensor]              #Get the datafile from the raw data df
                        if pd.isna(datafile_name):                             #If there is no datafile specified   
                            datafile_name = ""                                 #It will be stored as a nameless df (without datafile specified, no input for the processing can be defined)
                        else:                                                  #If there is a datafile specified
                            datafile_name = datafile_name                      #Obtain its name from the df
                        process_output = DataFile(datafile_name, label="Raw Data File", generated_from=sample) #Create the datafile object
                        process_input = sample                                 #The input of the measurement protocol is the sample
                        measurement_outputs[sample] = process_output           #store the raw datafile in a list, such that it can act as input for the processing protocol
                        assay.data_files.append(process_output)                #Add the datafile to the assay
                        
                    elif "Processing" in df_name:                              #If it is the processing protocol
                        process_input = measurement_outputs[sample]            #Obtain the input from the measurement outputs 
                        datafile_name = df_processed.loc[study, sensor]        #Retrieve the datafile name  
                        if pd.isna(datafile_name):                             #If there is no datafile name 
                            datafile_name = " "                                #It will be stored as a nameless df 
                        else:                                                  #If there is a datafile specified
                            datafile_name = datafile_name                      #Obtain its name from the df 
                        
                        process_output = DataFile(datafile_name, label="Processed Data File", generated_from=sample) #Create the datafile object
                        assay.data_files.append(process_output)                #Add the output to the assay


                    # Prepare parameter values
                    protocol_parameters_temp = {}                              #Create a dict for protocol parameters
                    parameter_values_temp_list = []                            #Create a list for parameter values
                    protname = str(df_measurement_details[df_measurement_details.Name==sensor]['Measurement type'].values[0]+" "+df_name[: -len("protocols")].strip()) #Obtain the protocol name
                    protocol = protocols[protname]                             #And use the protocol name to find the (formerly created) protocol from the protocol list

                    for protocol_parameter, data in df_protocol[1:].iterrows():#For each protocol parameter
                        parvalue = data[sensor]                                #Obtain its value from the corresponding column in the df   
                        if pd.notna(parvalue):                                 #If there is a value specified for the sensor 
                            protocol_parameters_temp[protocol_parameter] = protocol_parameters[protocol_parameter] #Add the protocol parameter to the temporarily protocol parameter list
                            unit = df_protocol.loc[protocol_parameter][1]      #Get the unit                
                            parametervalue = ParameterValue(
                            category=protocol_parameters[protocol_parameter],
                            value=parvalue,
                            unit=get_or_create_unit(unit)
                            )
                            
                            parameter_values_temp_list.append(parametervalue)  #Add the parameter value to the parameter value list 
                    process = Process(executes_protocol=protocol, parameter_values=parameter_values_temp_list) #Create the process with the created protocol and temporarily parameter value list
                    process.inputs.append(process_input)                       #Add the (formerly created) process input of the process
                    process.outputs.append(process_output)                     #Add the (formerly created) process output to the process
                    assay.process_sequence.append(process)                     #Add the process to the process sequence
                    
            ###If both a measurement and a processing protocol are defined, link them below. Note that it now only works for measurement and processing. Potentially should be upgraded to allow for more protocols.        
            if len(assay.process_sequence)>1:                                  #If >1 protocol (i.e. measurement AND processing) 
                for i in range(len(assay.process_sequence) - 1):                    
                    process1 = assay.process_sequence[i]                       #Get the processes
                    process2 = assay.process_sequence[i + 1]
                    plink(process1, process2)                                  #And link them

        ###Specify some measurement details
        assay.measurement_type = OntologyAnnotation(df_measurement_details[df_measurement_details['Name']==sensor]['Measurement type'].values[0])
        assay.technology_type = OntologyAnnotation(df_measurement_details[df_measurement_details['Name']==sensor]['Sensor type'].values[0])
        assay.technology_platform = df_measurement_details[df_measurement_details['Name']==sensor]['Sensor model'].values[0]
        
        ###Add the assay to the study
        studies[study].assays.append(assay)

###Each unit needs to be added to each study and assay explicitely.
for study in studies:
    studies[study].units.extend(unit_list)
    for assay in studies[study].assays:
        assay.units.extend(unit_list)

###Write the investigation, isa-tab- and isa-json files
csv_directory = os.getcwd()                                                    #Get directory

###Write isa-json
inv_dict = investigation.to_dict()                                             #Create a dict of the investigation          
with open('ISA_output_isajson.json', 'w', encoding='utf-8') as f:
    json.dump(
        investigation,
        f,
        cls=ISAJSONEncoder,
        sort_keys=True,
        indent=4,
        separators=(',', ': ')
    )

###Write isa-tab

#Create a path for the directory for studies- and assays
studies_dir = os.path.join(current_dir, "Studies")
assays_dir = os.path.join(current_dir, "Assays")

# create the folders if they don't exist
os.makedirs(studies_dir, exist_ok=True)
os.makedirs(assays_dir, exist_ok=True)

###Write study and assay txt files
write_study_table_files(investigation, studies_dir) 
write_assay_table_files(investigation,  assays_dir,write_factor_values=False)

###Write investigation txt file
isatab.dump(investigation,os.getcwd())

# Output ISA-tab Excel file
output_excel = 'ISA_Output_isatab.xlsx'


with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
    # Iterate over all CSV files in the directory
    
    ###First write studies and move study files to study folder
    for filename in os.listdir(csv_directory):
        if  (filename.startswith('s')) &   (filename.endswith('.txt')):
            # Full path to the CSV file
            src = os.path.join(csv_directory, filename)
            dst = os.path.join(studies_dir, filename)
             
            # Read the CSV file into a DataFrame
            df = pd.read_csv(src,sep='\t',encoding='Windows-1252')

            # Use the file name (without extension) as the sheet name
            sheet_name = os.path.splitext(filename)[0]

            # Write the DataFrame to a sheet in the Excel file
            df.to_excel(writer, index=False, sheet_name=sheet_name)
            
            shutil.move(src, dst)
            
        ###Ten write assays and move assay files to assay folder
        elif  (filename.startswith('a')) & (filename.endswith('.txt')):
            # Full path to the CSV file
            src = os.path.join(csv_directory, filename)
            dst = os.path.join(assays_dir, filename)
             
            # Read the CSV file into a DataFrame
            df = pd.read_csv(src,sep='\t',encoding='Windows-1252')

            # Use the file name (without extension) as the sheet name
            sheet_name = os.path.splitext(filename)[0]

            # Write the DataFrame to a sheet in the Excel file
            df.to_excel(writer, index=False, sheet_name=sheet_name)
            shutil.move(src, dst)

print(f"Excel file created: {output_excel}")






        
        
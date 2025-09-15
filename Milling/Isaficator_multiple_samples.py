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
input_file = str(current_dir+"\\Input_template_multiple_samples.xlsm")

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
        
#%%
df_studydetails = pd.read_excel(input_file, sheet_name='Study details',header=0)                                 #Read study detail data
df_studydetails = df_studydetails.rename(columns=column_mapping[Sheetnames.STUDYDETAILS])                        #Map given names -> isa names 
df_studydetails= df_studydetails.dropna(axis=1,how='all')                                                        #Drop empty columns 
df_testmatrix = pd.read_excel(input_file, sheet_name='Test matrix',header=None)                                  #Read test matrix

studies = {}                                                                   #Initialize studies
for i,row in df_studydetails.iterrows():                                       #For every study
    study = Study()                                                            #Create a study object
    for column in df_studydetails.columns[1:-1]:                               #For every column (except the 'number of runs')
        setattr(study,column,str(row[column]))                                 #Set the attribute
    study.sources.append(source)                                               #Add source to study 
    identifier = row['identifier']                                             #Select identifier
    study.filename=identifier+'.txt'                                           #Study name is identifier.txt
    studies[identifier] = study                                                #Add study to list of studies
    investigation.studies.append(study)                                        #Add study to investigation

###Create the experiment preparation protocol and process based on the data given in the df    
experiment_preparation_protocol = Protocol(name=df_setup[df_setup['category'] == 'Name of experiment preparation protocol']['value'].iloc[0],protocol_type="Experiment preparation")
experiment_preparation_process = Process(executes_protocol=experiment_preparation_protocol)    

study_factors = {}                                                             #Initialize dict of study factors

for _,row in df_testmatrix[2:].iterrows():                                     #For each row in the testmatrix (skip the headers)
    factorname = row[0]                                                        #Select the factor name
    factortype = row[1]                                                        #And the factor type
    unit =get_or_create_unit(row[2])                                           #And the factor unit
    study_factors[factorname] = StudyFactor(name=factorname, factor_type=OntologyAnnotation(factortype)) #Create the study factor 
    
for study in studies:                                                          #For every study 
    num_runs =  df_studydetails.loc[df_studydetails["identifier"] == study]['Number of runs'].values[0] #Find the number of runs (=number of samples)
    studies[study].samples = batch_create_materials(dummy_sample, n=num_runs)  #Create #N_runs samples
    studies[study].factors.extend(list(study_factors.values()))                #Add study factors to the study
    studies[study].characteristic_categories.extend(characteristic_category_list) #Add characteristic categories to the study
    
    for factor in df_testmatrix.iloc[2:,0]:                                    #For each study factor (skip headers)
        for i,sample in enumerate(studies[study].samples):                     #For each sample 
            run_name = f"Run {i+1:02d}"                                        #Create name of the run

            #Find the column index matching both study and run_name (study is in first row, run_name is in the second row)
            col_idx = df_testmatrix.columns[
                (df_testmatrix.iloc[0] == study) &
                (df_testmatrix.iloc[1] == run_name)
            ][0]
            
            # Now get the factor value of the corresponding run
            factorvalue = df_testmatrix.loc[df_testmatrix.iloc[:, 0] == factor, col_idx].values[0] #Find the value
            factorunit = get_or_create_unit(df_testmatrix[df_testmatrix.iloc[:,0]==factor][2].values[0]) #Find the unit (NaN if not specified)
            FV = FactorValue(factor_name=study_factors[factor],value=factorvalue,unit=factorunit) #Define the factor value with the predefined study factor and the retrieved values
            sample.factor_values.append(FV)                                    #Add factor values to the sample

#%% Create experiment preparation protocol 
experiment_preparation_protocol = Protocol(name=df_setup[df_setup['category'] == 'Name of experiment preparation protocol']['value'].iloc[0],protocol_type="Experiment preparation")


for study in studies:                                                          #Create the protocol for each study
  studies[study].protocols.append(experiment_preparation_protocol)             #Add the protocol to each study
  experiment_preparation_process = Process(executes_protocol=experiment_preparation_protocol) #Create the process
  for src in studies[study].sources:                                           #Add the source as the input
      experiment_preparation_process.inputs.append(src)
  for sam in studies[study].samples:                                           #Add each run to the outputs
      experiment_preparation_process.outputs.append(sam)
  studies[study].process_sequence.append(experiment_preparation_process)       #Add the experiment preparation protocol to the process sequence          

#%% Assign the protocol parameters
protocol_parameters = {}                                                       #Initialize protocol parameter dict
protocols = {}                                                                 #Initialize protocols
df_measurement_details = pd.read_excel(input_file, sheet_name='Measurement Details',header=0) #Get measurement details data
df_measurement_details.index=df_measurement_details.iloc[:,0]                  #Set index
df_measurement_details = df_measurement_details.iloc[:,1:]                     #Remove index from df (as it is now in the index)

df_dict = pd.read_excel(input_file, sheet_name=None,header=None)               #Create a dict of all sheets. This is done to find all "protocols"

for df_name, df_data in df_dict.items():                                       #Go through all sheets
    if df_name.lower().endswith("protocols"):                                  #If it is a protocol 
        df_protocol=df_data                                                    #Retrieve its data
        df_protocol.index = df_protocol.iloc[:,0]                              #Set index
        df_protocol.columns = df_protocol.iloc[0]                              #Set header
        df_protocol = df_protocol.iloc[1:, 1:]                                 #Remove index and header from df (as they are now in the index and header) 
        
        for sensor in df_measurement_details['Name']:                          #For each sensor 
            measurement_type = df_measurement_details[df_measurement_details['Name']==sensor]['Measurement type'].values[0] #Retrieve measurement type
            protname = str(measurement_type+" "+df_name[: -len("protocols")].strip()) #Create a protocol for the measurement ype and protocol type (e.g. vibration data measurement / current processing)
            findprot = df_protocol[sensor]                                     #Find protocol corresponding to sensor 
            protocol_parameters_temp = {}                                      #Initialize protocol parameters  
            for protocol_parameter,data in df_protocol[1:].iterrows():         #For each protocol parameter
                parametername = protocol_parameter                             #Retrieve its name
                
                protocol_parameters[parametername] = ProtocolParameter(parameter_name=parametername) #Create the protocol parameter
                if not pd.isna(data[sensor]):                                  #If the protocol parameter is specified for this sensor
                    protocol_parameters_temp[protocol_parameter] = protocol_parameters[parametername] #Add the protocol parameter to assign a value to it below
            
            protocol = Protocol(name=protname,protocol_type=df_name[: -len("protocols")].strip(),parameters=list(protocol_parameters_temp.values())) #Create the protocol
            protocols[protname] = protocol                                     #Add the prtoocol to the protocol list
            for study in studies:                                              #For each study  
                studies[study].protocols.append(protocol)                      #Add the protocol 
#%% Create assays                                                                                       
df_assays = df['Assays']                                                       #Read df with assay names
df_assays.index=df_assays.iloc[:,0]                                            #Set index
df_assays.columns=df_assays.iloc[0]                                            #Set columns
df_assays = df_assays.iloc[1:,1:]                                              #Remove index and header from df (as they are now in columns and header) 

sensors = df_protocol.columns.unique().dropna()                                #List all sensors
 
df_raw = df['Measurement output']                                              #Names of raw datafiles                                           
df_raw.columns=df_raw.iloc[0]                                                  #Set header 
df_raw = df_raw.iloc[1:,:]                                                     #Remove header from df (as now specified in header)

df_processed = df['Processing output']                                         #Names of processed datafiles
df_processed.columns=df_processed.iloc[0]                                      #Set header
df_processed = df_processed.iloc[1:,:]                                         #Remove header from df (as now specified in header)

###Assign the parameter values to the assays
for study in studies:                                                          #For each study
    for sensor in sensors:                                                     #For each sensor
        assay = Assay(filename=df_assays.loc[study, sensor])                   #Create an assay with the name specified in the df
        
        ###Define some sensor information
        assay.measurement_type = OntologyAnnotation(df_measurement_details[df_measurement_details.Name==sensor]['Measurement type'].values[0])
        assay.technology_type = OntologyAnnotation(df_measurement_details[df_measurement_details.Name==sensor]['Sensor type'].values[0])
        assay.technology_platform = df_measurement_details[df_measurement_details.Name==sensor]['Sensor model'].values[0]
        
        
        studies[study].assays.append(assay)                                    #Add assay to study
        
        measurement_outputs = {}                                               #Initialize measurement outputs (this is done such that outputs can be retrieved as inputs for the processing protocol later on)
        
        for i,sample in enumerate(studies[study].samples):                     #Add each sample to the assay
            assay.samples.append(sample)
            
        for df_name, df_data in df_dict.items():                                # For each sheet
            if df_name.lower().endswith("protocols"):                           # If it describes a protocol
                df_protocol = df_data                                           # Select the sheet
                df_protocol.index = df_protocol.iloc[:, 0]                      # Set index
                df_protocol.columns = df_protocol.iloc[0]                       # Set columns
                df_protocol = df_protocol.iloc[1:, 1:]                          # Remove index and col from rest of df

    
                # Prepare parameter values
                protocol_parameters_temp = {}                                   #Initialize protocol parameters      
                parameter_values_temp_list = []                                 #Initialize list of parameter values 
                protname = str(df_measurement_details[df_measurement_details.Name==sensor]['Measurement type'].values[0]+" "+df_name[: -len("protocols")].strip()) #Find protocol name
                protocol = protocols[protname]                                 #Find protocol from previously created protocols

                for protocol_parameter, data in df_protocol[1:].iterrows():    #For each protocol parameter
                    parvalue = data[sensor]                                    #Find the value corresponding to the sensor 
                    if pd.notna(parvalue):                                     #If a value is specified
                        protocol_parameters_temp[protocol_parameter] = protocol_parameters[protocol_parameter] #Add it to the temporarily parameter dict
                        unit = get_or_create_unit(df_protocol.loc[protocol_parameter][1])                      #Get the unit
                        parametervalue = ParameterValue(                                                       #Create the parameter value
                        category=protocol_parameters[protocol_parameter],
                        value=parvalue,
                        unit=get_or_create_unit(unit)
                            )
                        parameter_values_temp_list.append(parametervalue)      #Add the parameter value to the temporarily parameter value list
                
                ###Create processes for each sample
                for i,sample in enumerate(assay.samples):                      #For each sample
                    run_name = f"Run {i+1:02d}"                                #Define the run name
                    row_idx = df_raw.index[                                    #Find the row corresponding to the study and the run (first column contains study, second column the run in the study)
                        (df_raw.iloc[:,0] == study) & 
                        (df_raw.iloc[:,1] == run_name)
                        ][0]
                    process = Process(executes_protocol=protocol, parameter_values=parameter_values_temp_list,name=f'process_{protocol.name}_{sensor}_{study}_{assay}')   #Create the process
                   
                    
    
                    # Determine process input and output based on protocol type
                    if "measurement" in df_name.lower():                       #If measurementp rotocol
                        datafile_name = df_raw.loc[row_idx, sensor]            #Get datafile corresponding to study/run and sensor for from raw datafile df
                        if pd.isna(datafile_name):                             #If not specified, it is included as datafile with no name (required to have input for processing protocol)
                            datafile_name = ""
                        else:                                                  #If specified, get its name from the df
                            datafile_name = datafile_name
                            
                        process_output = DataFile(datafile_name, label="Raw Data File", generated_from=sample) #Create the datafile object
                        process_input = sample                                 #Set sample as input
                        measurement_outputs[run_name] = process_output         #Store output (such that it can be retrieved by processing input)
                        assay.data_files.append(process_output)                #Add file to assay 
                        process.inputs.append(process_input)                   #Set process input  
                        process.outputs.append(process_output)                 #Set process output
                        
                    elif "processing" in df_name.lower():                      #If processing protocol
                        datafile_name = df_processed.loc[row_idx, sensor]      #Get name from processing output df
            
                        if pd.isna(datafile_name):                             #If not specified, it is stored as nameless datafile
                            datafile_name = ""
                        else:
                            datafile_name = datafile_name                      #Otherwise, give it the name retrieved from the df
                        process_output = DataFile(datafile_name, label="Processed Data File", generated_from=sample) #Create the datafile
                        assay.data_files.append(process_output)                #Add datafile to assay
                        process_input = measurement_outputs[run_name]          #Get the input created by the measurement protocol 
                        process.inputs.append(process_input)                   #Set process input
                        process.outputs.append(process_output)                 #Set processs output
                        
                    assay.process_sequence.append(process)                     #Add process to assay
                    
        for i in range(len(assay.samples)):                                    #Link measurement protocol to processing protocol
            process1 = assay.process_sequence[i]
            process2 = assay.process_sequence[i + len(assay.samples)]
            plink(process1, process2)
                
#Units need to be explicitely added to the studies and assays            
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




        
        
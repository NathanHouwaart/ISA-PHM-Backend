# -*- coding: utf-8 -*-
"""
Created on Fri Apr 18 08:53:14 2025

@author: KeizersLS
"""

import pandas as pd
import os
#from pronto import Ontology
from isatools.model import *
from isatools.isatab.dump.write import *
from isatools import isatab
import json
#import openpyxl

current_dir = os.getcwd()
input_file = str(current_dir+"\\ISA-PHM template_v3_Rev.D.xlsx")
#Read the data 
df = pd.read_excel(input_file, sheet_name=None,header=None)  # Use row 2 (index 1) as the header

###Input the general investigation details
# Step 1: Set the first row as the new header


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

df_inv = pd.read_excel(input_file, sheet_name='Investigation details',header=1)
df_inv = df_inv.rename(columns=column_mapping[Sheetnames.INVESTIGATIONDETAILS])
df_inv = df_inv.dropna(axis=1,how='all')
first_row = df_inv.iloc[0]
investigation = Investigation()
general_columns = ['identifier', 'Title', 'description', 'submission data', 'public_release_date', 'comments']
for index, row in df_inv.iterrows():
    for key in [col for col in general_columns if col in df_inv.columns]:
        value = row[key]
        if pd.notna(value):
            setattr(investigation, key, str(value))
        print(f"{key}: {value}")

contact_columns=['last_name','first_name','mid_initials','email','affiliation','roles']

for index, row in df_inv.iterrows():
    contact = Person()
    for key in [col for col in contact_columns if col in df_inv.columns]:
        value = row[key]
        if pd.notna(value):
            if key == 'roles':
                roles = [role.strip() for role in value.split(',')]
                setattr(contact, key, [OntologyAnnotation(term=role) for role in roles])
            else:
                
                setattr(contact, key, value)
    investigation.contacts.append(contact)

publication_columns = ['title','author_list','doi','status']

pub = Publication()
for key in [col for col in publication_columns if col in df_inv.columns]:
    value = df_inv.iloc[0][key]
    if pd.notna(value):
        if key == 'author_list':
            # Concatenate all rows' author_list values into a single string
            authors = '; '.join(df_inv['author_list'].dropna().astype(str))
            setattr(pub, key, authors)
        elif key == 'status':
            setattr(pub, key, OntologyAnnotation(value))
        else:
            setattr(pub, key, value)
investigation.publications.append(pub)

df_setup = pd.read_excel(input_file, sheet_name='Set-up details',header=0)
df_setup = df_setup.rename(columns=column_mapping[Sheetnames.SETUPDETAILS])
df_setup = df_setup.dropna(axis=1,how='all')
first_row = df_setup.iloc[0]

sourcename = df_setup[df_setup['category']=='Location or lab name']['value'].values[0]
source = Source(name=sourcename)
samplename = df_setup[df_setup['category']=='Set-up or test specimen name']['value'].values[0]
dummy_sample = Sample(name=samplename, derives_from=[source])
# Process each row
for i,row in df_setup[2:].iterrows():
    char = Characteristic()
    for column in df_setup.columns:
        if pd.notna(row[column]):
            setattr(char,column,row[column])
    dummy_sample.characteristics.append(char)


df_studydetails = pd.read_excel(input_file, sheet_name='Study details',header=0)
df_studydetails = df_studydetails.rename(columns=column_mapping[Sheetnames.STUDYDETAILS])
df_studydetails= df_studydetails.dropna(axis=1,how='all')

df_testmatrix = pd.read_excel(input_file, sheet_name='Test matrix',header=0)

studies = {}
for i,row in df_studydetails.iterrows():
    study = Study()
    for column in df_studydetails.columns[1:]:          
        setattr(study,column,str(row[column]))
    study.sources.append(source)
    identifier = row['identifier']
    study.filename=identifier+'.txt'
    studies[identifier] = study
    investigation.studies.append(study)
    
study_factors = {}

for _,row in df_testmatrix.iterrows():
    factorname = row['Variable']
    factortype = row['Variable type']
    unit = row['Unit']
    study_factors[factorname] = StudyFactor(name=factorname, factor_type=factortype)
    
for study in studies:   
    studies[study].samples = batch_create_materials(dummy_sample, n=1)
    for factor in df_testmatrix.Variable:
        factorvalue = df_testmatrix[df_testmatrix.Variable==factor][study].values[0]
        factorunit = OntologyAnnotation(df_testmatrix[df_testmatrix.Variable==factor]['Unit'].values[0])
        
        FV = FactorValue(factor_name=study_factors[factor],value=factorvalue,unit=factorunit)
        
        for sample in studies[study].samples:
            sample.factor_values.append(FV)


#Define the experiment preparation protocol
experiment_preparation_protocol = Protocol(name=df_setup[df_setup['category'] == 'Name of experiment preparation protocol']['value'].iloc[0],protocol_type="Experiment preparation")

for study in studies:
    studies[study].protocols.append(experiment_preparation_protocol)
    experiment_preparation_process = Process(executes_protocol=experiment_preparation_protocol)
    for src in studies[study].sources:
        experiment_preparation_process.inputs.append(src)
    for sam in studies[study].samples:
        experiment_preparation_process.outputs.append(sam)
    studies[study].process_sequence.append(experiment_preparation_process)
    

protocol_parameters = {}
protocols = {}
df_measurement_details = pd.read_excel(input_file, sheet_name='Measurement Details',header=0)
df_measurement_details.index=df_measurement_details.iloc[:,0]
df_measurement_details = df_measurement_details.iloc[:,1:]

df_dict = pd.read_excel(input_file, sheet_name=None,header=None)

for df_name, df_data in df_dict.items():
    if df_name.lower().endswith("protocols"):
       
        df_protocol=df_data
        df_protocol.index = df_protocol.iloc[:,0] 
        df_protocol.columns = df_protocol.iloc[0] 
        df_protocol = df_protocol.iloc[1:, 1:]
        
        for sensor in df_measurement_details.index:
            measurement_type = df_measurement_details.loc[sensor]['Measurement type']
            protname = str(measurement_type+" "+df_name[: -len("protocols")].strip())
            findprot = df_protocol[sensor]
     
            protocol_parameters_temp = {}
            for protocol_parameter,data in df_protocol[1:].iterrows():
                parametername = protocol_parameter
                
                protocol_parameters[parametername] = ProtocolParameter(parameter_name=parametername)
                if not pd.isna(data[sensor]):
                    protocol_parameters_temp[protocol_parameter] = protocol_parameters[parametername]
            
            protocol = Protocol(name=protname,protocol_type=df_name[: -len("protocols")].strip(),parameters=protocol_parameters_temp)
            protocols[protname] = protocol
            for study in studies:
                studies[study].protocols.append(protocol)
                                                                                       
df_assays = df['Assays']
df_assays.index=df_assays.iloc[:,1]
df_assays.columns=df_assays.iloc[0]
df_assays = df_assays.iloc[1:,2:]

# df_measurement_files = df['Measurement protocols output']
# df_measurement_files.index=df_measurement_files.iloc[:,0]
# df_measurement_files.columns=df_measurement_files.iloc[0]
# df_measurement_files = df_measurement_files.iloc[1:,1:]

# df_processed_files = df_processed_files['Processing protocols output']
# df_processed_files.index=df_processed_files.iloc[:,0]
# df_processed_files.columns=df_processed_files.iloc[0]
# df_processed_files = df_processed_files.iloc[1:,1:]

sensors = df_protocol.columns.unique().dropna()      
 
df_raw = df['Measurement output']
df_raw.index=df_raw.iloc[:,0]
df_raw.columns=df_raw.iloc[0]
df_raw = df_raw.iloc[1:,1:]

df_processed = df['Processing output']
df_processed.index=df_processed.iloc[:,0]
df_processed.columns=df_processed.iloc[0]
df_processed = df_processed.iloc[1:,1:]



for study in studies:                                                            # For each study
    for sensor in sensors:                                                      # For each sensor
        assay = Assay(filename=df_assays.loc[study, sensor] + '.txt')           # Select the corresponding assay
        
        for sample in studies[study].samples:
            assay.samples.append(sample) 
            for df_name, df_data in df_dict.items():                                # For each sheet
                if df_name.lower().endswith("protocols"):                           # If it describes a protocol
                    df_protocol = df_data                                           # Select the sheet
                    df_protocol.index = df_protocol.iloc[:, 0]                      # Set index
                    df_protocol.columns = df_protocol.iloc[0]                       # Set columns
                    df_protocol = df_protocol.iloc[1:, 1:]                          # Remove index and col from rest of df

                    # Determine process input and output based on protocol type
                    if "Measurement" in df_name:
                        process_output = DataFile(df_raw.loc[study, sensor], label="Raw Data File", generated_from=sample)
                        process_input = sample
                    elif "Processing" in df_name:
                        process_input = DataFile(df_raw.loc[study, sensor], label="Raw Data File", generated_from=sample)
                        process_output = DataFile(df_processed.loc[study, sensor], label="Processed Data File", generated_from=sample)

                    # Append input/output data files if applicable
                    if isinstance(process_input, DataFile):
                        assay.data_files.append(process_input)
                    if isinstance(process_output, DataFile):
                        assay.data_files.append(process_output)

                    # Prepare parameter values
                    protocol_parameters_temp = {}
                    parameter_values_temp = {}
                    parameter_values_temp_list = []
                    protname = str(df_measurement_details.loc[sensor]['Measurement type']+" "+df_name[: -len("protocols")].strip())
                    protocol = protocols[protname]

                    for protocol_parameter, data in df_protocol[2:].iterrows():  # For each protocol parameter
                        parvalue = data[sensor]
                        if pd.notna(parvalue):
                            protocol_parameters_temp[protocol_parameter] = protocol_parameters[protocol_parameter]
                            unit = df_protocol.loc[protocol_parameter][1]

                            if pd.notna(unit):
                                parametervalue = ParameterValue(
                                category=protocol_parameters[protocol_parameter],
                                value=parvalue,
                                unit=OntologyAnnotation(unit)
                                )
                            else:
                                parametervalue = ParameterValue(
                                category=protocol_parameters[protocol_parameter],
                                value=parvalue
                                )
                            parameter_values_temp[protocol_parameter] = parametervalue
                            parameter_values_temp_list.append(parametervalue)
                    process = Process(executes_protocol=protocol, parameter_values=parameter_values_temp_list)
                    process.inputs.append(process_input)
                    process.outputs.append(process_output)
                    assay.process_sequence.append(process)
            for i in range(len(assay.process_sequence) - 1):
                process1 = assay.process_sequence[i]
                process2 = assay.process_sequence[i + 1]
                plink(process1, process2)
        #     for process in assay.process_sequence:
        #         plink(process,process)
        
        assay.measurement_type = OntologyAnnotation(df_measurement_details.loc[sensor]['Measurement type'])
        assay.technology_type = OntologyAnnotation(df_measurement_details.loc[sensor]['Sensor type'])
        assay.technology_platform = df_measurement_details.loc[sensor]['Sensor model']
        studies[study].assays.append(assay)

### TO DO: 
# MORE SAMPLES
csv_directory = os.getcwd()

   
#Write files
inv_obj = investigation

###Write isa-json
inv_dict = inv_obj.to_dict()
with open("ISA_Output_json.json", "w") as f:
    json.dump(inv_dict, f, indent=4)  # Writes JSON to the file 

###Write study and assay txt files
write_study_table_files(inv_obj, current_dir)  # ,write_factor_values=False)
write_assay_table_files(inv_obj,  current_dir,write_factor_values=False)#,write_factor_values=False)

###Write investigation txt file
isatab.dump(inv_obj,os.getcwd())

# Output ISA-tab Excel file
output_excel = 'ISA_Output_isatab.xlsx'

with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
    # Iterate over all CSV files in the directory
    for filename in os.listdir(csv_directory):

        if  ((filename.startswith('s')) | (filename.startswith('a'))) & (filename.endswith('.txt')):
            # Full path to the CSV file
            csv_path = os.path.join(csv_directory, filename)

            # Read the CSV file into a DataFrame
            df = pd.read_csv(csv_path,sep='\t',encoding='Windows-1252')

            # Use the file name (without extension) as the sheet name
            sheet_name = os.path.splitext(filename)[0]

            # Write the DataFrame to a sheet in the Excel file
            df.to_excel(writer, index=False, sheet_name=sheet_name)

print(f"Excel file created: {output_excel}")




        
        
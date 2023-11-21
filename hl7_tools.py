# Program to demonstrate parsing of hl7 file.
# The format used is VXU 2.5.1 which is a Vaccine Message
# Author: Sandeep Chintabathina
# Modified by Alex Onopa

# Libraries needed
import sys
import tqdm
import time
import hl7
import pandas as pd
import glob
import os
import shutil
import datetime
import multiprocessing

import config

loinc_lookup = pd.read_csv('./lookups/Loinc_Sarscov2_Export_20230920180638.csv')
covid_loinc_codes = list(loinc_lookup['LOINC_NUM'])

observed_loincs = []

race_vals = []
# Function to gather multiple race codes and place in dictionary
def get_race_codes(record, data):
    try:
        race_str = str(record.segment('PID')[10])
    except IndexError:
        data['patientRace'] = ''
        data['patientRaceCode'] = ''
        return data
    #if race_str == '2028-9^Asian^HL70005^^^^2.5.1~2076-8^Native Hawaiian or Other Pacific Islander^HL70005^^^^2.5.1':
    #    print(record.segment('PID'))
    race_vals.append(race_str)
    # Split repetitions
    tokens = race_str.split('~')
    # Process each race str
    for i in range(0, len(tokens)):
        if (len(tokens[i]) > 0):
            tokens2 = tokens[i].split('^')
            data['patientRaceCode'] = tokens2[0]
            if len(tokens2) > 1:
                data['patientRace'] = tokens2[1]
            else:
                data['patientRace'] = ''
            if i > 1:
                tokens2 = tokens[i].split('^')
                data['patientRaceCode' + str(i + 1)] = tokens2[0]
                if len(tokens2) > 1:
                    data['patientRace' + str(i + 1)] = tokens2[1]
                else:
                    data['patientRace' + str(i + 1)] = ''
    return data


# Function to gather phone and email data
def get_phone_email(record, data):
    try:
        phone_str = str(record.segment('PID')[13])
    except IndexError:
        data['patientTelephoneNumber'] = ''
        data['Notes'] = ''
        return data
    # Split repetitions
    tokens = phone_str.split('~')
    # Process phone numbers
    data['patientTelephoneNumber'] = ''
    data['Notes'] = ''
    if len(tokens) > 0:
        for i in range(0, len(tokens)):
            area_code = ph_num = ''
            if len(tokens[i]) > 0:
                tokens2 = tokens[i].split('^')
                if tokens2[2] == 'PH' or tokens2[2] == 'CP':
                    area_code = ph_num = ''
                    area_code = tokens2[5]
                    ph_num = tokens2[6]
                    if i == 0:
                        data['patientTelephoneNumber'] = area_code + ph_num
                    else:
                        data['patientTelephoneNumber' + str(i + 1)] = area_code + ph_num
                if tokens2[2] == 'Internet':
                    email = tokens2[3]
                    data['Notes'] = email
    return data
    '''
    #Less generic solution
    area_code=ph_num=''
    if record['PID.13.1.6']!='':
        area_code ='('+record['PID.13.1.6']+')'
        ph_num = record['PID.13.1.7']
    data['patient_phone_1'] = area_code+ph_num

    area_code=ph_num=''
    if record['PID.13.2.6']!='':
        area_code ='('+record['PID.13.2.6']+')'
        ph_num = record['PID.13.2.7']
    data['patient_phone_2'] = area_code+ph_num

    email=''
    if record['PID.13.3.4']!='':
        email = record['PID.13.3.4']
    data['patient_email'] = email
    return data
    '''


def try_get_value(record, index):
    try:
        val = record[index]
        return val
    except (KeyError, IndexError):
        return ''


# Function to create a dictionary from a hl7 container message
def create_dict_elr(record):
    data = {}
    # Pick fields of interest
    #data['sending_application'] = try_get_value(record, 'MSH.3.1') # don't think I need this
    #data['Lab Name'] = try_get_value(record, 'OBX.23.1')
    data['sendingFacility'] = try_get_value(record, 'MSH.4.1')
    data['receivingFacility'] = try_get_value(record, 'MSH.6.1')
    data['submission_datetime'] = try_get_value(record, 'MSH.7.1')
    data['message_type'] = try_get_value(record, 'MSH.9.1')
    data['message_control_id'] = try_get_value(record, 'MSH.10.1')
    data['version_id'] = try_get_value(record, 'MSH.12.1')
    data['patientId'] = try_get_value(record, 'PID.3.1')
    data['patient_id_type'] = try_get_value(record, 'PID.3.1.5')
    data['patientLastName'] = try_get_value(record, 'PID.5.1.1')
    data['patientMiddleName'] = try_get_value(record, 'PID.5.1.3')
    data['patientFirstName'] = try_get_value(record, 'PID.5.1.2')
    data['patientSuffix'] = try_get_value(record, 'PID.5.1.4')
    data['patientDOB'] = try_get_value(record, 'PID.7.1')
    data['patientGender'] = try_get_value(record, 'PID.8.1')
    # Get multiple race codes
    data = get_race_codes(record, data)
    data['patientEthnicityCode'] = try_get_value(record, 'PID.22.1.1')
    data['patientEthnicity'] = try_get_value(record, 'PID.22.1.2')
    data['patientAddressLine1'] = try_get_value(record, 'PID.11.1.1')
    data['patientAddressCity'] = try_get_value(record, 'PID.11.1.3')
    data['patientAddressZip'] = try_get_value(record, 'PID.11.1.5')
    data['PATIENTADDRESSCOUNTY'] = try_get_value(record, 'PID.11.1.9')
    data['patientAddressState'] = try_get_value(record, 'PID.11.1.4')
    data['patientAddressCountry'] = try_get_value(record, 'PID.11.1.6')
    # Get patient contact ph and email
    data = get_phone_email(record, data)
    data['specimenCollectionDate'] = try_get_value(record, 'SPM.17.1.1')
    data['specimenReceivedDate'] = try_get_value(record, 'SPM.18.1.1')
    data['placerOrder'] = try_get_value(record, 'SPM.2.1.2')
    data['specimenType'] = try_get_value(record, 'SPM.4.1.2')
    data['specimenSNOMEDCode'] = try_get_value(record, 'SPM.4.1.1')
    data['labResultDate'] = try_get_value(record, 'OBX.14.1') # TODO: this or OBR.22.1?
    data['resultedTest'] = try_get_value(record, 'OBX.3.1.2')
    data['resultedTest_LOINC'] = try_get_value(record, 'OBX.3.1.1')
    data['observation'] = try_get_value(record, 'OBX.5.1.2')
    data['resultSNOMEDCode'] = try_get_value(record, 'OBX.5.1.1')
    data['performingFacility'] = try_get_value(record, 'OBX.23.1')
    data['performingFacilityCLIA'] = try_get_value(record, 'OBX.23.1.10')
    data['performingFacilityAddress'] = try_get_value(record, 'OBX.24.1.1')
    data['performingFacility_City'] = try_get_value(record, 'OBX.24.1.3')
    data['performingFacility_State'] = try_get_value(record, 'OBX.24.1.4')
    data['performingFacility_Zip'] = try_get_value(record, 'OBX.24.1.5')
    data['performingFacility_CallBackNumber'] = try_get_value(record, 'ORC.23.1')  # TODO: check this works?
    data['providerLastName'] = try_get_value(record, 'OBR.16.1.2.1') # TODO: this or ORC.12?
    data['providerFirstName'] = try_get_value(record, 'OBR.16.1.3.1') # TODO: this or ORC.12?
    data['providerAddress'] = try_get_value(record, 'ORC.24.1.1') + try_get_value(record, 'ORC.24.1.2') # TODO: is this needed?
    data['Provider_City'] = try_get_value(record, 'ORC.24.1.3')
    data['Provider_State'] = try_get_value(record, 'ORC.24.1.4')
    data['Provider_Zip'] = try_get_value(record, 'ORC.24.1.5')
    data['Provider_CallBackNumber'] = try_get_value(record, 'OBR.17.1.6') + try_get_value(record, 'OBR.17.1.7') # TODO: This or ORC.23.1.6 + ORC.23.1.7?
    data['orderingFacility'] = try_get_value(record, 'ORC.21.1')
    data['orderingFacility_Address'] = try_get_value(record, 'ORC.22.1.1') + try_get_value(record, 'ORC.22.1.2')
    data['orderingFacility_City'] = try_get_value(record, 'ORC.22.1.3')
    data['orderingFacility_State'] = try_get_value(record, 'ORC.22.1.4')
    data['orderingFacility_Zip'] = try_get_value(record, 'ORC.22.1.5')
    data['orderingFacility_CallBackNumber'] = try_get_value(record, 'ORC.23.1.6') + try_get_value(record, 'ORC.23.1.7') # TODO: this or ORC.23.1.1, or ORC.14.7?

    obx = record['OBX']
    for seg in obx:
        try:
            loinc = seg[3][0][0][0]
            observed_loincs.append(loinc)
        except IndexError:
            print('uh oh')
            continue

    return data


def parse_hl7_hhie():
    #file_list = glob.glob('./data/raw/hl7/hhie/*.txt')
    #file_list = glob.glob('./data/raw/hl7/hhie/2023-09-18-20-59-24-0688.txt')
    # First token is empty string - so ignore it
    # Store records in a list
    records = []
    file_count = 0
    record_count = 0
    covid_count = 0

    hl7_files = glob.iglob('./data/raw/hl7/hhie/*.txt')
    print(f'parsing {len(hl7_files)} HHIE HL7s...')
    for i, filename in enumerate(hl7_files):
        #print(filename)
        with open(filename) as hl7_file:
            message = hl7_file.read()
            file_count = file_count + 1
            tokens = message.split('MSH|^~')
            if len(tokens) > 2:
                exit('more than 1 record in file ' + filename + ', you need to rewrite the HHIE parser to handle this')
            record = hl7.parse(message.replace('\n', '\r'))
            file_count = file_count + 1
            if file_count % 100 == 0:
                print(file_count)
            if config.covid_select:
                if record['OBX.3.1'] not in covid_loinc_codes:
                    continue
                covid_count = covid_count + 1
            # print(record['PID.5.1.2']+' '+record['PID.5.1.1'])
            # Unable to convert hl7 container object into a dictionary directly
            # So creating a custom dictionary instead
            some_dict = create_dict_elr(record)
            some_dict['filename'] = filename
            some_dict['Submission Date'] = datetime.datetime.fromtimestamp(os.path.getmtime(filename)).date()
            records.append(some_dict)

    print("parsed ", file_count, ' files')
    print("parsed ", record_count, ' records')
    print("selected ", covid_count, ' covid records')
    # Convert to a dataframe and output in tabular format
    df = pd.DataFrame(records)
    if config.covid_select:
        fname = 'hhie_tabular_output_covid'
    else:
        fname = 'hhie_tabular_output_all'
    df.to_csv(f'./data/processed/hl7/{fname}.csv', index=False, columns=records[0].keys())
    print('Done writing to file')
    #print(pd.value_counts(df['patient_race_name_1']))
    return df


def process_hl7_parallel(input_file):
    with open(input_file) as hl7_file:
        message = hl7_file.read()
        tokens = message.split('MSH|^~')
        if len(tokens) > 2:
            exit('more than 1 record in file ' + input_file + ', you need to rewrite the HHIE parser to handle this')
        record = hl7.parse(message.replace('\n', '\r'))

        if config.covid_select:
            if record['OBX.3.1'] not in covid_loinc_codes:
                return None

        # Unable to convert hl7 container object into a dictionary directly
        # So creating a custom dictionary instead
        some_dict = create_dict_elr(record)
        some_dict['filename'] = input_file
        some_dict['Submission Date'] = datetime.datetime.fromtimestamp(os.path.getmtime(input_file)).date()
        return some_dict


def parse_hl7_hhie_parallel():
    file_list = glob.glob('./data/raw/hl7/hhie/*.txt')
    cpu_count = multiprocessing.cpu_count()
    print(f'parsing {len(file_list)} files on {cpu_count} threads...')
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        data = pool.map(process_hl7_parallel, file_list)
    hl7_record_list = [d for d in data if d is not None]
    print(f'parsed {len(hl7_record_list)} covid records')
    pd.DataFrame(hl7_record_list).to_csv('./data/processed/hl7/hhie_tabular_output.csv')

def parse_hl7_aims():
    print('parsing AIMS HL7s...')
    file_list = glob.glob('./data/raw/hl7/aims/*.txt')
    #file_list = glob.glob('./data/raw/hl7/2023-09-14-14-07-18-0142.txt')
    # First token is empty string - so ignore it
    # Store records in a list
    records = []
    record_count = 0
    covid_count = 0
    for i, filename in enumerate(file_list):
        #print(filename)
        with open(filename) as hl7_file:
            message = hl7_file.read()
            tokens = message.split('MSH|^~\&|')
            for i in range(1, len(tokens)):
                message = 'MSH|^~\&|' + tokens[i]
                record = hl7.parse(message.replace('\n', '\r'))
                record_count = record_count + 1
                if config.covid_select:
                    if record['OBX.3.1'] not in covid_loinc_codes:
                        continue
                    covid_count = covid_count + 1
                # Unable to convert hl7 container object into a dictionary directly
                # So creating a custom dictionary instead
                some_dict = create_dict_elr(record)
                some_dict['filename'] = filename
                some_dict['Submission Date'] = datetime.datetime.fromtimestamp(os.path.getmtime(filename)).date()

                records.append(some_dict)
    print("parsed ", len(file_list), ' files')
    print('parsed ' + str(record_count) + ' aims records')
    print('selected ' + str(covid_count) + ' covid records')
    df = pd.DataFrame(records)
    df.to_csv('./data/processed/hl7/aims_tabular_output.csv', index=False, columns=records[0].keys())

    # Convert to a dataframe and output in tabular format
    df = pd.DataFrame(records)
    print('Done writing to file')
    #print(pd.value_counts(df['patient_race_name_1']))
    return df


def move_hl7_elr():
    aims_folder_input = '//10.164.29.15/MavenECR/AIMS_HL7s/'
    aims_folder_output = './data/raw/hl7/aims/'

    hhie_folder_input = '//10.164.29.15/MavenECR/HHIE_HL7s/'
    hhie_folder_output = './data/raw/hl7/hhie/'

    print('moving AIMS HL7s...')
    for f in tqdm.tqdm(glob.glob(aims_folder_input + '*.txt')):
        fname = os.path.basename(f)
        outpath = aims_folder_output + fname
        if not os.path.exists(outpath):
            shutil.copy2(f, outpath)
    print('building HHIE HL7 file list...')
    time.sleep(1)
    hhie_file_list = glob.glob(hhie_folder_input + '*.txt')
    print('moving HHIE files...')
    time.sleep(1)
    for f in tqdm.tqdm(hhie_file_list):
        fname = os.path.basename(f)
        outpath = hhie_folder_output + fname
        if not os.path.exists(outpath):
            shutil.copy2(f, outpath)


def process_hl7s():
    tick = time.perf_counter()
    move_hl7_elr()
    tock = time.perf_counter()
    movetime = tock-tick

    tick = time.perf_counter()
    parse_hl7_hhie_parallel()
    tock = time.perf_counter()
    parsetime = tock-tick
    parse_hl7_aims()

    print(f"moving hl7 files took {movetime:0.4f} seconds")
    print(f"parsing hl7s to CSV took {parsetime:0.4f} seconds")


if __name__ == '__main__':
    process_hl7s()

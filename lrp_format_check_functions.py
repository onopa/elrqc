# Set of functions to check formatting for LRP fields.
from datetime import datetime
import re

import dateutil.parser
import pandas as pd
import numpy as np
from email.utils import parseaddr

race_lookup_df_1 = pd.read_excel('./lookups/MasterCodeSet_ver1_12_1_2022.xlsx', dtype='str', keep_default_na=False)
race_lookup_df_1.drop(['Description', 'Coding System'], axis=1, inplace=True)
race_lookups_1 = race_lookup_df_1.Mnemonic
race_lookups_1 = [k.upper() for k in race_lookups_1]
race_lookup_dict_1 = dict(zip(race_lookups_1, race_lookup_df_1.Code))
race_keys_1 = list(race_lookup_dict_1.keys())
race_codes_1 = [k for k in race_keys_1]

race_lookup_df_2 = pd.read_excel('./lookups/LookupTable-237-values-20230707-123230.xlsx', dtype='str', keep_default_na=False)
race_lookup_df_2.drop('DESC', axis=1, inplace=True)
race_lookups_2 = race_lookup_df_2.IN
race_lookups_2 = [k.upper() for k in race_lookups_2]
race_lookup_dict_2 = dict(zip(race_lookups_2, race_lookup_df_2.OUT))
race_keys_2 = list(race_lookup_dict_2.keys())
race_codes_2 = [k for k in race_keys_2]

del race_lookup_df_1, race_lookup_df_2

ethnicity_lookup_df = pd.read_excel('./lookups/MasterCodeSet_ver1_12_1_2022.xlsx', sheet_name='Ethnicity', dtype='str', keep_default_na=False)
ethnicity_lookup_dict = dict(zip(ethnicity_lookup_df.Mnemonic, ethnicity_lookup_df.Description))
ethnicity_keys = list(ethnicity_lookup_dict.keys())
ethnicity_codes = [k.upper() for k in ethnicity_keys]
del ethnicity_lookup_df

observation_lookup_df = pd.read_excel('./lookups/MasterCodeSet_ver1_12_1_2022.xlsx', sheet_name='Observation', dtype='str', keep_default_na=False)
observation_lookup_dict = dict(zip(observation_lookup_df.Mnemonic, observation_lookup_df.Description))
observation_codes = [k.upper() for k in list(observation_lookup_dict.keys())]
del observation_lookup_df

# specimenType_lookup_df = pd.read_excel('../docs/MasterCodeSet_ver1_12_1_2022.xlsx', sheet_name='SpecimenType', dtype='str', keep_default_na=False)
# specimenType_lookup_dict = dict(zip(specimenType_lookup_df.Mnemonic, specimenType_lookup_df.Description))
# specimenType_codes = [k.upper() for k in list(specimenType_lookup_dict.keys())]

def check_col(col, fn, *args):
    total_errors = 0
    for element in col:
        if pd.isna(element):
            continue
        else:
            total_errors += fn(element, *args)
    return total_errors


def check_length(input_value, length):
    if len(input_value) <= length:
        return 0  # input_value is valid
    else:
        return 1  # input_value is invalid


def check_charlist(input_value, length):
    forbidden_chars = set('^~\&ʻ')
    if any((c in forbidden_chars) for c in input_value) or len(input_value) > length:
        return 1
    else:
        return 0


def check_alnum(input_value, length):
    #print(input_value)
    if (input_value.isalnum()) and (len(input_value) <= length):
        return 0  # input_value is valid
    else:
        return 1  # input_value is invalid

# def check_alnum_col(column, length):
#     num_errors = 0
#     for entry in column:
#         if (len(str(entry)) > length) or (not pd.isna(entry) and not str(entry).isalnum()):
#             num_errors += 1
#     return num_errors

def check_alnum_exact(input_value, length):
    if input_value.isalnum() and len(input_value) == length:
        return 0  # input_value is valid
    else:
        return 1  # input_value is invalid

# def check_alnum_exact_col(column, length):
#     num_errors = 0
#     for entry in column:
#         if (len(entry) != length) or (not pd.isna(entry) and not str(entry).isalnum()):
#             num_errors += 1
#     return num_errors

def check_numeric(input_value, length):
    if len(input_value) <= length and input_value.isdigit():
        return 0  # input_value is a valid numeric value
    else:
        return 1  # input_value is not a valid numeric value


def check_numeric_exact(input_value, length):
    if len(input_value) == length and input_value.isdigit():
        return 0  # input_value is a valid 5-digit numeric value
    else:
        return 1  # input_value is not a valid 5-digit numeric value


def check_yyyymmdd(input_value):
    try:
        datetime.strptime(input_value, '%Y%m%d')
        return 0  # input_value is a valid date
    except ValueError:
        return 1  # input_value is not a valid date


def check_date_time(input_value):
    if len(input_value) == 8:
        try:
            pd.to_datetime(input_value)
            return 0
        except (dateutil.parser.ParserError, pd.errors.OutOfBoundsDatetime, ValueError):
            return 1
    elif len(input_value) == 10:
        try:
            pd.to_datetime(input_value, format='%Y%m%d%H')
            return 0
        except (dateutil.parser.ParserError, pd.errors.OutOfBoundsDatetime, ValueError):
            return 1
    elif len(input_value) == 14:
        try:
            pd.to_datetime(input_value, format='%Y%m%d%H%M%S')
            return 0
        except (dateutil.parser.ParserError, pd.errors.OutOfBoundsDatetime, ValueError):
            return 1
    elif len(input_value) == 19:
        try:
            pd.to_datetime(input_value)
            return 0
        except (dateutil.parser.ParserError, pd.errors.OutOfBoundsDatetime, ValueError):
            return 1
    else:
        return 1


def check_timestamp(input_value):
    try:
        pd.to_datetime(input_value)
        return 0
    except ValueError:
        return 1


def check_zip(input_value):
    pattern = r"^\d{5}(?:[-\s]\d{4})?$"
    if re.match(pattern, input_value):
        return 0
    else:
        return 1


def check_gender(input_value):
    valid_input_values = ('M', 'F', 'X', 'O', 'U')
    if (len(input_value) > 1) or (input_value.upper() not in valid_input_values):
        return 1  # input_value is valid
    else:
        return 0  # input_value is invalid

# def check_gender_col(col):
#     valid_input_values = ['M', 'F', 'X', 'O', 'U']
#     num_errors = 0
#     for element in col:
#         if element in valid_input_values:
#             continue
#         else:
#             num_errors += 1
#     return num_errors


def check_race(input_value):
    valid_input_values = ['I', 'A', 'B', 'P', 'W', 'O', 'U']
    valid_input_values = valid_input_values + race_codes_1 + race_codes_2
    if input_value.upper() in valid_input_values:
        return 0  # input_value is valid
    else:
        return 1  # input_value is invalid


def check_race_code(input_value):
    valid_input_values = ['2106-3', '2028-9', '2054-5', '1002-5', '2131-1', '2076-8', 'UNK', 'PHC1175']
    if input_value.upper() in valid_input_values:
        return 0
    else:
        return 1


def check_ethnicity(input_value):
    valid_input_values = ethnicity_codes
    if input_value.upper() in valid_input_values:
        return 0  # input_value is valid
    else:
        return 1  # input_value is invalid


def check_ynuunk(input_value):
    valid_input_values = ('Y', 'N', 'U', 'YES', 'NO', 'UNK', 'UNKNOWN')
    if input_value.upper() in valid_input_values:
        return 0  # input_value is valid
    else:
        return 1  # input_value is invalid


def check_state(input_value):
    valid_abbreviations = ('AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN',
                           'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV',
                           'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN',
                           'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY')

    if input_value.upper() in valid_abbreviations:
        return 0  # input_value is a valid state abbreviation
    else:
        return 1  # input_value is not a valid state abbreviation

def check_loinc(input_value):
    pattern = r"\d\d\d\d\d-\d"
    if re.match(pattern, input_value):
        return 0
    else:
        return 1

def check_email(input_value):
    pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
    if re.match(pattern, input_value):
        return 0  # input_value is a valid email format
    else:
        return 1  # input_value is not a valid email format


def check_observation(input_value):
    valid_observation_values = observation_codes
    if input_value.upper() in valid_observation_values:
        return 0  # input_value is valid
    else:
        return 1  # input_value is invalid


# def check_specimen_type(input_value):
#     valid_input_values = specimenType_codes
#     if input_value.upper() in valid_input_values:
#         return 0
#     else:
#         return 1

def count_race(col, selection_str):
    total_count = 0
    if selection_str == 'missing':
        for element in col:
            if pd.isna(element):
                total_count += 1
    elif selection_str == 'misformat':
        for element in col:
            if pd.isna(element):
                continue
            if element.upper() not in race_codes_1 + race_codes_2:
                total_count += 1
    else:
        for element in col:
            if pd.isna(element):
                continue
            if element.upper() in race_codes_1:
                if race_lookup_dict_1[element.upper()] == selection_str:
                    total_count += 1
            elif element.upper() in race_codes_2:
                if race_lookup_dict_2[element.upper()] == selection_str:
                    total_count += 1
            else:
                continue
    return total_count


def count_misformat_race(col):
    misformat_list = []
    for val in col:
        if check_race(str(val)):
            misformat_list.append(val)
    misformat_series = pd.DataFrame(misformat_list)
    misformat_counts = misformat_series.value_counts().reset_index(name='count')
    return pd.DataFrame(misformat_counts)


def get_race_lookup_dict():
    return race_lookup_dict_1


def get_check_dict():
    output = {'patientLastName': lambda x: check_charlist(x, 75),  # Patient Last Name.
              'patientFirstName': lambda x: check_charlist(x, 75),  # Patient First Name.
              'patientDOB': lambda x: check_date_time(x), # Patient Date of Birth. change to check viability of dates?
              'patientGender': lambda x: check_gender(x),  # Patient Sex
              'patientId': lambda x: check_alnum(x, 75),  # Patient ID
              'patientRace': lambda x: check_race(x),  # Patient Race
              'patientRaceCode': lambda x: check_race_code(x),  # Patient Race Code
              'patientEthnicity': lambda x: check_ethnicity(x),  # ,  # Patient Ethnicity TODO: KMC missing col
              'patientAddressLine1': lambda x: check_charlist(x, 75),  # Patient Street Address.
              'patientAddressCity': lambda x: check_charlist(x, 75),  # Patient City.
              'patientAddressState': lambda x: check_state(x),  # Patient State
              'patientAddressZip': lambda x: check_zip(x),  # Patient Zip Code#
              'PATIENTADDRESSCOUNTY': lambda x: check_charlist(x, 75),  # Patient County TODO: CLS missing col
              'patientTelephoneNumber': lambda x: check_numeric_exact(x, 10),  # Patient Phone No
              'Notes': lambda x: check_email(x),  # Patient Email. TODO: DPS mapper wrong. also - why Notes??
              'specimenCollectionDate': lambda x: check_date_time(x),# Specimen Collection Date. change to check viability of dates?
              'specimenReceivedDate': lambda x: check_date_time(x),  # Specimen Received Date. check viability?
              'placerOrder': lambda x: check_alnum(x, 75),  # Accession Number
              'specimenType': lambda x: check_charlist(x, 75), # Specimen Source. what about cdc loinc code mapping file?
              'labResultDate': lambda x: check_date_time(x),  # Test Result Date. check date viability?
              'resultedTest': lambda x: check_length(x, 150),  # Test Name todo: ETN mapper wrong
              'resultedTest_LOINC': lambda x: check_loinc(x),  # Test LOINC Code TODO: DPS mapper wrong?
              #'observation': lambda x: check_observation(x),
              'performingFacility': lambda x: check_charlist(x, 75),  # Testing Lab Name.
              'performingFacilityCLIA': lambda x: check_alnum_exact(x, 10), # Testing Lab CLIA TODO: WHC missing column
              'performingFacilityAddress': lambda x: check_charlist(x, 75), # Testing Lab Street Address. TODO: CVS mapper wrong
              'performingFacility_City': lambda x: check_charlist(x, 75),  # Testing Lab City.
              'performingFacility_State': lambda x: check_state(x),  # Testing Lab State
              'performingFacility_Zip': lambda x: check_zip(x),  # Testing Lab Zip Code
              'performingFacility_CallBackNumber': lambda x: check_numeric_exact(x, 10), # Testing Lab Phone No todo: ETN mapper wrong
              'ProviderName': lambda x: check_charlist(x, 75), # Ordering Provider Last Name. char list? TODO: var redundant with following 2.
              'providerLastName': lambda x: check_charlist(x, 75), # Ordering Provider Last Name. char list? TODO: redundant?
              'providerFirstName': lambda x: check_charlist(x, 75), # Ordering Provider First Name. char list? TODO: redundant?
              'ProviderAddress': lambda x: check_charlist(x, 75),  # Ordering Provider Street Address.
              'Provider_City': lambda x: check_charlist(x, 75),  # Ordering Provider City.
              'Provider_State': lambda x: check_state(x),  # Ordering Provider State
              'Provider_Zip': lambda x: check_zip(x),  # Ordering Provider Zip Code
              'Provider_CallBackNumber': lambda x: check_numeric_exact(x, 10), # Ordering Provider Phone No  TODO: CLS mapping incorrect, check w/ jonathan
              'orderingFacility': lambda x: check_charlist(x, 75),  # Ordering Facility Name.
              'orderingFacility_Address': lambda x: check_charlist(x, 75),  # Ordering Facility Street Address.
              'orderingFacility_City': lambda x: check_charlist(x, 75),  # Ordering Facility City
              'orderingFacility_State': lambda x: check_state(x),  # Ordering Facility State
              'orderingFacility_Zip': lambda x: check_zip(x),  # Ordering Facility Zip Code
              'orderingFacility_CallBackNumber': lambda x: check_numeric_exact(x, 10), # Ordering Facility Phone No TODO: CLS mapper wrong
              'isThisFirstTest': lambda x: check_ynuunk(x), # Patient First Covid Test TODO: what is meant by 'description' or 'code'? Y/N vs Yes/No? Also - move to optional vars
              'isAHealthcareWorker': lambda x: check_ynuunk(x), # Patient Employed in Healthcare TODO: as above, optional + desc/code
              'symptomOnsetDate': lambda x: check_yyyymmdd(x), # Patient Symptom Onset Date TODO: KMC missing, move to optional vars
              'isSymptomatic': lambda x: check_ynuunk(x), # Patient Symptomatic As Defined By CDC TODO: KMC missing col, so optional. also description/code?
              'isHospitalized': lambda x: check_ynuunk(x), # Patient Hospitalized TODO: as above, optional + desc/code?
              'inICU': lambda x: check_ynuunk(x),  # Patient In ICU TODO: as above, optional + desc/code
              'isPregnant': lambda x: check_ynuunk(x),  # Patient Pregnant TODO: as above, optional + desc/code
              'isResidentOfCongregateCareSetting': lambda x: check_ynuunk(x), # Patient Resident in a Congregate Care Setting, todo: as above, optional + desc/code
              'patientAge': lambda x: check_numeric(x, 3), # Patient Age at Time of Collection (Years) todo: ETN missing column, move to optional. also KMC mapper is missing this
              }
    return output

def get_check_function_dict_csv():
    check_dict = get_check_dict()
    check_dict.update({'observation': lambda x: check_observation(x)})
    return check_dict

def get_check_function_dict_hl7():
    check_dict = get_check_dict()
    check_dict.update({'observation': lambda x: check_charlist(x, 75)})
    return check_dict

def get_check_col_dict():
    output = {'patientLastName': lambda x: check_col(x, check_charlist, 75),  # Patient Last Name.
              'patientFirstName': lambda x: check_col(x, check_charlist, 75),  # Patient First Name.
              'patientDOB': lambda x: check_col(x, check_date_time),  # Patient Date of Birth. change to check viability of dates?
              'patientGender': lambda x: check_col(x, check_gender),  # Patient Sex
              'patientId': lambda x: check_col(x, check_alnum, 75),  # Patient ID
              'patientRace': lambda x: check_col(x, check_race),  # Patient Race. uses LUT.
              'patientRaceCode': lambda x: check_col(x, check_race_code),
              'patientEthnicity': lambda x: check_col(x, check_ethnicity),  # Patient Ethnicity. uses LUT TODO: KMC missing col (???)
              'patientAddressLine1': lambda x: check_col(x, check_charlist, 75),  # Patient Street Address.
              'patientAddressCity': lambda x: check_col(x, check_charlist, 75),  # Patient City.
              'patientAddressState': lambda x: check_col(x, check_state),  # Patient State
              'patientAddressZip': lambda x: check_col(x, check_zip),  # Patient Zip Code#
              'PATIENTADDRESSCOUNTY': lambda x: check_col(x, check_charlist, 75),  # Patient County TODO: CLS missing col
              'patientTelephoneNumber': lambda x: check_col(x, check_numeric_exact, 10),  # Patient Phone No
              'Notes': lambda x: check_col(x, check_email),  # Patient Email. TODO: DPS mapper wrong. also - why Notes??
              'specimenCollectionDate': lambda x: check_col(x, check_date_time),  # Specimen Collection Date. change to check viability of dates?
              'specimenReceivedDate': lambda x: check_col(x, check_date_time),  # Specimen Received Date. check viability?
              'placerOrder': lambda x: check_col(x, check_alnum, 75),  # Accession Number
              'specimenType': lambda x: check_col(x, check_charlist, 75),  # uses LUT
              'labResultDate': lambda x: check_col(x, check_date_time),  # Test Result Date. check date viability?
              'resultedTest': lambda x: check_col(x, check_length, 150),  # Test Name todo: ETN mapper wrong
              'resultedTest_LOINC': lambda x: check_col(x, check_loinc),  # Test LOINC Code
              #'observation': lambda x: check_col(x, check_observation),  # uses LUT
              'performingFacility': lambda x: check_col(x, check_charlist, 75),  # Testing Lab Name.
              'performingFacilityCLIA': lambda x: check_col(x, check_alnum_exact, 10),  # Testing Lab CLIA TODO: WHC missing column
              'performingFacilityAddress': lambda x: check_col(x, check_charlist, 75),  # Testing Lab Street Address. TODO: CVS mapper wrong
              'performingFacility_City': lambda x: check_col(x, check_charlist, 75),  # Testing Lab City.
              'performingFacility_State': lambda x: check_col(x, check_state),  # Testing Lab State
              'performingFacility_Zip': lambda x: check_col(x, check_zip),  # Testing Lab Zip Code
              'performingFacility_CallBackNumber': lambda x: check_col(x, check_numeric_exact, 10),  # Testing Lab Phone No todo: ETN mapper wrong
              'ProviderName': lambda x: check_col(x, check_charlist, 75),  ######## Ordering Provider Last Name. char list? TODO: var redundant with following 2.
              'providerLastName': lambda x: check_col(x, check_charlist, 75),  ######## Ordering Provider Last Name. char list? TODO: redundant?
              'providerFirstName': lambda x: check_col(x, check_charlist, 75),  ####### Ordering Provider First Name. char list? TODO: redundant?
              'ProviderAddress': lambda x: check_col(x, check_charlist, 75),  # Ordering Provider Street Address.
              'Provider_City': lambda x: check_col(x, check_charlist, 75),  # Ordering Provider City.
              'Provider_State': lambda x: check_col(x, check_state),  # Ordering Provider State
              'Provider_Zip': lambda x: check_col(x, check_zip),  # Ordering Provider Zip Code
              'Provider_CallBackNumber': lambda x: check_col(x, check_numeric_exact, 10),  # Ordering Provider Phone No  TODO: CLS mapping incorrect, check w/ jonathan
              'orderingFacility': lambda x: check_col(x, check_charlist, 75),  # Ordering Facility Name.
              'orderingFacility_Address': lambda x: check_col(x, check_charlist, 75), # Ordering Facility Street Address.
              'orderingFacility_City': lambda x: check_col(x, check_charlist, 75),  # Ordering Facility City
              'orderingFacility_State': lambda x: check_col(x, check_state),  # Ordering Facility State
              'orderingFacility_Zip': lambda x: check_col(x, check_zip),  # Ordering Facility Zip Code
              'orderingFacility_CallBackNumber': lambda x: check_col(x, check_numeric_exact, 10),  # Ordering Facility Phone No TODO: CLS mapper wrong
              'isThisFirstTest': lambda x: check_col(x, check_ynuunk),  # Patient First Covid Test TODO: what is meant by 'description' or 'code'? Y/N vs Yes/No? Also - move to optional vars
              'isAHealthcareWorker': lambda x: check_col(x, check_ynuunk),  # Patient Employed in Healthcare TODO: as above, optional + desc/code
              'symptomOnsetDate': lambda x: check_col(x, check_yyyymmdd),  # Patient Symptom Onset Date TODO: KMC missing, move to optional vars
              'isSymptomatic': lambda x: check_col(x, check_ynuunk),  # Patient Symptomatic As Defined By CDC TODO: KMC missing col, so optional. also description/code?
              'isHospitalized': lambda x: check_col(x, check_ynuunk),  # Patient Hospitalized TODO: as above, optional + desc/code?
              'inICU': lambda x: check_col(x, check_ynuunk),  # Patient In ICU TODO: as above, optional + desc/code
              'isPregnant': lambda x: check_col(x, check_ynuunk),  # Patient Pregnant TODO: as above, optional + desc/code
              'isResidentOfCongregateCareSetting': lambda x: check_col(x, check_ynuunk),  # Patient Resident in a Congregate Care Setting, todo: as above, optional + desc/code
              'patientAge': lambda x: check_col(x, check_numeric, 3),  # Patient Age at Time of Collection (Years) todo: ETN missing column, move to optional. also KMC mapper is missing this
              #'Lab Name': lambda x: check_col(x, check_charlist, 5),
              'ADDITIONAL_NOTES_1': lambda x: check_col(x, check_charlist, 75)
              }
    return output

#
def get_check_col_dict_csv():
    check_col_dict = get_check_col_dict()
    check_col_dict.update({'observation': lambda x: check_col(x, check_observation)})
    return check_col_dict

#
def get_check_col_dict_hl7():
    check_col_dict = get_check_col_dict()
    check_col_dict.update({'observation': lambda x: check_col(x, check_charlist, 75)})
    return check_col_dict


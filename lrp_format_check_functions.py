# Set of functions to check formatting for LRP fields.
from datetime import datetime
import re
import pandas as pd
import numpy as np
from email.utils import parseaddr

race_lookup_df = pd.read_excel('./lookups/LookupTable-237-values-20230707-123230.xlsx', dtype='str', keep_default_na=False)
race_lookup_df.drop('DESC', axis=1, inplace=True)
race_lookup_dict = dict(zip(race_lookup_df.IN, race_lookup_df.OUT))
race_keys = list(race_lookup_dict.keys())
race_codes = [k.upper() for k in race_keys]
del race_lookup_df

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

def check_timestamp(input_value):
    try:
        pd.to_datetime(input_value)
        return 0
    except ValueError:
        return 1
# def check_yyyymmdd_col(column):
#     num_errors = 0
#     for entry in column:
#         try:
#             datetime.strptime(str(entry), '%Y%m%d')
#             continue
#         except ValueError:
#             num_errors += 1
#     return num_errors

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
    valid_input_values = valid_input_values + race_codes
    if input_value.upper() in valid_input_values:
        return 0  # input_value is valid
    else:
        return 1  # input_value is invalid


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
    if selection_str == 'misformat':
        for element in col:
            if pd.isna(element):
                continue
            if element.upper() not in race_codes:
                total_count += 1
    else:
        for element in col:
            if pd.isna(element):
                continue
            try:
                if race_lookup_dict[element] == selection_str:
                        total_count += 1
            except KeyError:
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


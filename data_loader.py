import pandas as pd
import datetime
import glob
import sys
from natsort import natsorted
from hl7_tools import parse_hl7_aims, parse_hl7_hhie
import config

class LabData:
    def __init__(self, lab_name, df, source_type):
        self.lab_name = lab_name
        self.df = df
        self.source_type = source_type # use 'csv' or 'hl7'
        self.missingness = pd.DataFrame()
        self.misformatting = pd.DataFrame()
        self.race = pd.DataFrame()
        self.misformat_values = pd.DataFrame()
        self.lags = pd.DataFrame()

    def __lt__(self, other):
        if self.lab_name < other.lab_name:
            return True
        else:
            return False

    def __eq__(self, other):
        if self.get_name() == other.get_name():
            return True
        else:
            return False


def filter_input_data(df):
    print(f'filtering data: {config.filter_var} == {config.filter_val}')
    filter_var = config.filter_var
    filter_val = config.filter_val
    df = df.loc[df[filter_var] == filter_val]
    return df

# get SFTP/LRP data as df
def import_csv_df():
    df_list = []
    for labfile in glob.glob('./data/processed/csv_labs_concat/*.csv'):
        print(labfile)
        df = pd.read_csv(labfile, dtype='str')
        df_list.append(df)

    output = pd.concat(df_list)
    output['Submission Date'] = pd.to_datetime(output['File Creation Date'])
    output.drop(['File Creation Date'], axis=1, inplace=True)

    return output

# Get SFTP/LRP data as csv
def import_csv_dict():
    df_dict = {}
    for labfile in glob.glob('./data/processed/csv_labs_concat/*.csv'):
        print('loading ' + labfile)
        df = pd.read_csv(labfile, dtype='str')
        df['Submission Date'] = pd.to_datetime(df['File Creation Date'], format='%Y-%m-%d')
        df.drop('File Creation Date', axis=1, inplace=True)
        lab_name = df['Lab Name'][0]
        df_dict[lab_name] = df

    return df_dict

# Get HHIE/AIMS xls data pulls (deprecated - switched to using hl7)
def import_hhie_df():
    filepath = './data/raw/hhie_aims/current_data_pull.xls'

    renaming_dict = {
        'AuditDateTime': 'Submission Date',
        'LastName_5_1': 'patientLastName',
        'FirstName_5_2': 'patientFirstName',
        'DOB_7': 'patientDOB',
        'Gender_8': 'patientGender',
        'PatientID_3_1': 'patientId',
        'Race_10_2': 'patientRace',
        'Ethnicity_22_2': 'patientEthnicity',
        'Address1_11_1': 'patientAddressLine1',
        'City_11_3': 'patientAddressCity',
        'State_11_4': 'patientAddressState',
        'Zip_11_5': 'patientAddressZip',
        'County_11_9': 'PATIENTADDRESSCOUNTY',
        'Phone_13_6_7': 'patientTelephoneNumber',
        'SpecCollectionDate_17_1': 'specimenCollectionDate',
        'Email_13_4': 'Notes',
        'SpecReceivedDate_18_1': 'specimenReceivedDate',
        'TestingLabSpecID_2_2_1': 'placerOrder',
        'SpecTypeFreeText_4_9': 'specimenType',
        'ResultDate_22': 'labResultDate',
        'TestPerformedDesc_3_2': 'resultedTest',
        'TestPerformedCode_3_1': 'resultedTest_LOINC',
        'TestResultDesc_5_2': 'observation',
        'TestingLabName_23_1': 'performingFacility',
        'TestingLabCLIA_23_6_2': 'performingFacilityCLIA',
        'TestingLabAddress2_24_2': 'performingFacilityAddress',
        'TestingLabCity_24_3': 'performingFacility_City',
        'TestingLabState_24_4': 'performingFacility_State',
        'TestingLabZip_24_5': 'performingFacility_Zip',
        'OrderingProvLastName_16_2': 'providerLastName',
        'OrderingProvFirstName_16_3': 'providerFirstName',
        'OrderingProvAddress1_24_1': 'ProviderAddress',
        'OrderingProvCity_24_3': 'Provider_City',
        'OrderingProvState_24_4': 'Provider_State',
        'OrderingProvZip_24_5': 'Provider_Zip',
        'OrderCallBackPhone_17_6_7': 'performingFacility_CallBackNumber',
        'OrderingFacilityName_21_1': 'orderingFacility',
        'OrderingFacilityAddress1_22_1': 'orderingFacility_Address',
        'OrderingFacilityCity_22_3': 'orderingFacility_City',
        'OrderingFacilityState_22_4': 'orderingFacility_State',
        'OrderingFacilityZip_22_5': 'orderingFacility_Zip',
        'OrderingFacilityPhone_23_1': 'orderingFacility_CallBackNumber',
        'Age': 'patientAge',
        'LabMnemonic': 'Lab Name',
    }

    # TODO: what are these
    extra_vars = ['MiddleName_5_3', 'SpecTypeCode_4_1', 'TestResultCode_5_1', 'ObservationMethodDesc_17_2']

    xl = pd.ExcelFile(path_or_buffer=filepath)
    sheets = xl.sheet_names
    df = pd.DataFrame()
    for i, sheet in enumerate(sheets):
        print('loading HHIE/AIMS excel:' + sheet)
        if i == 0:
            sheetdf = xl.parse(sheet_name=sheets[0], skiprows=[0, 2], dtype='str', header=0)
        else:
            sheetdf = xl.parse(sheet_name=sheets[0], skiprows=[0, 1, 2], dtype='str', header=None)
            sheetdf.columns = df.columns
        sheetdf.drop_duplicates(subset='TestingLabSpecID_2_2_1', inplace=True)
        df = pd.concat([df, sheetdf])
        df.drop_duplicates(subset='TestingLabSpecID_2_2_1', inplace=True)

    df.rename(columns=renaming_dict, inplace=True)
    df.loc[df['patientRace'] == 'Asian or Other Pacific Islander', 'patientRace'] = 'Asian'
    all_cols = df.columns
    cols_to_drop = [col for col in all_cols if col not in list(renaming_dict.values())]
    print(cols_to_drop)
    df2 = df.drop(cols_to_drop, axis=1)
    df2['Submission Date'] = [k.date() for k in pd.to_datetime(df2['Submission Date'])]
    #df2['Submission Date'] = df2['Submission Date'].apply(lambda x: x.date())

    return df2

# get hl7 data from csv form
def import_hl7_df():
    hl7_files = glob.glob('./data/processed/hl7/*tabular_output.csv')
    hl7_df_list = []
    for hl7_file in hl7_files:
        hl7_df = pd.read_csv(hl7_file, dtype='str')
        hl7_df['Submission Date'] = pd.to_datetime(hl7_df['Submission Date'])
        hl7_df_list.append(hl7_df)
    hl7_concat_df = pd.concat(hl7_df_list)
    return hl7_concat_df

def import_hl7_dict():
    hl7_files = glob.glob('./data/processed/hl7/*tabular_output.csv')
    hl7_df_list = []
    for hl7_file in hl7_files:
        hl7_df = pd.read_csv(hl7_file, dtype='str').reset_index()
        hl7_df['Submission Date'] = pd.to_datetime(hl7_df['Submission Date'])
        hl7_df_list.append(hl7_df)
    hl7_concat_df = pd.concat(hl7_df_list)
    labs = pd.unique(hl7_concat_df['Lab Name'])
    hl7_dict = {}
    for lab in labs:
        lab_df = hl7_concat_df.loc[hl7_concat_df['Lab Name'] == lab]
        hl7_dict[lab] = lab_df
    return hl7_dict


def import_hl7_data():
    hl7_tabular_files = glob.glob('./data/processed/hl7/*tabular_output.csv')
    hl7_df_list = []
    for hl7_tabular_file in hl7_tabular_files:
        hl7_df = pd.read_csv(hl7_tabular_file, dtype='str')
        hl7_df[config.time_var] = pd.to_datetime(hl7_df[config.time_var])
        hl7_df_list.append(hl7_df)
    hl7_concat_df = pd.concat(hl7_df_list)
    if config.filter_option:
        hl7_concat_df = filter_input_data(hl7_concat_df)
    labs = pd.unique(hl7_concat_df[config.grouping_var])
    labs = natsorted(labs)
    hl7_data_list = []
    print('loading HL7 data...')
    for lab_name in labs:
        if pd.isna(lab_name):
            lab_name = 'MISSING'
            lab_df = hl7_concat_df.loc[pd.isna(hl7_concat_df[config.grouping_var])].reset_index(drop=True)
        else:
            lab_df = hl7_concat_df.loc[hl7_concat_df[config.grouping_var] == lab_name].reset_index(drop=True)
        hl7_object = LabData(lab_name, lab_df, 'hl7')
        hl7_data_list.append(hl7_object)
    return hl7_data_list


def import_csv_data():
    csv_data_list = []
    print('loading CSV data...')
    for lab_file in glob.glob('./data/processed/csv_labs_concat/*.csv'):
        df = pd.read_csv(lab_file, dtype='str')
        df.reset_index(inplace=True)
        df['Submission Date'] = pd.to_datetime(df['File Creation Date'], format='%Y-%m-%d')
        df.drop('File Creation Date', axis=1, inplace=True)
        lab_name = df['Lab Name'][0]
        csv_lab_object = LabData(lab_name, df, 'csv')
        csv_data_list.append(csv_lab_object)
    return csv_data_list

def import_hl7_and_sftp_dict():
    dict1 = import_hl7_dict()
    dict2 = import_csv_dict()
    dict1.update(dict2)
    return dict1

# get ecr data from csv data pull form
def import_ecr(output_type):
    if output_type not in ('df', 'dict'):
        raise ValueError('neet to select df or dict return type')
    ecr_df = pd.read_csv('./data/raw/ecr/ecr_data2.csv', dtype='str')
    ecr_df['report_time'] = pd.to_datetime(ecr_df['report_time'], format='%Y%m%d%H%M%S-%f').apply(datetime.datetime.date)

    return ecr_df

# get missingness data
def import_missingness(output_type):
    if output_type not in ('df', 'dict'):
        raise ValueError('neet to select df or dict return type')
    missingness_files = glob.glob('./data/processed/hl7_missingness/*.csv') + glob.glob('./data/processed/csv_missingness/*.csv')
    file_df_list = []
    lab_name_list = []
    for missfile in missingness_files:
        lab_df = pd.read_csv(missfile)
        lab_df['Submission Date'] = pd.to_datetime(lab_df['Submission Date'])
        lab_name = lab_df['Lab Name'][0]

        file_df_list.append(lab_df)
        lab_name_list.append(lab_name)

    if output_type == 'df':
        return pd.concat(file_df_list)

    elif output_type == 'dict':
        return dict(zip(lab_name_list, file_df_list))

    else:
        sys.exit('something went wrong on missingness import')

# get misformatting data
def import_misformatting(output_type):
    if output_type not in ('df', 'dict'):
        raise ValueError('need to select df or dict return type')
    df_list = []
    name_list = []
    misformat_files = glob.glob('./data/processed/hl7_misformatting/*.csv') + glob.glob('./data/processed/csv_misformatting/*.csv')
    for file in misformat_files:
        df = pd.read_csv(file)
        df['Submission Date'] = pd.to_datetime(df['Submission Date'])
        df_list.append(df)
        name_list.append(df['Lab Name'][0])

    if output_type == 'dict':
        return dict(zip(name_list, df_list))
    elif output_type == 'df':
        return pd.concat(df_list)
    else:
        sys.exit('problem with misformat import, please select either \'df\' or \'dict\'')

# get misformatted values
def import_misformat_values(output_type):
    if output_type not in ('df', 'dict'):
        raise ValueError('need to select df or dict return type')
    misformat_list = []
    for misformat_file in glob.glob('./data/processed/csv_misformat_values/*.csv'):
        mfdf_csv = pd.read_csv(misformat_file, dtype='str')
        misformat_list.append(mfdf_csv)

    for misformat_file in glob.glob('./data/processed/hl7_misformat_values/*.csv'):
        mfdf_hl7 = pd.read_csv(misformat_file)
        misformat_list.append(mfdf_hl7)

    if output_type == 'df':
        output_df = pd.concat(misformat_list)
        return output_df
    elif output_type == 'dict':
        lab_names = []
        for lab in misformat_list:
            lab_names.append(lab['Lab Name'][0])

        output_dict = dict(zip(lab_names, misformat_list))
        return output_dict
    else:
        sys.exit('problem with misformat values import')



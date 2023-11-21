import pandas as pd
import datetime
import glob
from lrp_format_check_functions import *
import multiprocessing
from data_loader import import_hhie_df

check_function_dict = {'patientLastName': lambda x: check_col(x, check_charlist, 75),  # Patient Last Name.
                       'patientFirstName': lambda x: check_col(x, check_charlist, 75),  # Patient First Name.
                       'patientDOB': lambda x: check_col(x, check_timestamp),  # Patient Date of Birth. change to check viability of dates?
                       'patientGender': lambda x: check_col(x, check_gender),  # Patient Sex
                       'patientId': lambda x: check_col(x, check_alnum, 75),  # Patient ID
                       'patientRace': lambda x: check_col(x, check_race),  # Patient Race. uses LUT.
                       'patientEthnicity': lambda x: check_col(x, check_ethnicity),  # Patient Ethnicity. uses LUT TODO: KMC missing col (???)
                       'patientAddressLine1': lambda x: check_col(x, check_charlist, 75), # Patient Street Address.
                       'patientAddressCity': lambda x: check_col(x, check_charlist, 75),  # Patient City.
                       'patientAddressState': lambda x: check_col(x, check_state),  # Patient State
                       'patientAddressZip': lambda x: check_col(x, check_numeric_exact, 5),  # Patient Zip Code#
                       'PATIENTADDRESSCOUNTY': lambda x: check_col(x, check_charlist, 75),  # Patient County TODO: CLS missing col
                       'patientTelephoneNumber': lambda x: check_col(x, check_numeric_exact, 10),  # Patient Phone No
                       'Notes': lambda x: check_col(x, check_email),  # Patient Email. TODO: DPS mapper wrong. also - why Notes??
                       'specimenCollectionDate': lambda x: check_col(x, check_timestamp),  # Specimen Collection Date. change to check viability of dates?
                       'specimenReceivedDate': lambda x: check_col(x, check_timestamp),  # Specimen Received Date. check viability?
                       'placerOrder': lambda x: check_col(x, check_alnum, 75),  # Accession Number
                       'specimenType': lambda x: check_col(x, check_charlist, 75),  # uses LUT
                       'labResultDate': lambda x: check_col(x, check_timestamp),  # Test Result Date. check date viability?
                       'resultedTest': lambda x: check_col(x, check_length, 150),  # Test Name todo: ETN mapper wrong
                       'resultedTest_LOINC': lambda x: check_col(x, check_loinc),  # Test LOINC Code
                       'observation': lambda x: check_col(x, check_observation),  # uses LUT
                       'performingFacility': lambda x: check_col(x, check_charlist, 75),  # Testing Lab Name.
                       'performingFacilityCLIA': lambda x: check_col(x, check_alnum_exact, 10),  # Testing Lab CLIA TODO: WHC missing column
                       'performingFacilityAddress': lambda x: check_col(x, check_charlist, 75),  # Testing Lab Street Address. TODO: CVS mapper wrong
                       'performingFacility_City': lambda x: check_col(x, check_charlist, 75),  # Testing Lab City.
                       'performingFacility_State': lambda x: check_col(x, check_state),  # Testing Lab State
                       'performingFacility_Zip': lambda x: check_col(x, check_numeric_exact, 5),  # Testing Lab Zip Code
                       'performingFacility_CallBackNumber': lambda x: check_col(x, check_numeric_exact, 10),  # Testing Lab Phone No todo: ETN mapper wrong
                       'ProviderName': lambda x: check_col(x, check_charlist, 75),  ######## Ordering Provider Last Name. char list? TODO: var redundant with following 2.
                       'providerLastName': lambda x: check_col(x, check_charlist, 75),  ######## Ordering Provider Last Name. char list? TODO: redundant?
                       'providerFirstName': lambda x: check_col(x, check_charlist, 75),  ####### Ordering Provider First Name. char list? TODO: redundant?
                       'ProviderAddress': lambda x: check_col(x, check_charlist, 75),  # Ordering Provider Street Address.
                       'Provider_City': lambda x: check_col(x, check_charlist, 75),  # Ordering Provider City.
                       'Provider_State': lambda x: check_col(x, check_state),  # Ordering Provider State
                       'Provider_Zip': lambda x: check_col(x, check_numeric_exact, 5),  # Ordering Provider Zip Code
                       'Provider_CallBackNumber': lambda x: check_col(x, check_numeric_exact, 10),  # Ordering Provider Phone No  TODO: CLS mapping incorrect, check w/ jonathan
                       'orderingFacility': lambda x: check_col(x, check_charlist, 75),  # Ordering Facility Name.
                       'orderingFacility_Address': lambda x: check_col(x, check_charlist, 75), # Ordering Facility Street Address.
                       'orderingFacility_City': lambda x: check_col(x, check_charlist, 75),  # Ordering Facility City
                       'orderingFacility_State': lambda x: check_col(x, check_state),  # Ordering Facility State
                       'orderingFacility_Zip': lambda x: check_col(x, check_numeric_exact, 5),  # Ordering Facility Zip Code
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
                       'Lab Name': lambda x: check_col(x, check_charlist, 5),
                       'ADDITIONAL_NOTES_1': lambda x: check_col(x, check_charlist, 75)
                       }


# prepares raw data for processing. loads data from excel spreadsheet, renames variables, and returns a list of tuples
# [(lab name, lab data)]. could have been a dict I suppose.
def prep_hhie_data_list():
    # read data

    df = import_hhie_df()

    #vars_to_drop = ['Submission Date']
    all_labs = pd.unique(df['patientRace'])

    # check missignness
    for lab in all_labs:
        lab_df = df.loc[df['patientRace'] == lab]
        lab_grp = lab_df.groupby(['Submission Date'])
        miss_count = lab_grp.agg(lambda x: x.isnull().sum())
        miss_total = pd.DataFrame(lab_grp.size())
        miss_count['Total Submission Count'] = miss_total
        miss_count['patientRace'] = lab
        miss_count.to_csv('./data/processed/hhie_race_missing/' + lab + '.csv')

    labdfs = []
    for lab in all_labs:
        labdfs.append([lab, df.loc[df['patientRace'] == lab]])

    return labdfs



def compile_formatting(lablist):
    labname = lablist[0]
    labdf = lablist[1]

    varnames = labdf.columns
    print('starting ' + labname)

    date_counts = pd.DataFrame(labdf.groupby('Submission Date').size())
    date_counts.columns = ['Total Submission Count']
    date_counts['patientRace'] = labname

    vars_to_drop = ['Submission Date', 'patientRace']

    agg_dict = dict((varname, check_function_dict[varname]) for varname in varnames if varname not in vars_to_drop)
    labdf_agg = labdf.groupby('Submission Date').agg(agg_dict)
    labdf_agg = pd.concat([date_counts, labdf_agg], axis=1)
    lab2 = labname.replace('/', '_')
    labdf_agg.to_csv('./data/processed/hhie_race_misformat/' + lab2 + '_misformat.csv')
    print('finished ' + labname)
    return labdf_agg


def check_hhie_parallel():
    labdfs = prep_hhie_data_list()

    print('starting multiprocess check of HHIE formatting')
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        data = pool.map(compile_formatting, labdfs)
    pd.concat(data, axis=1).to_csv('./data/processed/hhie_race_misformat.csv')

if __name__ == '__main__':
    # check SFTP/LRP
    #exec(open('check_format_SFTP-LRP.py').read())

    # check HHIE data
    check_hhie_parallel()
from lrp_format_check_functions import *
from data_loader import *
import pandas as pd
import glob
import datetime

check_function_dict = {'patientLastName': lambda x: check_col(x, check_charlist, 75),  # Patient Last Name.
                       'patientMiddleName': lambda x: check_col(x, check_charlist, 75),
                       'patientFirstName': lambda x: check_col(x, check_charlist, 75),  # Patient First Name.
                       'patientDOB': lambda x: check_col(x, check_yyyymmdd),  # Patient Date of Birth. change to check viability of dates?
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
                       'specimenCollectionDate': lambda x: check_col(x, check_yyyymmdd),  # Specimen Collection Date. change to check viability of dates?
                       'specimenReceivedDate': lambda x: check_col(x, check_yyyymmdd),  # Specimen Received Date. check viability?
                       'placerOrder': lambda x: check_col(x, check_alnum, 75),  # Accession Number
                       'specimenType': lambda x: check_col(x, check_charlist, 75),  # uses LUT
                       'labResultDate': lambda x: check_col(x, check_yyyymmdd),  # Test Result Date. check date viability?
                       'resultedTest': lambda x: check_col(x, check_length, 150),  # Test Name todo: ETN mapper wrong
                       'resultedTest_LOINC': lambda x: check_col(x, check_numeric, 75),  # Test LOINC Code TODO: DPS mapper wrong?
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
                       }

## Check which vars to drop?
vars_to_drop = ['Lab Name',
                'File Creation Date',
                'Submission Date',
                'ADDITIONAL_NOTES_1',
                'ADDITIONAL_NOTES_2',
                #'patientMiddleName',
                'performingFacilityAddress_Ln2',
                'patientAddressLine2',
                'Diabetes',
                'HeartCondition',
                'Immunocompromised',
                'KidneyDisease',
                'LiverDisease',
                'LungDisease',
                'Obese',
                'sendingFacility',
                'providerNPI',
                'ProviderAddress_Ln2',
                'SYMPTOMATIC',
                #'patientAddressCounty',  # specified exception for specific labs in lrp_prep_csvs script
                'Pregnant',  # Fulgent mapper issue?
                'DESCRIBE_SYMPTOMS', # only Vault uses this?
                #'PATIENTMIDDLENAME'
                ]

df_dict = import_csv_dict()

for lab_name, df in df_dict.items():
    # Get total case counts per week + lab name
    print('checking ' + lab_name)
    date_counts = pd.DataFrame(df.groupby('Submission Date').size())
    date_counts.columns = ['Total Submission Count']

    date_counts['Lab Name'] = lab_name

    varnames = [col for col in df.columns if col not in vars_to_drop]

    agg_dict = dict((varname, check_function_dict[varname]) for varname in varnames)
    # aggregate using agg dict to check formatting
    df_agg_format = df.groupby('Submission Date').agg(agg_dict)
    df_agg_format = pd.concat([date_counts, df_agg_format], axis=1)

    df_agg_format.to_csv('./data/processed/csv_misformatting/' + lab_name + '_misformat.csv')

    # check missingness
    df_group = df.groupby(['Lab Name', 'Submission Date'])

    miss_count = df_group.size()
    df_miss = df_group.agg(lambda x: x.isnull().sum())
    df_miss['Total Submission Count'] = miss_count
    df_miss.to_csv('./data/processed/csv_missingness/' + lab_name + '_miss.csv')

# # check missingness
# all_df = pd.concat([k for k in df_dict.values()])
# all_df['Submission Date'] = pd.to_datetime(all_df['File Creation Date'])
#
# vars_to_drop = [k for k in vars_to_drop if ((k in all_df.columns) and (k not in ['Lab Name', 'Submission Date']))]
# all_df.drop(vars_to_drop, axis=1, inplace=True)
#
# all_grp = all_df.groupby(['Lab Name','Submission Date'])
# all_miss = all_grp.agg(lambda x: x.isnull().sum())
# miss_count = all_grp.size()
# all_miss['Total Submission Count'] = miss_count
# all_miss.to_csv('./data/processed/csv_missingness/all_miss.csv')





import pandas as pd
import numpy as np
import glob
import datetime
from lrp_format_check_functions import *
from data_loader import *
#import yappi

#yappi.set_clock_type("cpu") # Use set_clock_type("wall") for wall time
#yappi.start()

check_function_dict = {'patientLastName': lambda x: check_charlist(x, 75),  # Patient Last Name.
                       'patientFirstName': lambda x: check_charlist(x, 75),  # Patient First Name.
                       'patientDOB': lambda x: check_yyyymmdd(x),  # Patient Date of Birth. change to check viability of dates?
                       'patientGender': lambda x: check_gender(x),  # Patient Sex
                       'patientId': lambda x: check_alnum(x, 75),  # Patient ID
                       'patientRace': lambda x: check_race(x),  # Patient Race
                       'patientEthnicity': lambda x: check_ethnicity(x),#,  # Patient Ethnicity TODO: KMC missing col
                       'patientAddressLine1': lambda x: check_charlist(x, 75), # Patient Street Address.
                       'patientAddressCity': lambda x: check_charlist(x, 75),  # Patient City.
                       'patientAddressState': lambda x: check_state(x),  # Patient State
                       'patientAddressZip': lambda x: check_numeric_exact(x, 5),  # Patient Zip Code#
                       'PATIENTADDRESSCOUNTY': lambda x: check_charlist(x, 75),  # Patient County TODO: CLS missing col
                       'patientTelephoneNumber': lambda x: check_numeric_exact(x, 10),  # Patient Phone No
                       'Notes': lambda x: check_email(x),  # Patient Email. TODO: DPS mapper wrong. also - why Notes??
                       'specimenCollectionDate': lambda x: check_yyyymmdd(x),  # Specimen Collection Date. change to check viability of dates?
                       'specimenReceivedDate': lambda x: check_yyyymmdd(x),  # Specimen Received Date. check viability?
                       'placerOrder': lambda x: check_alnum(x, 75),  # Accession Number
                       'specimenType': lambda x: check_charlist(x, 75),  # Specimen Source. what about cdc loinc code mapping file?
                       'labResultDate': lambda x: check_yyyymmdd(x),  # Test Result Date. check date viability?
                       'resultedTest': lambda x: check_length(x, 150),  # Test Name todo: ETN mapper wrong
                       'resultedTest_LOINC': lambda x: check_numeric(x, 75),  # Test LOINC Code TODO: DPS mapper wrong?
                       'observation': lambda x: check_observation(x),
                       'performingFacility': lambda x: check_charlist(x, 75),  # Testing Lab Name.
                       'performingFacilityCLIA': lambda x: check_alnum_exact(x, 10),  # Testing Lab CLIA TODO: WHC missing column
                       'performingFacilityAddress': lambda x: check_charlist(x, 75),  # Testing Lab Street Address. TODO: CVS mapper wrong
                       'performingFacility_City': lambda x: check_charlist(x, 75),  # Testing Lab City.
                       'performingFacility_State': lambda x: check_state(x),  # Testing Lab State
                       'performingFacility_Zip': lambda x: check_numeric_exact(x, 5),  # Testing Lab Zip Code
                       'performingFacility_CallBackNumber': lambda x: check_numeric_exact(x, 10),  # Testing Lab Phone No todo: ETN mapper wrong
                       'ProviderName': lambda x: check_charlist(x, 75),  ######## Ordering Provider Last Name. char list? TODO: var redundant with following 2.
                       'providerLastName': lambda x: check_charlist(x, 75),  ######## Ordering Provider Last Name. char list? TODO: redundant?
                       'providerFirstName': lambda x: check_charlist(x, 75),  ####### Ordering Provider First Name. char list? TODO: redundant?
                       'ProviderAddress': lambda x: check_charlist(x, 75),  # Ordering Provider Street Address.
                       'Provider_City': lambda x: check_charlist(x, 75),  # Ordering Provider City.
                       'Provider_State': lambda x: check_state(x),  # Ordering Provider State
                       'Provider_Zip': lambda x: check_numeric_exact(x, 5),  # Ordering Provider Zip Code
                       'Provider_CallBackNumber': lambda x: check_numeric_exact(x, 10),  # Ordering Provider Phone No  TODO: CLS mapping incorrect, check w/ jonathan
                       'orderingFacility': lambda x: check_charlist(x, 75),  # Ordering Facility Name.
                       'orderingFacility_Address': lambda x: check_charlist(x, 75), # Ordering Facility Street Address.
                       'orderingFacility_City': lambda x: check_charlist(x, 75),  # Ordering Facility City
                       'orderingFacility_State': lambda x: check_state(x),  # Ordering Facility State
                       'orderingFacility_Zip': lambda x: check_numeric_exact(x, 5),  # Ordering Facility Zip Code
                       'orderingFacility_CallBackNumber': lambda x: check_numeric_exact(x, 10),  # Ordering Facility Phone No TODO: CLS mapper wrong
                       'isThisFirstTest': lambda x: check_ynuunk(x),  # Patient First Covid Test TODO: what is meant by 'description' or 'code'? Y/N vs Yes/No? Also - move to optional vars
                       'isAHealthcareWorker': lambda x: check_ynuunk(x),  # Patient Employed in Healthcare TODO: as above, optional + desc/code
                       'symptomOnsetDate': lambda x: check_yyyymmdd(x),  # Patient Symptom Onset Date TODO: KMC missing, move to optional vars
                       'isSymptomatic': lambda x: check_ynuunk(x),  # Patient Symptomatic As Defined By CDC TODO: KMC missing col, so optional. also description/code?
                       'isHospitalized': lambda x: check_ynuunk(x),  # Patient Hospitalized TODO: as above, optional + desc/code?
                       'inICU': lambda x: check_ynuunk(x),  # Patient In ICU TODO: as above, optional + desc/code
                       'isPregnant': lambda x: check_ynuunk(x),  # Patient Pregnant TODO: as above, optional + desc/code
                       'isResidentOfCongregateCareSetting': lambda x: check_ynuunk(x),  # Patient Resident in a Congregate Care Setting, todo: as above, optional + desc/code
                       'patientAge': lambda x: check_numeric(x, 3),  # Patient Age at Time of Collection (Years) todo: ETN missing column, move to optional. also KMC mapper is missing this
                       }

dict_keynames = [key for key in check_function_dict]

for labfile in glob.glob('./data/processed/csv_labs_concat/*.csv'):
    print(labfile)
    df = pd.read_csv(labfile, dtype='str')
    df['Submission Date'] = pd.to_datetime(df['File Creation Date'], format='%Y-%m-%d')

    lab_name = df['Lab Name'][0]

    output = pd.DataFrame(columns=['Lab Name', 'Submission Date', 'variable', 'value'])
    cols_to_check = [col for col in df.columns if col in dict_keynames]
    for varname in cols_to_check:
        #print(varname)
        col = df[varname]
        function_to_use = check_function_dict[varname]
        col_output = pd.DataFrame(columns=['Lab Name', 'Submission Date', 'variable', 'value'])
        col_output_labname = []
        col_output_date = []
        col_output_varname = []
        col_output_value = []

        for i, element in enumerate(col):
            #print(i)

            if pd.isna(element):
                continue
            result = function_to_use(str(element))
            if result == 1:
                #print(element)
                col_output_labname.append(lab_name)
                col_output_date.append(df['Submission Date'][i])
                col_output_varname.append(varname)
                col_output_value.append(element)

        col_output['Lab Name'] = col_output_labname
        col_output['Submission Date'] = col_output_date
        col_output['variable'] = col_output_varname
        col_output['value'] = col_output_value
        output = pd.concat([output, col_output])

    output.to_csv('./data/processed/csv_misformat_values/' + lab_name + '.csv', index = False)


df2 = import_hhie_df()

hhie_labs = pd.unique(df2['Lab Name'])

labdf_dict = {}
for lab in hhie_labs:
    df_lab = df2.loc[df2['Lab Name'] == lab].reset_index()
    labdf_dict[lab] = df_lab

del df, df2

check_function_dict = {'patientLastName': lambda x: check_charlist(x, 75),  # Patient Last Name.
                       'patientFirstName': lambda x: check_charlist(x, 75),  # Patient First Name.
                       'patientDOB': lambda x: check_timestamp(x),  # Patient Date of Birth. change to check viability of dates?
                       'patientGender': lambda x: check_gender(x),  # Patient Sex
                       'patientId': lambda x: check_alnum(x, 75),  # Patient ID
                       'patientRace': lambda x: check_race(x),  # Patient Race
                       'patientEthnicity': lambda x: check_ethnicity(x),#,  # Patient Ethnicity TODO: KMC missing col
                       'patientAddressLine1': lambda x: check_charlist(x, 75), # Patient Street Address.
                       'patientAddressCity': lambda x: check_charlist(x, 75),  # Patient City.
                       'patientAddressState': lambda x: check_state(x),  # Patient State
                       'patientAddressZip': lambda x: check_numeric_exact(x, 5),  # Patient Zip Code#
                       'PATIENTADDRESSCOUNTY': lambda x: check_charlist(x, 75),  # Patient County TODO: CLS missing col
                       'patientTelephoneNumber': lambda x: check_numeric_exact(x, 10),  # Patient Phone No
                       'Notes': lambda x: check_email(x),  # Patient Email. TODO: DPS mapper wrong. also - why Notes??
                       'specimenCollectionDate': lambda x: check_timestamp(x),  # Specimen Collection Date. change to check viability of dates?
                       'specimenReceivedDate': lambda x: check_timestamp(x),  # Specimen Received Date. check viability?
                       'placerOrder': lambda x: check_alnum(x, 75),  # Accession Number
                       'specimenType': lambda x: check_charlist(x, 75),  # Specimen Source. what about cdc loinc code mapping file?
                       'labResultDate': lambda x: check_timestamp(x),  # Test Result Date. check date viability?
                       'resultedTest': lambda x: check_length(x, 150),  # Test Name todo: ETN mapper wrong
                       'resultedTest_LOINC': lambda x: check_numeric(x, 75),  # Test LOINC Code TODO: DPS mapper wrong?
                       'observation': lambda x: check_observation(x),  ############# TODO: Result Description or Value. consult with Jonathan
                       'performingFacility': lambda x: check_charlist(x, 75),  # Testing Lab Name.
                       'performingFacilityCLIA': lambda x: check_alnum_exact(x, 10),  # Testing Lab CLIA TODO: WHC missing column
                       'performingFacilityAddress': lambda x: check_charlist(x, 75),  # Testing Lab Street Address. TODO: CVS mapper wrong
                       'performingFacility_City': lambda x: check_charlist(x, 75),  # Testing Lab City.
                       'performingFacility_State': lambda x: check_state(x),  # Testing Lab State
                       'performingFacility_Zip': lambda x: check_numeric_exact(x, 5),  # Testing Lab Zip Code
                       'performingFacility_CallBackNumber': lambda x: check_numeric_exact(x, 10),  # Testing Lab Phone No todo: ETN mapper wrong
                       'ProviderName': lambda x: check_charlist(x, 75),  ######## Ordering Provider Last Name. char list? TODO: var redundant with following 2.
                       'providerLastName': lambda x: check_charlist(x, 75),  ######## Ordering Provider Last Name. char list? TODO: redundant?
                       'providerFirstName': lambda x: check_charlist(x, 75),  ####### Ordering Provider First Name. char list? TODO: redundant?
                       'ProviderAddress': lambda x: check_charlist(x, 75),  # Ordering Provider Street Address.
                       'Provider_City': lambda x: check_charlist(x, 75),  # Ordering Provider City.
                       'Provider_State': lambda x: check_state(x),  # Ordering Provider State
                       'Provider_Zip': lambda x: check_numeric_exact(x, 5),  # Ordering Provider Zip Code
                       'Provider_CallBackNumber': lambda x: check_numeric_exact(x, 10),  # Ordering Provider Phone No  TODO: CLS mapping incorrect, check w/ jonathan
                       'orderingFacility': lambda x: check_charlist(x, 75),  # Ordering Facility Name.
                       'orderingFacility_Address': lambda x: check_charlist(x, 75), # Ordering Facility Street Address.
                       'orderingFacility_City': lambda x: check_charlist(x, 75),  # Ordering Facility City
                       'orderingFacility_State': lambda x: check_state(x),  # Ordering Facility State
                       'orderingFacility_Zip': lambda x: check_numeric_exact(x, 5),  # Ordering Facility Zip Code
                       'orderingFacility_CallBackNumber': lambda x: check_numeric_exact(x, 10),  # Ordering Facility Phone No TODO: CLS mapper wrong
                       'isThisFirstTest': lambda x: check_ynuunk(x),  # Patient First Covid Test TODO: what is meant by 'description' or 'code'? Y/N vs Yes/No? Also - move to optional vars
                       'isAHealthcareWorker': lambda x: check_ynuunk(x),  # Patient Employed in Healthcare TODO: as above, optional + desc/code
                       'symptomOnsetDate': lambda x: check_timestamp(x),  # Patient Symptom Onset Date TODO: KMC missing, move to optional vars
                       'isSymptomatic': lambda x: check_ynuunk(x),  # Patient Symptomatic As Defined By CDC TODO: KMC missing col, so optional. also description/code?
                       'isHospitalized': lambda x: check_ynuunk(x),  # Patient Hospitalized TODO: as above, optional + desc/code?
                       'inICU': lambda x: check_ynuunk(x),  # Patient In ICU TODO: as above, optional + desc/code
                       'isPregnant': lambda x: check_ynuunk(x),  # Patient Pregnant TODO: as above, optional + desc/code
                       'isResidentOfCongregateCareSetting': lambda x: check_ynuunk(x),  # Patient Resident in a Congregate Care Setting, todo: as above, optional + desc/code
                       'patientAge': lambda x: check_numeric(x, 3),  # Patient Age at Time of Collection (Years) todo: ETN missing column, move to optional. also KMC mapper is missing this
                       }

for labname, labdf in labdf_dict.items():
    print('checking ' + labname)

    cols_to_check = [col for col in labdf.columns if col in dict_keynames]

    output = pd.DataFrame(columns=['Lab Name', 'Submission Date', 'variable', 'value'])
    for varname in cols_to_check:
        print(varname)
        col = labdf[varname]
        function_to_use = check_function_dict[varname]
        col_output = pd.DataFrame(columns=['Lab Name', 'Submission Date', 'variable', 'value'])
        col_output_labname = []
        col_output_date = []
        col_output_varname = []
        col_output_value = []
        for j, element in enumerate(col):
            if pd.isna(element):
                continue
            result = function_to_use(str(element))
            if result == 1:
                col_output_labname.append(labname)
                col_output_date.append(labdf['Submission Date'][j])
                col_output_varname.append(varname)
                col_output_value.append(element)

        col_output['Lab Name'] = col_output_labname
        col_output['Submission Date'] = col_output_date
        col_output['variable'] = col_output_varname
        col_output['value'] = col_output_value
        output = pd.concat([output, col_output])

    output.to_csv('./data/processed/hhie_misformat_values/' + labname + '.csv', index=False)

#yappi.get_func_stats().print_all()
#yappi.get_thread_stats().print_all()

from data_loader import *
from lrp_format_check_functions import *

agg_dict = {
    'Total_Count': lambda x: len(x),
    'American Indian or Alaska Native': lambda x: count_race(x, 'I'),
    'Asian': lambda x: count_race(x, 'A'),
    'Black or African American': lambda x: count_race(x, 'B'),
    'Native Hawaiian or Other Pacific Islander': lambda x: count_race(x, 'P'),
    'White': lambda x: count_race(x, 'W'),
    'Other': lambda x: count_race(x, 'O'),
    'Unknown or Refused to Answer': lambda x: count_race(x, 'U'),
    'Misformatted': lambda x: count_race(x, 'misformat'),
    'Missing': lambda x: count_race(x, 'missing')
}


def compile_csv_race_data():
    print('compiling race data for csv files')
    for labfile in glob.glob('./data/processed/csv_labs_concat/*.csv'):
        print('starting csv ' + labfile)
        df = pd.read_csv(labfile, dtype='str')
        df['Submission Date'] = pd.to_datetime(df['File Creation Date'], format='%Y-%m-%d')

        # Get total case counts per week + lab name
        lab_name = df['Lab Name'][0]
        try:
            df = df[['Submission Date', 'patientRace']]
        except KeyError:
            try:
                df = df[['Submission Date', 'PATIENTRACE']]
                df.columns = ['Submission Date', 'patientRace']
            except KeyError:
                df['patientRace'] = np.NAN

        df2 = df.groupby('Submission Date')
        grouped_race = df2['patientRace']
        # try:
        #     grouped_race = df2['patientRace']
        # except KeyError:
        #     df['patientRace'] = np.NAN
        #     df2 = df.groupby('Submission Date')
        #     grouped_race = df2['patientRace']

        output = pd.DataFrame()
        for key, val in agg_dict.items():
            agg_col = grouped_race.agg(var=val)
            output = pd.concat([output, agg_col], axis=1)

        output.columns = [var for var in agg_dict.keys()]

        output['Lab Name'] = lab_name
        # AI = grouped_race.agg(American_Indian=lambda x: count_race(x, 'I'))
        # B = grouped_race.agg(Black=lambda x: count_race(x, 'B'))
        #
        # output = pd.concat([AI, B], axis= 1)
        output.to_csv('./data/processed/csv_race_counts/' + lab_name + '.csv', index=True)

        misformatted_counts = count_misformat_race(df['patientRace'])
        misformatted_counts.columns = ['patientRace', 'count']

        misformatted_counts.to_csv('./data/processed/csv_misformat_race_counts/' + lab_name + '.csv', index=True)


def prep_hl7():
    print('loading HL7 data...')
    df_hl7 = import_hl7_df()

    all_labs = pd.unique(df_hl7['Lab Name'])
    lab_data_list = []
    for lab in all_labs:
        labdf = df_hl7.loc[df_hl7['Lab Name'] == lab]
        lab_data_list.append([lab, labdf])

    return lab_data_list


def process_hl7(labitem):
    lab_name = labitem[0]
    print(lab_name)
    lab_df = labitem[1]
    lab_df.drop(['Lab Name'], axis=1, inplace=True)
    group_df = lab_df.groupby('Submission Date')
    grouped_race = group_df['patientRaceCode']
    output = pd.DataFrame()
    for key, val in agg_dict.items():
        agg_col = grouped_race.agg(val)
        output = pd.concat([output, agg_col], axis=1)

    output.columns = [var for var in agg_dict.keys()]
    output['Lab Name'] = lab_name
    output.reset_index(inplace=True)
    output.rename(columns={'index': 'Submission Date'}, inplace=True)
    output.to_csv('./data/processed/hl7_race_counts/' + lab_name + '.csv', index=False)

    #misformatted_counts = count_misformat_race(lab_df['patientRaceCode'])
    #misformatted_counts.columns = ['patientRace', 'count']
    #misformatted_counts.to_csv('./data/processed/hl7_misformat_race_counts/' + lab_name + '.csv', index=False)


def compile_hl7_race_parallel():
    labdfs = prep_hl7()
    # print('starting multiprocess')
    # with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
    #    data = pool.map(process_hl7, labdfs)
    for labitem in labdfs:
        process_hl7(labitem)


def compile_ecr_race_data():
    ecr_df = import_ecr('df')
    ecr_df = ecr_df[['report_time', 'race_name', 'facility_id', 'facility_name']]

    facility_lookup = {'1.2.840.114350.1.13.105.2.7.2.686980': 'ECR - HPH',
                       '1.2.840.114350.1.13.114.2.7.2.686980': 'ECR - Kaiser',
                       '1.2.840.114350.1.13.133.2.7.2.686980': 'ECR - Queens',
                       np.nan: 'ECR - No Facility ID'
                       }

    ecr_df['Lab Name'] = ecr_df['facility_id'].map(facility_lookup)

    ecr_df.rename(columns={'report_time': 'Submission Date',
                           'race_name': 'patientRace'
                           },
                  inplace=True)

    ecr_lab_list = pd.unique(ecr_df['Lab Name'])
    ecr_data_list = []
    for lab_name in ecr_lab_list:
        print(lab_name)
        lab_df = ecr_df.loc[ecr_df['Lab Name'] == lab_name]
        # ecr_data_list.append([lab, lab_df])
        ecr_grouped = lab_df.groupby('Submission Date')
        ecr_grouped_race = ecr_grouped['patientRace']
        ecr_output = pd.DataFrame()
        for key, val in agg_dict.items():
            agg_col = ecr_grouped_race.agg(var=val)
            ecr_output = pd.concat([ecr_output, agg_col], axis=1)

        ecr_output.columns = [var for var in agg_dict.keys()]
        ecr_output['Lab Name'] = lab_name

        ecr_output.to_csv('./data/processed/ecr_race_counts/' + lab_name + '.csv', index=True)

        misformatted_counts = count_misformat_race(lab_df['patientRace'])
        misformatted_counts.columns = ['patientRace', 'count']

        misformatted_counts.to_csv('./data/processed/ecr_misformat_race_counts/' + lab_name + '.csv', index=True)




if __name__ == '__main__':
    compile_csv_race_data()
    compile_hl7_race_parallel()
    #compile_ecr_race_data()

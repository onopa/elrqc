import pandas as pd
import datetime
import glob
from lrp_format_check_functions import *
import multiprocessing
from data_loader import *




# prepares raw data for processing. loads data from excel spreadsheet, renames variables, and returns a list of tuples
# [(lab name, lab data)]. could have been a dict I suppose. Also, calculate missingness with a quick agg call and
# write to file.
def prep_hl7_data_list():
    # read data
    hl7_df = import_hl7_df()
    all_labs = pd.unique(hl7_df['Lab Name'])
    labdfs = []
    # check and write missignness while it's convenient, and subset labs. return list of [labname, labdf] pairs for multiprocess
    for lab in all_labs:
        lab_df = hl7_df.loc[hl7_df['Lab Name'] == lab]
        labdfs.append([lab, lab_df])
        lab_grp = lab_df.groupby(['Submission Date'])
        miss_count = lab_grp.agg(lambda x: x.isnull().sum())
        miss_total = pd.DataFrame(lab_grp.size())
        miss_count['Total Submission Count'] = miss_total
        miss_count['Lab Name'] = lab
        miss_count.to_csv('./data/processed/hl7_missingness/' + lab + '.csv')

    # Add Denise data pull - DEPRECATED by addition of HL7
    # df = import_hhie_df()
    #
    # #vars_to_drop = ['Submission Date']
    # all_labs = pd.unique(df['Lab Name'])
    #
    # # check missignness
    # for lab in all_labs:
    #     lab_df = df.loc[df['Lab Name'] == lab]
    #     labdfs.append([lab, lab_df])
    #     lab_grp = lab_df.groupby(['Submission Date'])
    #     miss_count = lab_grp.agg(lambda x: x.isnull().sum())
    #     miss_total = pd.DataFrame(lab_grp.size())
    #     miss_count['Total Submission Count'] = miss_total
    #     miss_count['Lab Name'] = lab
    #     miss_count.to_csv('./data/processed/hhie_missingness/' + lab + '.csv')

    return labdfs


# This is what the multiprocess does.
# Take a tuple of (labname, df), groups df by submission date, and run format checks on data with an agg.
# Writes results to csv. Returning the result isn't necessary, but I have it anyway to fit the pool.map format
def run_format_checks(input_list):
    labname = input_list[0]
    labdf = input_list[1]

    varnames = labdf.columns
    print('starting ' + labname)

    date_counts = pd.DataFrame(labdf.groupby('Submission Date').size())
    date_counts.columns = ['Total Submission Count']
    date_counts['Lab Name'] = labname

    vars_to_drop = ['Submission Date', 'Lab Name']
    check_col_dict = get_check_col_dict_hl7()
    agg_dict = dict((varname, check_col_dict[varname]) for varname in varnames if varname in check_col_dict.keys())
    labdf_agg = labdf.groupby('Submission Date').agg(agg_dict)
    # add columns for lab name and total count
    labdf_agg = pd.concat([date_counts, labdf_agg], axis=1)
    lab2 = labname.replace('/', '_')
    labdf_agg.to_csv('./data/processed/hl7_misformatting/' + lab2 + '_misformat.csv')
    print('finished ' + labname)
    return labdf_agg


# Function to process HL7 data. Gets data prepped, then runs the multiprocess
def check_hl7_parallel():
    labdfs = prep_hl7_data_list()

    print('starting multiprocess check of HL7 data formatting')
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        data = pool.map(run_format_checks, labdfs)
    pd.concat(data, axis=1).to_csv('./data/processed/hl7_all_misformat.csv')


# Check CSV data (not multiprocess yet).
def check_sftp_lrp():
    csv_df_dict = import_csv_dict()
    check_col_dict = get_check_col_dict_csv()
    for lab_name, df in csv_df_dict.items():
        # Get total case counts per week + lab name
        print('checking ' + lab_name)
        date_counts = pd.DataFrame(df.groupby('Submission Date').size())
        date_counts.columns = ['Total Submission Count']

        date_counts['Lab Name'] = lab_name
        varnames = [col for col in df.columns if col in check_col_dict.keys()]

        # use function dictionary to
        agg_dict = dict((varname, check_col_dict[varname]) for varname in varnames)

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

if __name__ == '__main__':
#     # check SFTP/LRP
    check_sftp_lrp()

#     # check HL7 data
    check_hl7_parallel()
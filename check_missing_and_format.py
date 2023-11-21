import time

import pandas as pd
import datetime
import glob
from lrp_format_check_functions import *
import multiprocessing
from data_loader import *
import config
import tqdm

grouping_var = config.grouping_var
time_var = config.time_var


# This is the actual check method for data missingness and format quality.
# Takes a lab data object, groups the raw df by submission date, then checks the format and missingness with agg calls.
# You could write the results to csv, but the lab data objects get updated with the 2 new data quality DFs and returned.
def run_format_checks(input_data: LabData):
    # check missing:
    tick = time.perf_counter()
    lab_name = input_data.lab_name
    #print(f'checking {lab_name}')
    lab_df = input_data.df
    source_type = input_data.source_type

    # cleaned name
    lab2 = str(lab_name).replace('/', '_')

    var_names = lab_df.columns
    #print('starting ' + lab2)

    # groupby and get total counts for each date
    grouped_lab_df = lab_df.groupby(time_var)
    date_counts = pd.DataFrame(grouped_lab_df.size())
    date_counts.columns = ['Total Submission Count']
    date_counts[grouping_var] = lab_name

    # check missingness
    miss_count_df = grouped_lab_df.agg(lambda x: x.isnull().sum())
    miss_count_df = pd.concat([date_counts, miss_count_df], axis=1)
    input_data.missingness = miss_count_df
    # write to csv
    miss_count_df.to_csv(f'./data/processed/hl7_missingness/{lab2}.csv')

    # check format:

    # get check function dict
    if source_type == 'hl7':
        check_col_dict = get_check_col_dict_hl7()
    elif source_type == 'csv':
        check_col_dict = get_check_col_dict_csv()
    else:
        check_col_dict = get_check_col_dict()

    # subset only check variables that have a check function and are in the dataframe
    agg_dict = dict((var_name, check_col_dict[var_name]) for var_name in var_names if var_name in check_col_dict.keys())
    # do the agg
    lab_df_agg = grouped_lab_df.agg(agg_dict)
    # add columns for lab name and total count
    misformat_counts_df = pd.concat([date_counts, lab_df_agg], axis=1)

    misformat_counts_df.to_csv(f'./data/processed/hl7_misformatting/{lab2}_misformat.csv')
    input_data.misformatting = misformat_counts_df
    tock = time.perf_counter()
    print(f'finished checking {lab_name} in {tock-tick} s')
    return input_data


def run_format_checks2(input_data: LabData):
    tick = time.perf_counter()
    # check missing:
    lab_name = input_data.lab_name
    #print(f'starting checks for {lab_name}')
    lab_df = input_data.df
    source_type = input_data.source_type

    # cleaned name
    lab2 = str(lab_name).replace('/', '_')

    var_names = [col for col in lab_df.columns if col not in [grouping_var, time_var]]
    #print('starting ' + lab2)

    metadata = lab_df[[grouping_var, time_var]]

    # check missingness
    miss_df = lab_df.map(pd.isnull)
    miss_df.drop([time_var, grouping_var], axis=1, inplace=True)
    miss_df = pd.concat([metadata, miss_df], axis=1)
    input_data.missingness = miss_df
    # write to csv
    miss_df.to_csv(f'./data/processed/{source_type}_missingness_noagg/{lab2}.csv')

    # check format:

    # get check function dict
    if source_type == 'hl7':
        check_dict = get_check_col_dict_hl7_2()
    elif source_type == 'csv':
        check_dict = get_check_col_dict_csv_2()
    else:
        check_dict = get_check_col_dict2()

    # subset only check variables that have a check function and are in the dataframe
    check_dict = dict((var_name, check_dict[var_name]) for var_name in var_names if var_name in check_dict.keys())
    # run the checks
    numrows = len(lab_df)
    numcols = len(check_dict)
    misformat_df = pd.DataFrame(index=range(numrows), columns=list(check_dict.keys()))
    misformat_values_df = pd.DataFrame(columns=['Variable Name', 'Value'])
    for var_name in check_dict.keys():
        check_function = check_dict[var_name]
        col_to_check = lab_df[var_name]
        output_col = []
        misformat_varnames = []
        misformat_vals = []

        # checked_col = col_to_check.map(check_dict[var_name])
        # misformat_df[var_name] = checked_col
        for i, element in enumerate(col_to_check):
            checked_val = check_function(element)
            output_col.append(checked_val)
            if checked_val == 1:
                misformat_vals.append(element)
                misformat_varnames.append(var_name)

        var_misformatted = pd.DataFrame(list(zip(misformat_varnames, misformat_vals)), columns=['Variable Name', 'Value'])
        var_misformatted = var_misformatted.groupby(['Variable Name'], as_index=False).value_counts()
        misformat_values_df = pd.concat([misformat_values_df, var_misformatted], axis=0)

        misformat_df[var_name] = output_col

    # add columns for lab name and total count
    misformat_df = pd.concat([metadata, misformat_df], axis=1)

    #misformat_df.to_csv(f'./data/processed/{source_type}_misformatting_noagg/{lab2}_misformat.csv')
    input_data.misformatting = misformat_df
    input_data.misformat_values = misformat_values_df
    tock = time.perf_counter()

    #print(f'finished checking {lab_name} in {tock-tick: .2f} seconds', flush=True)
    return input_data

# Function to process HL7 data. Gets data prepped, then runs the multiprocess
# def check_hl7_parallel():
#     labdfs = prep_hl7_data_list()
#
#     print('starting multiprocess check of HL7 data formatting')
#     with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
#         data = pool.map(run_format_checks, labdfs)
#     pd.concat(data, axis=1).to_csv('./data/processed/hl7_all_misformat.csv')

#
# # Check missingness and misformatting for CSV data (not multiprocess yet).
# def check_sftp_lrp():
#     # get data as {labname: labdf} dict
#     csv_df_dict = import_csv_dict()
#     # get dictionary of check functions for agg
#     check_col_dict = get_check_col_dict_csv()
#     # iterate thru labs
#     for lab_name, df in csv_df_dict.items():
#         # Get total case counts per week + lab name
#         print('checking ' + lab_name)
#         # agg by submission data
#         date_counts = pd.DataFrame(df.groupby('Submission Date').size())
#         date_counts.columns = ['Total Submission Count']
#         date_counts['Lab Name'] = lab_name
#         varnames = [col for col in df.columns if col in check_col_dict.keys()]
#
#         # use function dictionary to
#         agg_dict = dict((varname, check_col_dict[varname]) for varname in varnames)
#
#         # aggregate using agg dict to check formatting
#         df_agg_format = df.groupby('Submission Date').agg(agg_dict)
#         df_agg_format = pd.concat([date_counts, df_agg_format], axis=1)
#
#         # export
#         df_agg_format.to_csv('./data/processed/csv_misformatting/' + lab_name + '_misformat.csv')
#
#         # check missingness
#         df_group = df.groupby(['Lab Name', 'Submission Date'])
#         miss_count = df_group.size()
#         df_miss = df_group.agg(lambda x: x.isnull().sum())
#         df_miss['Total Submission Count'] = miss_count
#         df_miss.to_csv('./data/processed/csv_missingness/' + lab_name + '_miss.csv')


def check_data_quality(lab_data_list):
    cpu_count = multiprocessing.cpu_count()
    print(f'running data quality checks on {cpu_count} cores')
    with multiprocessing.Pool(processes=cpu_count) as pool:
        total = len(lab_data_list)
        checked_data = list(tqdm.tqdm(pool.imap_unordered(run_format_checks2, lab_data_list), total=total))
    del lab_data_list
    # test w/o multiprocess
    # checked_data = []
    # for lab in lab_data_list:
    #     checked_data.append(run_format_checks2(lab))
    #     print('checked ' + lab.get_name())
    # print('test')
    return checked_data



if __name__ == '__main__':
    data = import_hl7_data()
    lab_data = check_data_quality(data)

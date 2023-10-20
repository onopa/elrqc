import pandas as pd
import os
import sys
import glob
import re
import datetime
from datetime import datetime

#def extract_parentheses(str):
#    pattern = r'\((.*?)\)'
#    match = re.search(pattern, str)
#        if match:
#            results.append(match.group(1))
#    return results


def combine_lrp_csvs():
    lab_names_sheet = pd.read_excel('./lookups/File_Mapper_LAB_CSV_v2.xlsx', sheet_name='File_Mapper_LAB_CSV - Copy')
    lab_mapper_sheet = pd.read_excel('./lookups/File_Mapper_LAB_CSV_v2.xlsx', sheet_name='ID To Mapper')
    mapping_folder = './lookups/ELR Mappings by Lab/'

    elr_csv_folder = './data/raw/ELR SFTP Files 05.05.2023/'
    lab_folders = glob.glob(elr_csv_folder + '*')
    # remove labs that weren't onboarded
    labs_to_remove = ['./data/raw/ELR SFTP Files 05.05.2023\\Coast Diagnostics (CST)', './data/raw/ELR SFTP Files 05.05.2023\\Verily (VLS)']
    lab_folders = [e for e in lab_folders if e not in labs_to_remove]

    # for each lab, append all csv files
    all_lab_df = pd.DataFrame()
    for lab_folder in lab_folders:
        # Rename variables using mapper csv. if multiple mappers exist (e.g. *_OLD.csv) try one at a time
        lab_name = lab_folder.split("\\")[-1]
        print(lab_name)
        lab_abbrev = lab_names_sheet.loc[lab_names_sheet['Facility Name'] == lab_name, 'Lab SFTP ID IN'].values[0]
        lab_mapper_path = lab_mapper_sheet.loc[lab_mapper_sheet['Lab SFTP ID IN'] == lab_abbrev, 'MapperFileName'].values[0]
        lab_mapper_basename = os.path.splitext(os.path.basename(lab_mapper_path))[0]  # Get the base name of the file
        lab_mapper_paths = glob.glob('./lookups/ELR Mappings by Lab/' + lab_mapper_basename + '*') # hopefully includes old/alternate versions
        lab_mappers = []
        for mapper_count, mapper_path in enumerate(lab_mapper_paths):
            lab_mapper = pd.read_csv(mapper_path, encoding="ISO-8859-1")[['ELR_DataElement', 'CSVColumnName']]
            lab_mapper['CSVColumnName'].loc[lab_mapper['CSVColumnName'].isna()] = lab_mapper['ELR_DataElement'].loc[lab_mapper['CSVColumnName'].isna()].apply(str.upper)
            #lab_mapper = lab_mapper[~lab_mapper['CSVColumnName'].isna()]
            lab_mapper['CSVColumnName'] = lab_mapper['CSVColumnName'].apply(str.upper)
            lab_mapper['CSVColumnName'] = lab_mapper['CSVColumnName'].str.replace(' ', '')
            varname_dict = dict(zip(lab_mapper['CSVColumnName'], lab_mapper['ELR_DataElement']))
            lab_mappers.append(varname_dict)
            if mapper_count == len(lab_mapper_paths)-1:
                extra_mapper = pd.read_csv(mapper_path, encoding="ISO-8859-1")[['ELR_DataElement', 'CSVColumnName']]
                extra_mapper_newnames = extra_mapper['ELR_DataElement']
                extra_mapper_oldnames = extra_mapper_newnames.apply(str.upper)
                extra_mapper_dict = dict(zip(extra_mapper_oldnames, extra_mapper_newnames))
                lab_mappers.append(extra_mapper_dict)
        # read in all files, rename, and concatenate
        lab_files = glob.glob(lab_folder + '/*.csv')
        print('starting to append ' + lab_name)
        lab_df = pd.DataFrame() # main df of key vars
        extra_df = pd.DataFrame() # extra vars not needed but split off
        core_dfs = []
        extra_dfs = []
        for lab_file in lab_files:
            try:
                print('appending ' + lab_file)

                timestamp = os.path.getmtime(lab_file)
                datestamp = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')

                # encodings are weird. try ISO if utf-8 doesn't work
                try:
                    submission_df = pd.read_csv(lab_file, dtype='str', encoding="utf-8", on_bad_lines='skip', index_col=False)
                except pd.errors.ParserWarning:
                    submission_df = pd.read_csv(lab_file, dtype='str', encoding="utf-8", on_bad_lines='skip')
                except UnicodeDecodeError:
                    submission_df = pd.read_csv(lab_file, dtype='str', encoding="ISO-8859-1", on_bad_lines='skip', index_col=False)

                submission_df.dropna(axis=0, how='all', inplace=True)
                # attempt to rename variables with mapper dictionaries
                submission_df.columns = map(str.upper, submission_df.columns)
                submission_df.columns = submission_df.columns.str.replace(' ', '')

                for mapper in lab_mappers:
                    cols = submission_df.columns
                    keys = list(mapper.keys())
                    mapper_use = dict((k, mapper[k]) for k in list(mapper.keys()) if k in submission_df.columns)
                    keys_use = (mapper_use.keys())
                    submission_df.rename(columns=mapper, inplace=True)

                # ETN mapper issue exception
                if lab_name in ('ETN_Production', 'Fulgent (FLG)', 'Helix (HDL)', 'HPH',
                                'Tripler Blood Donor Center (TBD)', 'Vault (VMS)', 'Walgreens', 'WHCHC', 'PWN Health'):
                    submission_df.rename(columns={'patientAddressCounty': 'PATIENTADDRESSCOUNTY'}, inplace=True)

                # split core and extra variables
                core_var_names = list(pd.read_csv(lab_mapper_paths[0], encoding="ISO-8859-1")['ELR_DataElement'])
                core_var_names_upper = list(map(str.upper, core_var_names))

                df_col_names = submission_df.columns.tolist()
                df_col_names_upper = list(map(str.upper, df_col_names))

                #submission_df.columns = df_col_names_upper
                #extra_cols = submission_df.loc[:, ~submission_df.columns.isin(core_var_names_upper)].copy()
                extra_cols = submission_df.loc[:, [k not in core_var_names_upper for k in df_col_names_upper]].copy()
                ## drop empty cols in extra df
                #extra_cols.dropna(how='all', axis=1, inplace=True)

                #submission_df_core = submission_df.loc[:, submission_df.columns.isin(core_var_names_upper)].copy()
                submission_df_core = submission_df.loc[:, [k in core_var_names_upper for k in df_col_names_upper]].copy()
                ## drop empty rows in main df (should I do this? probably?)
                #submission_df_core.dropna(how='all', axis=0, inplace=True)

                submission_df_core['Lab Name'] = lab_name
                submission_df_core['File Creation Date'] = datestamp
                #lab_df = pd.concat([lab_df, submission_df_core])
                core_dfs.append(submission_df_core)
                #extra_df = pd.concat([extra_df, extra_cols])
                extra_dfs.append(extra_cols)
            except pd.errors.EmptyDataError:
                print('no data in ' + lab_file + ', skipping')
            except pd.errors.ParserError:
                print('uh oh, parser error for file ' + lab_file)
        lab_df = pd.concat(core_dfs)
        lab_df.to_csv('./data/processed/csv_labs_concat/' + lab_abbrev + '.csv', index=False)
        all_lab_df = pd.concat([all_lab_df, lab_df])

        if len(extra_dfs) > 0:
            extra_df = pd.concat(extra_dfs)
            extra_df.to_csv('./data/processed/csv_labs_extra_vars/extra_vars_' + lab_abbrev + '.csv', index=False)
    all_lab_df.to_csv('./data/processed/csv_all_labs_concat.csv', index=False)


if __name__ == '__main__':
    combine_lrp_csvs()
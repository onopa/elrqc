from lrp_format_check_functions import *
from data_loader import *
import glob
import os

def compile_csv_errors():
    for labfile in glob.glob('./data/processed/csv_labs_concat/*.csv'):
        print('compiling errors for ' + os.path.basename(labfile))
        df = pd.read_csv(labfile, dtype='str')
        df['Submission Date'] = pd.to_datetime(df['File Creation Date'], format='%Y-%m-%d')

        lab_name = df['Lab Name'][0]

        #output = pd.DataFrame(columns=['Lab Name', 'Submission Date', 'variable', 'value'])
        #output = pd.DataFrame(columns=['Lab Name', 'variable', 'value'])
        check_function_dict = get_check_function_dict_csv()
        dict_keynames = [key for key in check_function_dict]
        cols_to_check = [col for col in df.columns if col in dict_keynames]
        output_list = []
        # check format for every column - iterate through, when value fails formatting test, append value to output list
        for varname in cols_to_check:
            # print(varname)
            col = df[varname]
            function_to_use = check_function_dict[varname]
            #col_output = pd.DataFrame(columns=['Lab Name', 'Submission Date', 'variable', 'value'])
            col_output = pd.DataFrame(columns=['Lab Name', 'Variable', 'Value'])
            col_output_labname = []
            #col_output_date = []
            col_output_varname = []
            col_output_value = []

            for i, element in enumerate(col):
                # print(i)

                if pd.isna(element):
                    continue
                result = function_to_use(str(element))
                if result == 1:
                    # print(element)
                    col_output_labname.append(lab_name)
                    #col_output_date.append(df['Submission Date'][i])
                    col_output_varname.append(varname)
                    col_output_value.append(element)

            col_output['Lab Name'] = col_output_labname
            #col_output['Submission Date'] = col_output_date
            col_output['Variable'] = col_output_varname
            col_output['Value'] = col_output_value
            output_counts = col_output.groupby(['Lab Name', 'Variable'], as_index=False).value_counts()
            output_list.append(output_counts)

        output = pd.concat(output_list)
        output.rename({'count': 'Count'}, inplace=True)
        output.to_csv('./data/processed/csv_misformat_values/' + lab_name + '.csv', index=False)


def compile_hl7_errors():

    df2 = import_hl7_df()
    #df_hhie = import_hhie_df()
    #df2 = pd.concat([df_hhie, df_hl7])

    hl7_labs = pd.unique(df2['Lab Name'])
    labdf_dict = {}
    for lab in hl7_labs:
        df_lab = df2.loc[df2['Lab Name'] == lab].reset_index()
        labdf_dict[lab] = df_lab

    del df2

    check_function_dict = get_check_function_dict_hl7()
    dict_keynames = [key for key in check_function_dict]

    for labname, labdf in labdf_dict.items():
        print('compiling errors for ' + labname)

        cols_to_check = [col for col in labdf.columns if col in dict_keynames]

        #output = pd.DataFrame(columns=['Lab Name', 'Submission Date', 'variable', 'value'])
        output_list = []
        for varname in cols_to_check:
            #print(varname)
            col = labdf[varname]
            function_to_use = check_function_dict[varname]
            #col_output = pd.DataFrame(columns=['Lab Name', 'Submission Date', 'variable', 'value'])
            col_output = pd.DataFrame(columns=['Lab Name', 'Variable', 'Value'])
            col_output_labname = []
            #col_output_date = []
            col_output_varname = []
            col_output_value = []
            for j, element in enumerate(col):
                if pd.isna(element):
                    continue
                result = function_to_use(str(element))
                if result == 1:
                    col_output_labname.append(labname)
                    #col_output_date.append(labdf['Submission Date'][j])
                    col_output_varname.append(varname)
                    col_output_value.append(element)

            col_output['Lab Name'] = col_output_labname
            #col_output['Submission Date'] = col_output_date
            col_output['Variable'] = col_output_varname
            col_output['Value'] = col_output_value
            output_counts = col_output.groupby(['Lab Name', 'Variable'], as_index=False).value_counts()
            output_list.append(output_counts)

        output = pd.concat(output_list)
        output.rename({'count': 'Count'}, inplace=True)
        output.to_csv('./data/processed/hl7_misformat_values/' + labname + '.csv', index=False)

# yappi.get_func_stats().print_all()
# yappi.get_thread_stats().print_all()

if __name__ == '__main__':
    compile_csv_errors()
    compile_hl7_errors()
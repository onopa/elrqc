import pandas as pd
from data_loader import LabData, import_hl7_data
from lrp_format_check_functions import *
from config import time_var, grouping_var
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

agg_dict = {
    #'Total_Count': lambda x: len(x),
    'American Indian or Alaska Native': lambda x: count_race2(x, 'I'),
    'Asian': lambda x: count_race2(x, 'A'),
    'Black or African American': lambda x: count_race2(x, 'B'),
    'Native Hawaiian or Other Pacific Islander': lambda x: count_race2(x, 'P'),
    'White': lambda x: count_race2(x, 'W'),
    'Other': lambda x: count_race2(x, 'O'),
    'Unknown or Refused to Answer': lambda x: count_race2(x, 'U'),
    'Misformatted': lambda x: count_race2(x, 'misformat'),
    'Missing': lambda x: count_race2(x, 'missing')
}


def process_race_data(lab: LabData):
    lab_name = lab.lab_name
    #print(f'starting {lab_name}')
    df = lab.df[[grouping_var, time_var, 'patientRaceCode']]

    output = pd.DataFrame()
    race_col = df['patientRaceCode']
    for key, val in agg_dict.items():
        col = race_col.map(val)
        output = pd.concat([output, col], axis=1)

    output.columns = [var for var in agg_dict.keys()]
    output = pd.concat([df[time_var], output], axis=1)

    lab.race = output
    return lab


def compile_hl7_race_parallel(lab_data_list_input):
    print('starting multiprocess check of race data')
    with Pool(processes=cpu_count()) as pool:
        lab_data_list_output = list(tqdm(pool.imap_unordered(process_race_data, lab_data_list_input), total=len(lab_data_list_input)))
    return lab_data_list_output


def compile_hl7_race_serial(lab_data_list_input):
    output = []
    for lab in lab_data_list_input:
        lab_output = process_race_data(lab)
        output.append(lab_output)
    return output


# if __name__ == '__main__':
#      lab_data_list = import_hl7_data()
#      output_data = compile_hl7_race_parallel(lab_data_list)
#      return output_data

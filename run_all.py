import os
import sys
import config
import time
from hl7_tools import process_hl7s
from data_loader import import_hl7_data
from check_missing_and_format import check_data_quality
from plotly_misformat_and_missing_v2 import generate_qc_plots
from compile_race_counts_v2 import compile_hl7_race_parallel
from plotly_race_barchart_v2 import generate_race_plots
from generate_qc_table import make_qc_tables
from plotly_report_lags_v2 import make_lag_plots

if __name__ == '__main__':
    if not os.path.exists('./data/raw/hl7/aims'):
        os.makedirs('./data/raw/hl7/aims')
    if not os.path.exists('./data/raw/hl7/hhie'):
        os.makedirs('./data/raw/hl7/hhie/')
    if not os.path.exists('./data/processed/hl7'):
        os.makedirs('./data/processed/hl7')

    print('REVIEW CONFIG SETTINGS:')
    print(f'only covid data? {config.covid_select}')
    print(f'grouping var: {config.grouping_var}')
    print(f'time var: {config.time_var}')
    print(f'filter data? {config.filter_option}')
    if config.filter_option:
        print(f'filter variable: {config.filter_var}')
        print(f'filter value: {config.filter_val}')
    print('if this is correct, enter Y. otherwise, go edit config.py to change your settings.')
    run_input = input(">>")
    if run_input not in ("y", "Y", "yes", "YES"):
        sys.exit('bye!')

    print("check for new HL7 data before running QC?")
    print("this will take a long time, because the network drive is slow :(")
    parse_input = input(">>")
    if parse_input in ("y", "Y", "yes", "YES"):
        process_hl7s()

    tick1 = time.perf_counter()
    print('importing data')
    lab_data = import_hl7_data()
    print('checking data quality')
    lab_data = check_data_quality(lab_data)
    print('compiling race counts')
    lab_data = compile_hl7_race_parallel(lab_data)
    print(f'making output folder for timestamp {config.now}')
    os.makedirs(config.output_folder)
    os.makedirs(f'{config.output_folder}/qc_tables')
    print('generating qc plot')
    plotly_qc = generate_qc_plots(lab_data)
    print('generating race plot')
    race_viz = generate_race_plots(lab_data)
    print('making lag plots')
    make_lag_plots(lab_data)
    print('making QC tables')
    make_qc_tables(lab_data)
    tock1 = time.perf_counter()
    print(f'finished QC steps in {tock1 - tick1: .2f} seconds')
    print('done')
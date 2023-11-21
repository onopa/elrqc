from datetime import datetime
import os

now = datetime.now().strftime("%Y%m%d_%H%M%S")
output_folder = f'C:/Users/alexander.onopa/workspace/lrp/elrqc/output/output_{now}'

grouping_var = 'performingFacility'
time_var = 'Submission Date'

filter_option = True
filter_var = 'sendingFacility'
filter_val = 'DLS'

covid_select = True
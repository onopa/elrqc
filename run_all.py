from check_missing_and_format import *
from hl7_tools import *
from compile_formatting_errors import *
from compile_race_counts import *
from combine_LRP_csvs import combine_lrp_csvs
import lrp_format_check_functions

if __name__ == '__main__':
    combine_lrp_csvs()
    move_hl7_elr()
    parse_hl7_hhie()
    parse_hl7_aims()

    check_sftp_lrp()
    check_hl7_parallel()

    compile_csv_errors()
    compile_hl7_errors()

    compile_csv_race_data()
    compile_hl7_race_parallel()



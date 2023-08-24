import pandas as pd
import os
import sys
import glob


def fix_hkn_dates(file_to_fix):
    df = pd.read_csv(file_to_fix)
    for col in df.columns[df.isna().any()].tolist():
        if df[col].dtype == 'float':
            df[col] = df[col].astype('Int64')
    num_nas_before = sum(pd.isna(df['SPECIMEN RECEIVED DATE']))
    if num_nas_before == 0:
        print('no missing specimen received dates in this file.')
        return
    print(str(num_nas_before) + ' missing dates, attempting to fill from specimen collection date')
    df['SPECIMEN RECEIVED DATE'].fillna(df['SPECIMEN COLLECTION DATE'], inplace=True)
    num_nas_after = sum(pd.isna(df['SPECIMEN RECEIVED DATE']))
    print(str(num_nas_after) + ' missing dates after fix, now saving file.')
    df.to_csv(file_to_fix, index=False)


if __name__ == '__main__':
    try:
        # os.chdir('E:/GGGLLC/COVID-19/any2elrProcessing/data/In/HKN_HI/')
        os.chdir('../ELR SFTP Files 05.05.2023/Hawaii Keiki School Nurse Testing (HKN)')
    except FileNotFoundError:
        print('E:/GGGLLC/COVID-19/any2elrProcessing/data/In/HKN_HI/ folder not found.\n'
              'Are you running this on the right computer?')
        sys.exit()
    hkn_files = glob.glob('./*.csv')
    if len(hkn_files) > 0:
        print('fixing dates in ' + str(len(hkn_files)) + ' files:')
        for i, file in enumerate(hkn_files):
            print('checking file ' + str(i+1) + ', ' + file)
            try:
                fix_hkn_dates(file)
            except KeyError:
                print('appropriate columns not found - something wrong with this one.')
    else:
        print("No HKN files to fix.")



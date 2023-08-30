from data_loader import *
import pandas as pd
import datetime as dt

week_prior = dt.datetime.today() - dt.timedelta(days=100)
three_months_prior = dt.datetime.today() - dt.timedelta(weeks=52)

missing_dict = import_missingness('dict')

misformat_dict = import_misformatting('dict')

lab_list = [k for k in missing_dict.keys()]

for lab_name in lab_list:
    print(lab_name)
    lab_missing_df = missing_dict[lab_name]
    lab_missing_df['Submission Date'] = pd.to_datetime(lab_missing_df['Submission Date'])
    lab_misformat_df = misformat_dict[lab_name]
    lab_misformat_df['Submission Date'] = pd.to_datetime(lab_misformat_df['Submission Date'])

    #varnames = list(set(lab_missing_df.columns) & set(lab_misformat_df.columns))
    varnames = [k for k in lab_missing_df.columns if k in lab_misformat_df.columns]
    if lab_missing_df.loc[lab_missing_df['Submission Date'] >= week_prior].size == 0:
        continue

    curr_missing = lab_missing_df.loc[lab_missing_df['Submission Date'] >= week_prior]
    curr_misformat = lab_misformat_df.loc[lab_misformat_df['Submission Date'] >= week_prior]
    curr_total = sum(curr_missing['Total Submission Count'])

    prev_missing = lab_missing_df.loc[(lab_missing_df['Submission Date'] < week_prior) |
                                      (lab_missing_df['Submission Date'] >= three_months_prior)]
    prev_misformat = lab_misformat_df.loc[(lab_misformat_df['Submission Date'] < week_prior) |
                                          (lab_misformat_df['Submission Date'] >= three_months_prior)]
    prev_total = sum(prev_missing['Total Submission Count'])

    curr_miss_count_list = []
    curr_miss_pct_list = []

    curr_misf_count_list = []
    curr_misf_pct_list = []

    curr_problem_count_list = []
    curr_quality_pct_list = []

    prev_miss_pct_list = []
    prev_misf_pct_list = []
    prev_quality_pct_list = []

    vars_to_check = [k for k in varnames if k not in ('Lab Name', 'Submission Date', 'Total Submission Count')]
    for varname in vars_to_check:
        curr_miss_count = sum(curr_missing[varname])
        curr_miss_count_list.append(curr_miss_count)
        curr_miss_pct = str(round(curr_miss_count/curr_total * 100, 1)) + '%'
        curr_miss_pct_list.append(curr_miss_pct)

        curr_misf_count = sum(curr_misformat[varname])
        curr_misf_count_list.append(curr_misf_count)
        curr_misf_pct = str(round(curr_misf_count/curr_total * 100, 1)) + '%'
        curr_misf_pct_list.append(curr_misf_pct)

        curr_problem_count = curr_miss_count + curr_misf_count
        curr_problem_count_list.append(curr_problem_count)
        curr_quality_pct = str(round((1 - curr_problem_count/curr_total) * 100,1)) + '%'
        curr_quality_pct_list.append(curr_quality_pct)

        prev_miss_count = sum(prev_missing[varname])
        prev_miss_pct = str(round(prev_miss_count/prev_total * 100, 1)) + '%'
        prev_miss_pct_list.append(prev_miss_pct)

        prev_misf_count = sum(prev_misformat[varname])
        prev_misf_pct = str(round(prev_misf_count/prev_total * 100, 1)) + '%'
        prev_misf_pct_list.append(prev_misf_pct)
        prev_problem_count = prev_miss_count + prev_misf_count
        prev_quality_pct = str(round((1 - (prev_problem_count/prev_total)) * 100, 1)) + '%'
        prev_quality_pct_list.append(prev_quality_pct)

        curr_valid_count = curr_total - curr_problem_count
        curr_valid_pct = curr_valid_count/curr_total

        prev_valid_count = prev_total - prev_problem_count
        prev_valid_pct = prev_valid_count/prev_total

    total_count_list = [curr_total] * len(prev_quality_pct_list)
    qcdf = pd.DataFrame({'Variable Name': vars_to_check,
                         'Total Weekly<br>Submissions': total_count_list,
                         'Total Missing': curr_miss_count_list,
                         '% Missing': curr_miss_pct_list,
                         'Total<br>Misformatted': curr_misf_count_list,
                         '% Misformatted': curr_misf_pct_list,
                         'Total Issues': curr_problem_count_list,
                         '% Quality': curr_quality_pct_list,
                         'Previous 12 Weeks<br>% Missing': prev_miss_pct_list,
                         'Previous 12 Weeks<br>% Misformat': prev_misf_pct_list,
                         'Previous 12 Weeks<br>% Quality': prev_quality_pct_list})

    qcdf.to_csv('./data/processed/qc_tables/' + lab_name + '.csv', index=False)
    # qcdf.style.set_table_styles([
    #   {"selector": "td, th", "props": [("border", "1px solid grey !important")]},
    # ])

    table_styles = [{"selector": "", "props": [("border", "1px solid grey"),
                                               ("font-family", "calibri"),
                                               ]},
                    {"selector": "tbody td", "props": [("border", "1px solid grey"),]},
                    {"selector": "th", "props": [("border", "1px solid grey"), ("background-color", "lightblue")]},
                    {"selector": "", "props": [("border-collapse", "collapse")]},
                    {"selector": "tbody tr:nth-child(even)", "props": [("background-color", "#f4f4f4")]},
                    ]


    check_fn_5percent = lambda x: ["background-color: #ff6060" if (i in [3, 5] and (float(v.replace("%", ""))/100 > 0.05))
                            or (i in [7] and float(v.replace("%", "")) < 95)
                            else "" for i, v in enumerate(x)]

    # y = lambda x: for i, v in enumerate(x):
    #     if i in [3, 5] and float(v.replace("%", ""))/100 > 0.05
    # ["background-color: #ff6060"] if (i in [7] and float(v.replace("%", "")) < 95)

    styled_qcdf = qcdf.style.apply(check_fn_5percent, axis=1).set_table_styles(table_styles).hide(axis="index")

    # styled_qcdf = qcdf.style.set_properties(**{"font-family": "arial",
    #                                            'border-style': 'solid',
    #                                            'border-collapse': 'collapse',
    #                                            'border-width': '1px'}).render()


    styled_qcdf.to_html('./data/processed/qc_tables/' + lab_name + '.html', index=False)
    #qcdf.to_html('./data/processed/qc_tables/' + lab_name + '.html', index=False)
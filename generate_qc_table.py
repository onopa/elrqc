from data_loader import LabData
import pandas as pd
import datetime as dt
from data_loader import import_hl7_data
from check_missing_and_format import check_data_quality
import config

def make_qc_tables(lab_data_list):

    week_prior = dt.datetime.today() - dt.timedelta(weeks=1)
    three_months_prior = dt.datetime.today() - dt.timedelta(weeks=12)

    qcmatrix_lab_list = []
    qcmatrix_miss_list = []
    qcmatrix_misf_list = []
    submission_count_list = []

    for lab in lab_data_list:
        lab: LabData
        lab_name = lab.lab_name
        lab_missing_df = lab.missingness
        lab_missing_df[config.time_var] = pd.to_datetime(lab_missing_df[config.time_var])
        lab_misformat_df = lab.misformatting
        lab_misformat_df[config.time_var] = pd.to_datetime(lab_misformat_df[config.time_var])

        if lab_missing_df.loc[lab_missing_df[config.time_var] >= week_prior].size == 0:
            continue


        varnames = [k for k in lab_missing_df.columns if k in lab_misformat_df.columns]


        curr_missing = lab_missing_df.loc[lab_missing_df[config.time_var] >= week_prior]
        curr_misformat = lab_misformat_df.loc[lab_misformat_df[config.time_var] >= week_prior]
        curr_total = len(curr_missing)

        submission_count_list.append(curr_total)

        prev_missing = lab_missing_df.loc[(lab_missing_df[config.time_var] < week_prior) |
                                          (lab_missing_df[config.time_var] >= three_months_prior)]
        prev_misformat = lab_misformat_df.loc[(lab_misformat_df[config.time_var] < week_prior) |
                                              (lab_misformat_df[config.time_var] >= three_months_prior)]
        prev_total = len(prev_missing)

        curr_miss_count_list = []
        curr_miss_pct_list = []

        curr_misf_count_list = []
        curr_misf_pct_list = []

        curr_problem_count_list = []
        curr_quality_pct_list = []

        prev_miss_pct_list = []
        prev_misf_pct_list = []
        prev_quality_pct_list = []

        vars_to_check = [k for k in varnames if k not in ('Lab Name', config.grouping_var, config.time_var, 'Total Submission Count')]
        for varname in vars_to_check:
            curr_miss_count = sum(curr_missing[varname])
            curr_miss_count_list.append(curr_miss_count)
            curr_miss_pct = str(round(curr_miss_count / curr_total * 100, 1)) + '%'
            curr_miss_pct_list.append(curr_miss_pct)

            curr_misf_count = sum(curr_misformat[varname])
            curr_misf_count_list.append(curr_misf_count)
            curr_misf_pct = str(round(curr_misf_count / curr_total * 100, 1)) + '%'
            curr_misf_pct_list.append(curr_misf_pct)

            curr_problem_count = curr_miss_count + curr_misf_count
            curr_problem_count_list.append(curr_problem_count)
            curr_quality_pct = str(round((1 - curr_problem_count / curr_total) * 100, 1)) + '%'
            curr_quality_pct_list.append(curr_quality_pct)

            prev_miss_count = sum(prev_missing[varname])
            prev_miss_pct = str(round(prev_miss_count / prev_total * 100, 1)) + '%'
            prev_miss_pct_list.append(prev_miss_pct)

            prev_misf_count = sum(prev_misformat[varname])
            prev_misf_pct = str(round(prev_misf_count / prev_total * 100, 1)) + '%'
            prev_misf_pct_list.append(prev_misf_pct)
            prev_problem_count = prev_miss_count + prev_misf_count
            prev_quality_pct = str(round((1 - (prev_problem_count / prev_total)) * 100, 1)) + '%'
            prev_quality_pct_list.append(prev_quality_pct)

            curr_valid_count = curr_total - curr_problem_count
            curr_valid_pct = curr_valid_count / curr_total

            prev_valid_count = prev_total - prev_problem_count
            prev_valid_pct = prev_valid_count / prev_total

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

        qcmatrix_lab_list.append(lab_name)

        lab_qcmatrix_miss = pd.DataFrame(curr_miss_pct_list).T
        lab_qcmatrix_miss.columns = vars_to_check
        qcmatrix_miss_list.append(lab_qcmatrix_miss)

        lab_qcmatrix_misf = pd.DataFrame(curr_misf_pct_list).T
        lab_qcmatrix_misf.columns = vars_to_check
        qcmatrix_misf_list.append(lab_qcmatrix_misf)

        qcdf.to_csv(f'{config.output_folder}/qc_tables/{lab_name}.csv', index=False)
        # qcdf.style.set_table_styles([
        #   {"selector": "td, th", "props": [("border", "1px solid grey !important")]},
        # ])

        table_styles = [{"selector": "", "props": [("border", "1px solid grey"),
                                                   ("font-family", "calibri"),
                                                   ]},
                        {"selector": "tbody td", "props": [("border", "1px solid grey"), ]},
                        {"selector": "th", "props": [("border", "1px solid grey"), ("background-color", "lightblue")]},
                        {"selector": "", "props": [("border-collapse", "collapse")]},
                        {"selector": "tbody tr:nth-child(even)", "props": [("background-color", "#f4f4f4")]},
                        ]

        check_fn_5percent = lambda x: [
            "background-color: #ff6060" if (i in [3, 5] and (float(v.replace("%", "")) / 100 > 0.05))
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

        styled_qcdf.to_html(f'{config.output_folder}/qc_tables/{lab_name}.html', index=False)
        # qcdf.to_html('./data/processed/qc_tables/' + lab_name + '.html', index=False)

    check_fn_matrix = lambda x: ["background-color: #ff6060" if (i > 1)  # and (float(v.replace("%", ""))/100 > 0.05))
                                                                or (i in [7] and float(v.replace("%", "")) < 95)
                                 else "" for i, v in enumerate(x)]

    if len(qcmatrix_miss_list) == 0:
        print('no recent tests, not generating QC tables')
        return -1

    qcmatrix_miss = pd.concat(qcmatrix_miss_list)
    qcmatrix_miss.reset_index(drop=True, inplace=True)
    for col in qcmatrix_miss.columns:
        qcmatrix_miss[col] = qcmatrix_miss[col].str.rstrip('%').astype('float') / 100.0
    qcmatrix_miss.insert(loc=0, column='Lab_Name', value=pd.Series(qcmatrix_lab_list))
    qcmatrix_miss.insert(loc=1, column='Submission Count', value=pd.Series(submission_count_list))

    # qcmatrix_miss_styled = qcmatrix_miss.style.format(precision=2).\
    # q

    import pandas.io.formats.excel
    pandas.io.formats.excel.header_style = None

    qcmatrix_miss_styled = qcmatrix_miss.style.background_gradient(cmap='Blues', vmin=0, vmax=1, subset=vars_to_check). \
        format(precision=2). \
        set_properties(**{'max-width': '10px', 'font-size': '10pt',
                          'text-align': 'left'}). \
        set_caption('Missing'). \
        set_table_styles([{'selector': 'th', 'props': [('font-size', '10pt',),
                                                       ('text-align', 'left')]},
                          {'selector': 'caption', 'props': [('text-align', 'left'),
                                                            ('font-size', '14pt')]}
                          ])

    # qcmatrix_start = pd.DataFrame([zip(qcmatrix_lab_list, submission_count_list)], columns = ['Lab Name', 'Submission Count'])
    # qcmatrix_miss_styled = qcmatrix_start.style.format().concat(qcmatrix_miss_styled)

    qcmatrix_misf = pd.concat(qcmatrix_misf_list)
    qcmatrix_misf.reset_index(drop=True, inplace=True)
    for col in qcmatrix_misf.columns:
        qcmatrix_misf[col] = qcmatrix_misf[col].str.rstrip('%').astype('float') / 100.0
    qcmatrix_misf.insert(loc=0, column='Lab_Name', value=pd.Series(qcmatrix_lab_list))
    qcmatrix_misf.insert(loc=1, column='Submission Count', value=pd.Series(submission_count_list))
    qcmatrix_misf_styled = qcmatrix_misf.style.background_gradient(cmap='Reds', vmin=0, vmax=1, subset=vars_to_check). \
        format(precision=2). \
        set_properties(**{'max-width': '10px', 'font-size': '10pt'}). \
        set_caption('Misformat'). \
        set_table_styles([{'selector': 'th', 'props': [('font-size', '10pt')]},
                          {'selector': 'caption', 'props': [('text-align', 'left'),
                                                            ('font-size', '14pt')]}])

    # qcmatrix_misf_styled = qcmatrix_start.style.format().insert(qcmatrix_misf_styled)

    # qcmatrix_miss_styled.to_html('./data/processed/qc_tables/' + 'qc_matrix_missing' + '.html', index=False)
    # qcmatrix_misf_styled.to_html('./data/processed/qc_tables/' + 'qc_matrix_misformat' + '.html', index=False)

    matrix_miss_html = qcmatrix_miss_styled.to_html(index=False)
    matrix_misf_html = qcmatrix_misf_styled.to_html(index=False)

    output_html = matrix_miss_html + '<br>' + matrix_misf_html

    with open(f'{config.output_folder}/qc_tables/all_lab_matrix.html', 'w') as output_file:
        output_file.write(output_html)

    # format header and write to excel
    writer = pd.ExcelWriter(f'{config.output_folder}/qc_tables/all_lab_matrix.xlsx', engine='xlsxwriter')

    miss_header_format = writer.book.add_format({
        'bold': True,
        'font_color': 'white',
        'bg_color': '#08306b',
        'border': 1,
        'align': 'left',
        'valign': 'vcenter'
    })

    misf_header_format = writer.book.add_format({
        'bold': True,
        'font_color': 'white',
        'bg_color': '#67000d',
        'border': 1,
        'align': 'left',
        'valign': 'vcenter'
    })

    qcmatrix_miss_styled.to_excel(writer, sheet_name='Missing', index=False, startrow=1, header=False)

    worksheet_miss = writer.sheets['Missing']
    for col_num, value in enumerate(qcmatrix_miss_styled.columns.values):
        worksheet_miss.write(0, col_num, value, miss_header_format)

    worksheet_miss.freeze_panes(1, 1)

    qcmatrix_misf_styled.to_excel(writer, sheet_name='Misformat', index=False, startrow=1, header=False)

    worksheet_misf = writer.sheets['Misformat']
    for col_num, value in enumerate(qcmatrix_misf_styled.columns.values):
        worksheet_misf.write(0, col_num, value, misf_header_format)

    worksheet_misf.freeze_panes(1, 1)

    writer.close()

if __name__ == '__main__':
    lab_data_list = import_hl7_data()
    lab_data_list = check_data_quality(lab_data_list)
    make_qc_tables(lab_data_list)
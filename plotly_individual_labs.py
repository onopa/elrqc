import plotly.graph_objects as go
import glob
from plotly.subplots import make_subplots
import numpy as np
from data_loader import *
import datetime


df_hhie = import_hhie_df()
#df_csv = import_csv_df()

def convert_resultdate(datestring):
    try:
        out = datetime.datetime.strptime(str(datestring), '%Y%m%d')
        return out
    except ValueError:
        return np.nan

#df_csv['labResultDate'] = df_csv['labResultDate'].apply(convert_resultdate)


#df = pd.concat([df_hhie, df_csv])
df = df_hhie
#del df_hhie, df_csv

miss = import_missingness('df')
misformat = import_misformatting('df')

# filter for recent data?
#df = df.loc[df['Submission Date'] >= datetime.datetime.strptime('2023-01-01', '%Y-%m-%d')]
#miss = miss.loc[miss['Submission Date'] >= datetime.datetime.strptime('2023-01-01', '%Y-%m-%d')]
#misformat = misformat.loc[misformat['Submission Date'] >= datetime.datetime.strptime('2023-01-01', '%Y-%m-%d')]

lab_list = pd.unique(df['Lab Name'])

## calculate reporting lag
# lag = df[['Lab Name', 'Submission Date']].copy()
# rddt = pd.to_datetime(df['labResultDate'])
# lag['Lab Result Date'] = rddt
# lag.reset_index(drop=True, inplace=True)
# deltaDays = []
# for i, val in enumerate(lag['Lab Result Date']):
#     # for misformatted dates, just go with nan
#     try:
#         resultDate = pd.to_datetime(lag['Lab Result Date'][i], format='%Y-%m-%d')
#         delta = pd.to_datetime(lag['Submission Date'][i]) - resultDate
#         deltaDays.append(delta.days)
#     except ValueError:
#         deltaDays.append(np.nan)
#
# lag['Reporting Lag'] = deltaDays
variable_list = list(df.columns)

vars_to_drop = ['Submission Date',
                'Lab Name',
                # 'patientMiddleName',
                # 'ADDITIONAL_NOTES_1',
                # 'ADDITIONAL_NOTES_2',
                # 'performingFacilityAddress_Ln2',
                ]
for v in vars_to_drop:
    variable_list.pop(variable_list.index(v))

dropdown_vars = []
for i, var_choice in enumerate(variable_list):
    vis = [False]* 3*len(variable_list)
    visible_indices = [3*i, 3*i+1, 3*i+2]
    for v in visible_indices:
        vis[v] = True
    dropdown_vars.append(dict(
        args=[{'visible': vis}],
        label=var_choice,
        method='restyle'
    ))

fig = go.Figure()

for lab in lab_list:
    print('starting ' + lab)
    #total/missing/misformat
    lab_miss = miss.loc[miss['Lab Name'] == lab].reset_index(drop=True)
    lab_misf = misformat.loc[misformat['Lab Name'] == lab]
    lab_df = df.loc[df['Lab Name'] == lab]

    # columns = list(lab_df.columns)
    # columns.pop(columns.index('Submission Date'))
    # columns.pop(columns.index('Lab Name'))
    total_submissions = lab_misf['Total Submission Count']
    for i, var in enumerate(variable_list): #columns:
        vis = False
        if i == 0:
            vis = True
        print('starting ' + var)
        total_submissions = lab_misf['Total Submission Count']#.astype(int)
        var_misformat = lab_misf[var]#.astype(int)
        var_miss = lab_miss[var]#.astype(int)
        normal_count = total_submissions - (var_misformat + var_miss)

        submission_dates = lab_misf['Submission Date']

        trace_bar_normal = go.Bar(x=submission_dates, y=normal_count,
                                  marker_color='blue',
                                  offsetgroup=0,
                                  visible=vis,
                                  name='normal ' + var
                                  )

        trace_bar_misf = go.Bar(x=submission_dates, y=var_misformat,
                                marker_color='red', base=normal_count,
                                offsetgroup=0,
                                visible=vis,
                                name='misformat '+ var)
        new_base = normal_count+var_misformat

        trace_bar_miss = go.Bar(x=submission_dates, y=var_miss,
                                marker_color='black', base=new_base,
                                offsetgroup=0,
                                visible=vis,
                                name='missing '+ var)

        fig.add_trace(trace_bar_normal)
        fig.add_trace(trace_bar_misf)
        fig.add_trace(trace_bar_miss)

    # reporting lag
    # use_lag = lag.loc[lag['Lab Name'] == lab].copy()
    # use_lag.drop('Lab Name', axis=1, inplace=True)
    # use_lag['Submission Date'] = use_lag['Submission Date'].apply(datetime.datetime.date)
    # use_lag = use_lag.groupby(['Submission Date', 'Reporting Lag'], as_index=False).count()
    # #use_lag = use_lag.loc[use['Reporting Lag'] > 0]
    # trace_lag = go.Box(y=use_lag['Reporting Lag'], x=use_lag['Submission Date'], #marker_size=use['Lab Result Date'],
    #                    #mode='markers',
    #                    #marker_color='blue', #fillcolor='lightseagreen',
    #                fillcolor='rgba(255,255,255,0)',
    #                boxpoints='all',
    #                opacity=0.66,
    #                pointpos=0,
    #                jitter=0.5,
    #                marker_color='blue',
    #                line_color='rgba(255,255,255,0)',
    #                marker_size=5,
    #                visible=False)


    #fig.update_xaxes(title='Date')
    #fig.update_yaxes(title='Reporting Lag', rangemode='nonnegative')  # , fixedrange = True)
    fig.update_layout(
        title_text=lab,
        # hovermode = 'x',
        height=1000,
        width=850,
        title_y=0.99,
        margin=dict(t=140),
        plot_bgcolor='rgba(0, 0, 0, 0)',
    )

    fig.update_layout(
        updatemenus=[
            # Dropdown menu for choosing the lab
            dict(
                buttons=dropdown_vars,
                direction='down',
                showactive=True,
                x=0.0,
                xanchor='left',
                y=1.11,
                yanchor='top'
            )
        ]
    )

    fig.show()
    fig.write_html("./viz/lab_specific_plots/" + lab + ".html")
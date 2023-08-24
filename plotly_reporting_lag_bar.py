import pandas as pd
from datetime import datetime, timedelta
import datetime as dt
import glob
import plotly.graph_objects as go
import numpy as np
from data_loader import *


df_hhie = import_hhie_df()
df_csv = import_csv_df()

def convert_resultdate(datestring):
    try:
        out = datetime.datetime.strptime(str(datestring), '%Y%m%d')
        return out
    except ValueError:
        return np.nan


print('converting dates')
#df_csv['labResultDate'] = df_csv['labResultDate'].apply(convert_resultdate)
df_hhie['Submission Date'] = pd.to_datetime(df_hhie['Submission Date'], errors='coerce')
df_hhie = df_hhie.loc[df_hhie['Submission Date'] > '2023-01-01 ']

df_csv['labResultDate'] = pd.to_datetime(df_csv['labResultDate'], errors='coerce')
df_csv = df_csv.loc[df_csv['Submission Date'] > '2023-01-01']

df_hhie['labResultDate'] = pd.to_datetime(df_hhie['labResultDate'], errors='coerce')

df = pd.concat([df_hhie, df_csv])
df.reset_index(drop=True, inplace=True)

lag_timedelta = df['Submission Date'].dt.date - df['labResultDate'].dt.date
df['Reporting Lag'] = lag_timedelta.dt.days


df = df[['Submission Date', 'Lab Name', 'Reporting Lag']]
df['Reporting Lag Factor'] = np.nan
df.loc[df['Reporting Lag'] <= 1, 'Reporting Lag Factor'] = '1 Day or Less'
df.loc[(df['Reporting Lag'] > 1) & (df['Reporting Lag'] <= 10), 'Reporting Lag Factor'] = '1-10 Days'
df.loc[df['Reporting Lag'] > 10, 'Reporting Lag Factor'] = '10+ Days'

# jitter for swarm plot?
#df['Reporting Lag Jittered'] = df['Reporting Lag'] + np.random.uniform(-0.1,0.1, len(lag_timedelta))
#
# lag = df[['Lab Name', 'Submission Date', 'labResultDate']].copy()
# lag.reset_index(drop=True, inplace=True)
# #result_datetime = pd.to_datetime(df['labResultDate'], errors='coerce')
# lag['Lab Result Date'] = pd.to_datetime(lag['labResultDate'], errors='coerce')
#
# deltaDays = []
# print('computing lags')
# for i, val in enumerate(lag['Lab Result Date']):
#     try:
#         resultDate = pd.to_datetime(lag['Lab Result Date'][i], format='%Y-%m-%d')
#         delta = pd.to_datetime(lag['Submission Date'][i]) - resultDate
#         deltaDays.append(delta.days)
#     except ValueError:
#         deltaDays.append(np.nan)
#
# df['Reporting Lag'] = deltaDays
print('lags computed')

# try to figure out where to use this sort of thing to set color for all plots

#df['markercolor'] = df['Reporting Lag'].apply(setcolor)


# define functions for building x and y variables for each different plot, called when building dropdown
def updateTimePlotX(lab):
    use = df.loc[df['Lab Name'] == lab]
    return use['Submission Date']

def updateTimePlotY(lab, var):
    use = df.loc[df['Lab Name'] == lab]
    return use[var]

# how to use this
def updateColor(lab):
    use = df.loc[df['Lab Name'] == lab]
    return use['markercolor']


availableLabs = df['Lab Name'].unique().tolist()

dropdown_labs = []


for i, lab_choice in enumerate(availableLabs):
    print('building dropdown ' + str(i))
    vis = [False] * 3*len(availableLabs)
    for v in [3*i, 3*i+1, 3*i+2]:
        vis[v] = True
    dropdown_labs.append(dict(
        args=[{'visible': vis}],
        label=lab_choice,
        method='restyle'))

fig = go.Figure()

for i, labname in enumerate(availableLabs):
    print('plotting ' + labname)
    vis = False
    if i == 0:
        vis = True
    labdf = df.loc[df['Lab Name'] == labname]

    grpdf = labdf.groupby('Submission Date', as_index=False)['Reporting Lag Factor'].value_counts()

    df_case1 = grpdf.loc[grpdf['Reporting Lag Factor'] == '1 Day or Less'].copy()
    df_case1.rename(columns={'count': '1 Day or Less'}, inplace=True)
    df_case2 = grpdf.loc[grpdf['Reporting Lag Factor'] == '1-10 Days'].copy()
    df_case2.rename(columns={'count': '2-10 Days'}, inplace=True)
    df_case3 = grpdf.loc[grpdf['Reporting Lag Factor'] == '10+ Days'].copy()
    df_case3.rename(columns={'count': '10+ Days'}, inplace=True)

    df_total = pd.merge(df_case1, df_case2, on='Submission Date', how='outer')
    df_total = pd.merge(df_total, df_case3, on='Submission Date', how='outer')
    df_total.fillna(0, inplace=True)

    trace1 = go.Bar(x=df_total['Submission Date'], y=df_total['1 Day or Less'],
                    marker_color="#4287f5",
                    base=0,
                    showlegend=True,
                    visible=vis,
                    name='1 Day or Less',
                    offsetgroup=0
                    )

    base = df_total['1 Day or Less']

    trace2 = go.Bar(x=df_total['Submission Date'], y=df_total['2-10 Days'],
                    marker_color='#f5da42',
                    base=base,
                    showlegend=True,
                    visible=vis,
                    name='2-10 Days',
                    offsetgroup=0)

    base = base + df_total['2-10 Days']

    trace3 = go.Bar(x=df_total['Submission Date'], y=df_total['10+ Days'],
                    marker_color='#bf2a2a',
                    base=base,
                    showlegend=True,
                    visible=vis,
                    name='10+ Days',
                    offsetgroup=0)


    fig.add_trace(trace1)
    fig.add_trace(trace2)
    fig.add_trace(trace3)
    #usedf['Submission Date'] = usedf['Submission Date'].dt.date

    # trace = go.Box(y=usedf['Reporting Lag Jittered'], x=usedf['Submission Date'],
    #                #mode='markers',
    #                marker_color='blue',
    #                fillcolor='rgba(255,255,255,0)',
    #                boxpoints='all',
    #                opacity=0.66,
    #                pointpos=0,
    #                jitter=1,
    #                line_color='rgba(255,255,255,0)',
    #                marker_size=5,
    #                visible=False)

    # trace1 = go.Histogram2d(x=usedf['Submission Date'], y=usedf['Reporting Lag'],
    #                         #colorscale='YlGnBu',
    #                         zmax=20,
    #                         nbinsx=50,
    #                         #nbinsy=50,
    #                         #zauto=False,
    #                         visible=vis
    #                         )

# trace2 = go.Scatter(x=usedf['Submission Date'], y=usedf['Reporting Lag'],
#                     mode='markers',  # Set the mode the lines (rather than markers) to show a line.
#                     opacity=1,
#                     #marker_color='blue',
#                     #fill='tozeroy',  # This will fill between the line and y=0.
#                     showlegend=False,
#                     name='Lab Count',
#                     hovertemplate='Date: %{x}<br>Lag: %{y}<extra></extra>',
#                     # Note: the <extra></extra> removes the trace label.
#

fig.update_xaxes(title='Date')
fig.update_yaxes(title='Reporting Lag (days)',
                 rangemode='nonnegative')  # , fixedrange = True)
fig.update_layout(
    title_text='Plotly Lag Hist2D',
    # hovermode = 'x',
    height=900,
    width=1440,
    title_y=0.95,
    margin=dict(t=140)
)

#fig.update_xaxes(range=['2020-03-01', datetime.datetime.today()])
# Add the buttons and dropdown
# Note: I've seen odd behavior with adding the dropdown first and then the buttons. (e.g., the dropdown turns into many buttons)
fig.update_layout(
    updatemenus=[
        # Dropdown menu for choosing the country
        dict(
            buttons=dropdown_labs,
            direction='down',
            showactive=True,
            x=0.0,
            xanchor='left',
            y=1.1,
            yanchor='top'
        ),
    ]
)

fig.show()
#fig.write_html("C:/Users/alexander.onopa/workspace/lrp/plotly_reporting_lag.html")

#
# import plotly.graph_objects as go
# import glob
# from plotly.subplots import make_subplots
# import numpy as np
# from data_loader import *
# import datetime
#
#
# df_hhie = import_hhie_df()
# df_csv = import_csv_df()
#
# def convert_resultdate(datestring):
#     try:
#         out = datetime.datetime.strptime(str(datestring), '%Y%m%d')
#         return out
#     except ValueError:
#         return np.nan
#
# df_csv['labResultDate'] = df_csv['labResultDate'].apply(convert_resultdate)
#
#
# df = pd.concat([df_hhie, df_csv])
# #del df_hhie, df_csv
#
# miss = import_missingness('df')
# misformat = import_misformatting('df')
#
# # filter for recent data
# df = df.loc[df['Submission Date'] >= datetime.datetime.strptime('2023-01-01', '%Y-%m-%d')]
# miss = miss.loc[miss['Submission Date'] >= datetime.datetime.strptime('2023-01-01', '%Y-%m-%d')]
# misformat = misformat.loc[misformat['Submission Date'] >= datetime.datetime.strptime('2023-01-01', '%Y-%m-%d')]
#
# #df = df.loc[df['Lab Name'] == 'KSR']
# lab_list = pd.unique(df['Lab Name'])
#
# # calculate reporting lag
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
#
#
# dropdown_labs = []
# for i, lab_choice in enumerate(lab_list):
#     vis = [False]* len(lab_list)
#     vis[i] = True
#     dropdown_labs.append(dict(
#         args=[{'visible': vis}],
#         label=lab_choice,
#         method='restyle'
#     ))
#
# fig = go.Figure()
#
# for lab in lab_list:
#     use = lag.loc[lag['Lab Name'] == lab].copy()
#     use.drop('Lab Name', axis=1, inplace=True)
#     use['Submission Date'] = use['Submission Date'].apply(datetime.datetime.date)
#
#     use = use.groupby(['Submission Date', 'Reporting Lag'], as_index=False).count()
#     #use = use.loc[use['Reporting Lag'] > 0]
#     # trace = go.Box(y=use['Reporting Lag'], x=use['Submission Date'], #marker_size=use['Lab Result Date'],
#     #                    #mode='markers',
#     #                    #marker_color='blue', #fillcolor='lightseagreen',
#     #                fillcolor='rgba(255,255,255,0)',
#     #                boxpoints='all',
#     #                opacity=0.66,
#     #                pointpos=0,
#     #                jitter=0.5,
#     #                marker_color='blue',
#     #                line_color='rgba(255,255,255,0)',
#     #                marker_size=5,
#     #                visible=False)
#
#     # trace = go.Violin(y=use['Reporting Lag'], x=use['Submission Date'],
#     #                   box_visible=True, meanline_visible=True,
#     #                   #marker_size=use['Lab Result Date'],
#     #                   line_color='black',
#     #                   fillcolor='lightseagreen',
#     #                   opacity=0.6,
#     #                   visible=False)
#
#     trace = go.Heatmap(y=use['Reporting Lag'],  x=use['Submission Date'],
#                        z=use['Lab Result Date'],
#                        zmax=10,
#                        )
#
#     layout = go.Layout(
#         plot_bgcolor='#777777'
#     )
#     # trace = go.Histogram2d(x=use['Submission Date'], y=use['Reporting Lag'],
#     #                        colorscale='Viridis',
#     #                        #zmax=10,
#     #                        nbinsx=time_range,
#     #                        nbinsy=20,
#     #                        zauto=False
#     #                        )
#
#     fig.add_trace(trace)
#
# #fig.update_xaxes(title='Date')
# #fig.update_yaxes(title='Reporting Lag', rangemode='nonnegative')  # , fixedrange = True)
# fig.update_layout(
#     title_text='Plotly lag Test',
#     # hovermode = 'x',
#     height=1000,
#     width=850,
#     title_y=0.99,
#     margin=dict(t=140),
#     plot_bgcolor='rgba(0, 0, 0, 0)',
# )
#
# fig.update_layout(
#     updatemenus=[
#         # Dropdown menu for choosing the lab
#         dict(
#             buttons=dropdown_labs,
#             direction='down',
#             showactive=True,
#             x=0.0,
#             xanchor='left',
#             y=1.11,
#             yanchor='top'
#         )
#     ]
# )
#
# fig.show()
fig.write_html("./viz/lag_bar_plot_3cat.html")
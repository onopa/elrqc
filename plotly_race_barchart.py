import datetime
import glob

import pandas as pd
# from lrp_format_check_functions import *
import plotly
import plotly.graph_objects as go
import pymmwr
from plotly.subplots import make_subplots

# dictionary of variables and colors for plots
colors = [plotly.colors.qualitative.Plotly[0],
          plotly.colors.qualitative.Plotly[1],
          plotly.colors.qualitative.Plotly[2],
          plotly.colors.qualitative.Plotly[3],
          plotly.colors.qualitative.Plotly[4],
          plotly.colors.qualitative.Plotly[5],
          plotly.colors.qualitative.Plotly[6],
          'black',
          'lightgray'
          ]

varnames = ['American Indian or Alaska Native',
            'Asian',
            'Black or African American',
            'Native Hawaiian or Other Pacific Islander',
            'White',
            'Other',
            'Unknown or Refused to Answer',
            'Misformatted',
            'Missing'
            ]

color_dict = dict(zip(varnames, colors))

df = pd.DataFrame()
df_percentages = pd.DataFrame()

files_to_load = glob.glob('./data/processed/csv_race_counts/*.csv') + \
                glob.glob('./data/processed/hhie_race_counts/*.csv') + \
                glob.glob('./data/processed/ecr_race_counts/*.csv')

for lab_file in files_to_load:
    lab_df = pd.read_csv(lab_file)

    # convert date to MMWR week ending date for aggregating by week
    mmwrwk = []
    weekEnding = []
    for i, val in enumerate(lab_df['Submission Date']):
        dt = datetime.datetime.strptime(val, '%Y-%m-%d')
        mmwr = pymmwr.date_to_epiweek(dt.date())
        mmwrwk.append(mmwr)
        mmwr2 = pymmwr.Epiweek(mmwr.year, mmwr.week, 7)
        week_ending_date = pymmwr.epiweek_to_date(mmwr2)
        weekEnding.append(week_ending_date)
    lab_df['Week Ending'] = weekEnding
    # drop lab name before agg so script doesn't complain
    lab_name = lab_df['Lab Name'][0]
    lab_df.drop(['Submission Date', 'Lab Name'], axis=1, inplace=True)
    # aggregate by week
    lab_df = lab_df.groupby('Week Ending', as_index=False).agg(sum)

    # Create percentage DF and normalize rows by Total Count col
    lab_percentages = lab_df.copy()
    lab_percentages[varnames] = lab_percentages[varnames].div(lab_percentages['Total_Count'], axis=0)
    # add lab name column back in
    lab_df['Lab Name'] = lab_name
    lab_percentages['Lab Name'] = lab_name

    df = pd.concat([df, lab_df])
    df_percentages = pd.concat([df_percentages, lab_percentages])

# melt for plotting - maybe not necessary
melt_df = pd.melt(df, id_vars=['Week Ending', 'Lab Name'])
melt_df_percentages = pd.melt(df_percentages, id_vars=['Week Ending', 'Lab Name'])


# get lab data
def update_data(data, lab_selection):
    use = data.loc[data['Lab Name'] == lab_selection]
    return use.copy()


# get data for all other labs
def update_data_others(data, lab_selection):
    use = data.loc[data['Lab Name'] != lab_selection]
    return use.copy()


def update_misformat_table(lab_selection):
    return


availableLabs = df['Lab Name'].unique().tolist()

## Set up dropdown selector
dropdown_labs = []

numVars = len(varnames)
numLabs = len(availableLabs)
# total number of traces in the chart is (2*numVars+2)*numLabs.
# first 2 subplots each have (numvars) traces, third subplot always has just 2 traces. repeat for each lab
vis_length = (2 * numVars + 2) * numLabs
# set visibility vector for swapping between each set with dropdown
for i, lab in enumerate(availableLabs):
    vis = [False] * vis_length
    # starting at correct index, set adjacent visibilities to True to display correct batch of traces for each dropdown selection
    bounds = [(2 * numVars + 2) * i + k for k in range(2 * numVars + 2)]
    for v in bounds:
        vis[v] = True
    dropdown_labs.append(dict(
        args=[{'visible': vis}],
        label=lab,
        method='restyle'
    ))

# set up DF for plotting overall race distribution for each lab vs other labs
# agg df by lab name to get each lab's totals across time
overall_agg_df = (df.drop('Week Ending', axis=1)).groupby('Lab Name', as_index=False).agg(sum)
# calculate new totals without missing/misformatted and drop those cols
overall_agg_df['Total_Count'] = overall_agg_df['Total_Count'] - (
        overall_agg_df['Misformatted'] + overall_agg_df['Missing'])

# drop missing/misformatted? useful to see JUST valid race info
# overall_agg_df.drop(['Missing', 'Misformatted'], axis=1, inplace=True)


# Create figure
fig = make_subplots(rows=3, cols=1,
                    specs=[[{'type': 'bar'}],
                           [{'type': 'bar'}],
                           [{'type': 'bar'}]])

# add traces for each lab, for each var
for i, lab in enumerate(availableLabs):
    vis = False
    if i == 0:
        vis = True

    ## Trace: epi curve w/ races by week
    use_df = update_data(melt_df, lab)
    # trace for every variable
    for j, var in enumerate(varnames):
        x_data = use_df['Week Ending']
        y_data = use_df.loc[use_df['variable'] == var]['value']
        # start first trace with a base of 0
        if j == 0:
            base_vals = [0] * len(y_data)
        trace1 = go.Bar(x=x_data,
                        y=y_data,
                        name=var,
                        visible=vis,
                        marker_color=color_dict[var],
                        offsetgroup=0,
                        base=base_vals
                        )
        fig.add_trace(trace1, row=1, col=1)
        # update base values to stack next bar trace on top of current one
        base_vals = [a + b for a, b in zip(base_vals, y_data)]

        ## Trace: percent distribution by week
        use_percent = update_data(melt_df_percentages, lab)
        x_data_2 = use_percent['Week Ending']
        y_data_2 = use_percent.loc[use_percent['variable'] == var]['value']
        # start off with the bar bases at 0
        if j == 0:
            base_vals_2 = [0] * len(y_data_2)
        trace2 = go.Bar(x=x_data_2,
                        y=y_data_2,
                        name=var,
                        visible=vis,
                        marker_color=color_dict[var],
                        showlegend=False,
                        offsetgroup=0,
                        base=base_vals_2
                        )
        fig.add_trace(trace2, row=2, col=1)

        # update new bases for bar plot to stack the next trace on top of current one
        base_vals_2 = [c + d for c, d in zip(base_vals_2, y_data_2)]

    ## Trace: overall race % vs all other labs
    # get overall race % distribution for current lab
    overall_lab_percent = update_data(overall_agg_df, lab)
    overall_lab_percent.drop('Lab Name', axis=1, inplace=True)
    overall_lab_percent = overall_lab_percent.div(max(overall_lab_percent['Total_Count']))
    overall_lab_percent['Lab Name'] = lab

    # get overall percent distribution across all other labs
    overall_other_df = update_data_others(overall_agg_df, lab)
    overall_other_df.drop('Lab Name', axis=1, inplace=True)
    # agg over all labs - this will produce a series. change to DF and combine with individual lab
    overall_other_agg = overall_other_df.agg(sum).to_frame().transpose()
    overall_other_percent = overall_other_agg.div(overall_other_agg['Total_Count'][0])
    overall_other_percent['Lab Name'] = 'All Other Labs'
    overall_percent_comparison = pd.concat([overall_lab_percent, overall_other_percent])

    # this is what we'll use for plotting
    overall_percent_melt = pd.melt(overall_percent_comparison, id_vars='Lab Name')

    colors = ['red', 'blue']
    names = ['Overall Percentage: ' + lab, 'Overall Percentage: Statewide']
    for j, group in enumerate([lab, 'All Other Labs']):
        comparison_barchart_df = overall_percent_melt.loc[overall_percent_melt['Lab Name'] == group]
        xdata = comparison_barchart_df['variable']
        ydata = comparison_barchart_df['value']
        trace3 = go.Bar(x=xdata,
                        y=ydata,
                        visible=vis,
                        offsetgroup=j,
                        name=names[j],
                        marker_color=colors[j]
                        )
        fig.add_trace(trace3, row=3, col=1)

fig.update_xaxes(title='Date')
fig.update_layout(
    title_text='ELR Race Data by Lab',
    # hovermode = 'x',
    height=1200,
    width=1200,
    title_y=1.00,
    margin=dict(t=140),
    # barmode='stack'
)

fig.update_layout(
    updatemenus=[
        # Dropdown menu for choosing the lab
        dict(
            buttons=dropdown_labs,
            direction='down',
            showactive=True,
            x=0.0,
            xanchor='left',
            y=1.08,
            yanchor='top'
        )
    ]
)

# fig.update_layout(legend_orientation="h",
#             xaxis1_rangeslider_visible=True, xaxis1_rangeslider_thickness=0.1 )
fig.show()
fig.write_html('./viz/plotly_race_barchart.html')

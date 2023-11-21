from datetime import datetime
import pandas as pd
from plotly import graph_objects as go, colors
from plotly.subplots import make_subplots
import pymmwr

import config
from data_loader import LabData, import_hl7_data
from config import time_var, grouping_var
from compile_race_counts_v2 import compile_hl7_race_parallel


class LabRaceData:
    def __init__(self, labname, race_df, percentage_df, lab_totals):
        self.lab_name = labname
        self.race_df = race_df
        self.percentage_df = percentage_df
        self.lab_totals = lab_totals

# dictionary of variables and colors for plots
colors = [colors.qualitative.Plotly[0],
          colors.qualitative.Plotly[1],
          colors.qualitative.Plotly[2],
          colors.qualitative.Plotly[3],
          colors.qualitative.Plotly[4],
          colors.qualitative.Plotly[5],
          colors.qualitative.Plotly[6],
          'black',
          'gray',
          'blue'
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


def prep_weekly_data(lab_object):
    lab_name = lab_object.lab_name
    lab_race_df = lab_object.race
    lab_race_df['Total Count'] = 1
    lab_race_df = lab_race_df.groupby(time_var, as_index=False).sum()
    mmwrwk = []
    week_ending = []
    for val in lab_race_df[time_var]:
        # dt = datetime.strptime(val, '%Y-%m-%d')
        # convert date to mmwr week
        mmwr = pymmwr.date_to_epiweek(val.date())
        mmwrwk.append(mmwr)
        mmwr2 = pymmwr.Epiweek(mmwr.year, mmwr.week, 7)
        week_ending_date = pymmwr.epiweek_to_date(mmwr2)
        week_ending.append(week_ending_date)
    lab_race_df['Week Ending'] = week_ending
    lab_race_df.drop(time_var, axis=1, inplace=True)

    # aggregate by week
    lab_race_df = lab_race_df.groupby('Week Ending', as_index=False).sum()

    # create percentage df
    lab_percentages = lab_race_df.copy()
    lab_percentages[varnames] = lab_percentages[varnames].div(lab_percentages['Total Count'], axis=0)

    lab_totals = lab_race_df.drop('Week Ending', axis=1).sum().reset_index()
    lab_totals.columns = ['race', 'total']
    output_race_data = LabRaceData(lab_name, lab_race_df, lab_percentages, lab_totals)
    return output_race_data


def make_lab_dropdown(dropdown_race_data_list):
    dropdown = []
    num_vars = len(varnames)
    num_labs = len(dropdown_race_data_list)

    # total number of traces in the chart is (2*numVars+2)*numLabs.
    # first 2 subplots each have (num_vars) traces, third subplot always has just 1 trace. repeat for each lab
    vis_length = (2 * num_vars + 1) * num_labs
    for i, lab_item in enumerate(dropdown_race_data_list):
        vis = [False] * vis_length
        # starting at correct index, set adjacent visibilities to True to display correct batch of traces for each dropdown selection
        bounds = [(2 * num_vars + 1) * i + k for k in range(2 * num_vars + 1)]
        for v in bounds:
            vis[v] = True
        dropdown.append(dict(
            args=[{'visible': vis}],
            label=lab_item.lab_name,
            method='restyle'
        ))
    return dropdown


def make_figure(plot_input_list):
    fig = make_subplots(rows=3, cols=1,
                        specs=[[{'type': 'bar'}],
                               [{'type': 'bar'}],
                               [{'type': 'bar'}]],
                        subplot_titles=['Race Count by Week',
                                        'Race Percentage by Week',
                                        'Total Count'])
    for i, data in enumerate(plot_input_list):
        vis = False
        if i == 0:
            vis = True

        use_df = pd.melt(data.race_df, id_vars=['Week Ending'])
        use_percent = pd.melt(data.percentage_df, id_vars=['Week Ending'])

        for j, var in enumerate(varnames):
            # Trace 1: stacked barplot
            x_data = use_df['Week Ending']
            y_data = use_df.loc[use_df['variable'] == var]['value']
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

            ## Trace 2: percent distribution by week
            x_data_2 = use_percent['Week Ending']
            y_data_2 = use_percent.loc[use_percent['variable'] == var]['value']

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

        # trace 3: overall distribution
        use_total_df = data.lab_totals
        x_data_3 = use_total_df['race']
        y_data_3 = use_total_df['total']
        trace3 = go.Bar(x=x_data_3,
                        y=y_data_3,
                        visible=vis,
                        #offsetgroup=j,
                        name='Total Counts in Date Range',
                        marker_color=colors,
                        showlegend=False,
                        )
        fig.add_trace(trace3, row=3, col=1)

    return fig

def update_fig(figure, dropdown):
    figure.update_xaxes(title='Date')
    figure.update_layout(
        title_text='ELR Race Data by Lab',
        # hovermode = 'x',
        height=1200,
        width=1000,
        title_y=1.00,
        margin=dict(t=140),
        # barmode='stack'
    )

    figure.update_layout(
        updatemenus=[
            # Dropdown menu for choosing the lab
            dict(
                buttons=dropdown,
                direction='down',
                showactive=True,
                x=0.0,
                xanchor='left',
                y=1.08,
                yanchor='top'
            )
        ]
    )
    figure['layout']['xaxis3']['title'] = 'Patient Race'
    return figure


# to be called by run_all script with external data:
def generate_race_plots(lab_data_list_input):
    weekly_race_data_list = []
    lab_data_list_input.sort()
    for lab_item in lab_data_list_input:
        lab_race_data = prep_weekly_data(lab_item)
        weekly_race_data_list.append(lab_race_data)

    dropdown_labs = make_lab_dropdown(weekly_race_data_list)
    plotted_fig = make_figure(weekly_race_data_list)
    finished_fig = update_fig(plotted_fig, dropdown_labs)
    finished_fig.to_html(f'{config.output_folder}/plotly_race_viz.html')
    return finished_fig

# to run from beginning, just for race viz
if __name__ == '__main__':
    lab_data_list = import_hl7_data()
    lab_data_list = compile_hl7_race_parallel(lab_data_list)
    race_data_list = []
    for lab in lab_data_list:
        lab_race_data = prep_weekly_data(lab)
        race_data_list.append(lab_race_data)

    dropdown_labs = make_lab_dropdown(race_data_list)
    plotted_fig = make_figure(race_data_list)
    output_fig = update_fig(plotted_fig, dropdown_labs)

from datetime import datetime
import numpy as np
import re
from numpy import nan
import pandas as pd
import plotly.graph_objects as go
import config
from data_loader import LabData, import_hl7_data


def fix_timestamp_with_decimals(date_string):
    if len(date_string) > 19:
        if any((c == '.') for c in date_string):
            return re.sub('\..*?-', '-', date_string)
        else:
            return date_string[0:19]
    return date_string

def convert_dates(lab_object):
    lab_object.df['labResultDate'] = pd.to_datetime(lab_object.df['labResultDate'], format='mixed')
    lab_object.df['specimenCollectionDate'] = pd.to_datetime(lab_object.df['specimenCollectionDate'], format='mixed')
    lab_object.df['specimenReceivedDate'] = pd.to_datetime(lab_object.df['specimenReceivedDate'], format='mixed')
    try:
        lab_object.df['submission_datetime'] = pd.to_datetime(lab_object.df['submission_datetime'], format='mixed')
    except pd._libs.tslibs.parsing.DateParseError:
        lab_object.df['submission_datetime'] = lab_object.df['submission_datetime'].map(fix_timestamp_with_decimals)
        lab_object.df['submission_datetime'] = pd.to_datetime(lab_object.df['submission_datetime'], format='mixed')
    return lab_object


def prep_lab_lags(lab_data):
    print(lab_data.lab_name)
    lab_data = convert_dates(lab_data)
    df = lab_data.df

    lags_df = pd.DataFrame()


    # compute lags in days - if I want to make it in hours I need to use [time deltas] / np.timedelta64(1, 'h')
    lags_df['specimenCollectionDate'] = df['specimenCollectionDate']
    lags_df['lag_collection_to_received'] = (df['specimenReceivedDate'] - df['specimenCollectionDate']).dt.days
    try:
        lags_df['lag_received_to_result'] = (df['labResultDate'] - df['specimenReceivedDate']).dt.days
    except TypeError:
        lags_df['lag_received_to_result'] = np.nan
    try:
        lags_df['lag_result_to_submission'] = (df['submission_datetime'] - df['labResultDate']).dt.days
    except TypeError:
        lags_df['lag_result_to_submission'] = np.nan

    lab_data.lags = lags_df
    return lab_data


def make_sankey_trace(lab_data):
    # sankey direction parameters
    source = [0, 0, 0, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6, 7, 7, 7]
    target = [2, 3, 4, 4, 5, 6, 7, 5, 6, 7, 5, 6, 7, 8, 9, 10, 8, 9, 10, 8, 9, 10]
    x_vals = [0.01, 0.01, 0.25, 0.25, 0.25, 0.5, 0.5, 0.5, 0.75, 0.75, 0.75]
    y_vals = [0.01, 0.5, 0.01, 0.5, 0.9, 0.01, 0.5, 0.9, 0.01, 0.5, 0.9]
    lag_counts = []
    lab_data = prep_lab_lags(lab_data)
    lag_df = lab_data.lags

    # this is annoying: calculate values for each flows
    # col 1 ok to col 2 ok (0 -> 2)
    count = sum(lag_df['lag_collection_to_received'] < 1)
    lag_counts.append(count)
    # col 1 ok to col 2 late (0 -> 3)
    count = sum(lag_df['lag_collection_to_received'] >= 1)
    lag_counts.append(count)
    # col 1 ok to col 2 missing (0 -> 4)
    count = sum((pd.isna(lag_df['lag_collection_to_received'])) & pd.notna(lag_df['specimenCollectionDate']))
    lag_counts.append(count)
    # col 1 missing to col 2 missing (1 -> 4
    count = sum((pd.isna(lag_df['lag_collection_to_received'])) & pd.isna(lag_df['specimenCollectionDate']))
    lag_counts.append(count)
    # col 2 ok to col 3 ok (2 -> 5)
    count = sum((lag_df['lag_collection_to_received'] < 1) & (lag_df['lag_received_to_result'] < 1))
    lag_counts.append(count)
    # col 2 ok to col 3 late
    count = sum((lag_df['lag_collection_to_received'] < 1) & (lag_df['lag_received_to_result'] >= 1))
    lag_counts.append(count)
    # col 2 ok to col 3 missing
    count = sum((lag_df['lag_collection_to_received'] < 1) & pd.isna(lag_df['lag_received_to_result']))
    lag_counts.append(count)
    # col 2 late to col 3 ok
    count = sum((lag_df['lag_collection_to_received'] >= 1) & (lag_df['lag_received_to_result'] < 1))
    lag_counts.append(count)
    # col 2 late to col 3 late
    count = sum((lag_df['lag_collection_to_received'] >= 1) & (lag_df['lag_received_to_result'] >= 1))
    lag_counts.append(count)
    # col 2 late to col 3 missing
    count = sum((lag_df['lag_collection_to_received'] >= 1) & pd.isna(lag_df['lag_received_to_result']))
    lag_counts.append(count)
    # col 2 missing to col 3 ok
    count = sum(pd.isna(lag_df['lag_collection_to_received']) & (lag_df['lag_received_to_result'] < 1))
    lag_counts.append(count)
    # col 2 missing to col 3 late
    count = sum(pd.isna(lag_df['lag_collection_to_received']) & (lag_df['lag_received_to_result'] >= 1))
    lag_counts.append(count)
    # col 2 missing to col 3 missing
    count = sum(pd.isna(lag_df['lag_collection_to_received']) & pd.isna(lag_df['lag_received_to_result']))
    lag_counts.append(count)
    # col 3 ok to col 4 ok
    count = sum((lag_df['lag_received_to_result'] < 1) & (lag_df['lag_result_to_submission'] < 1))
    lag_counts.append(count)
    # col 3 ok to col 4 late
    count = sum((lag_df['lag_received_to_result'] < 1) & (lag_df['lag_result_to_submission'] >= 1))
    lag_counts.append(count)
    # col 3 ok to col 4 missing
    count = sum((lag_df['lag_received_to_result'] < 1) & pd.isna(lag_df['lag_result_to_submission']))
    lag_counts.append(count)
    # col 3 late to col 4 ok
    count = sum((lag_df['lag_received_to_result'] >= 1) & (lag_df['lag_result_to_submission'] < 1))
    lag_counts.append(count)
    # col 3 late to col 4 late
    count = sum((lag_df['lag_received_to_result'] >= 1) & (lag_df['lag_result_to_submission'] >= 1))
    lag_counts.append(count)
    # col 3 late to col 4 missing
    count = sum((lag_df['lag_received_to_result'] >= 1) & pd.isna(lag_df['lag_result_to_submission']))
    lag_counts.append(count)
    # col 3 missing to col 4 ok
    count = sum(pd.isna(lag_df['lag_received_to_result']) & (lag_df['lag_result_to_submission'] < 1))
    lag_counts.append(count)
    # col 3 missing to col 4 late
    count = sum(pd.isna(lag_df['lag_received_to_result']) & (lag_df['lag_result_to_submission'] >= 1))
    lag_counts.append(count)
    # col 3 missing to col 4 missing
    count = sum(pd.isna(lag_df['lag_received_to_result']) & pd.isna(lag_df['lag_result_to_submission']))
    lag_counts.append(count)

    labels = ["Has Specimen Date", "Missing Specimen Date", "Specimen Lag < 1 Day", "Specimen Lag > 1 day",
                   "Missing Specimen Lag", "Result Lag < 1 Day", "Result Lag > 1 Day",
                   "Missing Result Lag", "Reporting Lag < 1 Day", "Reporting Lag > 1 Day", "Missing Reporting Lag"]

    colors = ['blue', 'black'] + ['blue', 'red', 'black']*3

    # need to build up second version of node and flow properties to discard empty nodes and flows
    source2 = []
    target2 = []
    lag_counts2 = []
    labels2 = []
    colors2 = []
    x_vals2 = []
    y_vals2 = []

    # this is horrible lol. surely there is a better way
    # iterate thru nodes
    for i, label in enumerate(labels):
        # get indices of sources and targets that correspond to node
        source_idx = [j for j in range(len(source)) if source[j] == i]
        target_idx = [j for j in range(len(target)) if target[j] == i]
        idxs_for_node = list(set(source_idx) | set(target_idx))
        # check all inputs and outputs of node to see if any are nonzero
        node_total_count = sum([lag_counts[v] for v in idxs_for_node])
        # any node with nonzero total count will be included
        if node_total_count != 0:
            labels2.append(label)
            colors2.append(colors[i])
            x_vals2.append(x_vals[i])
            y_vals2.append(y_vals[i])
            # the flows (sources, targets, and values) need to be narrowed down more,
            # since a node can have multiple inputs and outputs and we only want the nonzero ones.
            # just go use sources. if you also use targets, you will double count some flows.
            for idx in source_idx:
                if lag_counts[idx] != 0:
                    # add nonzero to new lists
                    source2.append(source[idx])
                    target2.append(target[idx])
                    lag_counts2.append(lag_counts[idx])

    # now that new lists are built up, need to re-number the nodes from 0 to n (no skipping numbers).
    # this is to prevent labels and colors from being mismatched
    node_idxs = list(set(source2) | set(target2))
    node_replace_dict = dict(zip(node_idxs, range(len(node_idxs))))
    for i, val in enumerate(source2):
        source2[i] = node_replace_dict[source2[i]]
        target2[i] = node_replace_dict[target2[i]]

    trace = go.Sankey(
        visible=False,
        arrangement='snap',
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color = "black", width = 0.5),
            label=labels2,
            color=colors2,
            x=x_vals2,
            #y=y_vals2
        ),
        link=dict(
            source=source2,  # indices correspond to labels, eg A1, A2, A1, B1, ...
            target=target2,
            value=lag_counts2))

    return trace


def make_lab_dropdown(lab_data_list):
    dropdown = []
    num_labs = len(lab_data_list)

    # total number of traces in the chart is (2*numVars+2)*numLabs.
    # first 2 subplots each have (num_vars) traces, third subplot always has just 1 trace. repeat for each lab
    for i, lab_item in enumerate(lab_data_list):
        total_count = lab_item.df.shape[0]
        vis = [False] * num_labs
        vis[i] = True
        dropdown.append(dict(
            args=
            [
                {'visible': vis},
                dict(
                    annotations=[dict(
                        text=str(f'Total Records: {total_count}'),
                        font=dict(
                            family="Helvetica, bold",
                            size=16,  # Set the font size here
                            color="RebeccaPurple"),
                        showarrow=False,
                        xref='paper', x=0.85,
                        yref='paper', y=1.06)]
                )
            ],
            label=lab_item.lab_name,
            method='update'
        ))
    return dropdown


def make_sankey_figure(lab_data_list):
    fig = go.Figure()

    for lab in lab_data_list:
        trace = make_sankey_trace(lab)
        fig.add_trace(trace)

    dropdown = make_lab_dropdown(lab_data_list)
    fig.update_layout(
        updatemenus=[
            # Dropdown menu for choosing the lab
            dict(
                buttons=dropdown,
                direction='down',
                showactive=True,
                x=0.0,
                xanchor='left',
                y=1.10,
                yanchor='top'
            )
        ]
    )

    fig.update_layout(title_text="Lab Timing Lags", font_size=10, width=1200, height=600)
    return fig

def make_lag_plots(lab_data):
    figure = make_sankey_figure(lab_data)
    figure.to_html(f'{config.output_folder}/lag_sankey.html')

if __name__ == '__main__':

    lab_data_list = import_hl7_data()
    figure = make_sankey_figure(lab_data_list)
    figure.to_html(f'{config.output_folder}/lag_sankey.html')


    # def nodify(node_names):
    #     node_names = unique_list
    #     # uniqe name endings
    #     ends = sorted(list(set([e[-1] for e in node_names])))
    #
    #     # intervals
    #     steps = 1 / len(ends)
    #
    #     # x-values for each unique name ending
    #     # for input as node position
    #     nodes_x = {}
    #     xVal = 0
    #     for e in ends:
    #         nodes_x[str(e)] = xVal
    #         xVal += steps
    #
    #     # x and y values in list form
    #     x_values = [nodes_x[n[-1]] for n in node_names]
    #     y_values = [0.1] * len(x_values)
    #
    #     return x_values, y_values
    #
    #
    # nodified = nodify(node_names=unique_list)
    #

import plotly.graph_objects as go
from plotly.subplots import make_subplots
from lrp_format_check_functions import get_check_function_dict_hl7  # for variable names
import pandas as pd
import config


def generate_qc_plots(lab_data_list):
    lab_data_list.sort()
    time_var = config.time_var
    grouping_var = config.grouping_var

    # TODO: add data table functionality
    #misformatted_df = pd.DataFrame(columns=[grouping_var, 'Variable', 'Value', 'count'])

    def update_time_plot_x(aggregated_df):  # not currently used since I can just grab the time column directly
        return aggregated_df[time_var]

    def update_time_plot_y(aggregated_df, varname):
        try:
            result = aggregated_df[varname]
        except KeyError:
            result = pd.Series(name=varname)
        return result

    def update_cells(celldf, var):
        use = celldf.loc[(celldf['Variable Name'] == var)]
        return use

    # TODO: add csv data as well?
    # hl7_labs = [lab for lab in lab_data_list if lab.get_source_type() == 'hl7']

    columns = list(get_check_function_dict_hl7().keys())
    columns = [col for col in columns if col not in [grouping_var, time_var]]

    # LAB/GROUP SELECTION DROPDOWN
    # create list with intial blank entry
    dropdown_labs = [dict(
        args=[{'x': [],
               'y': [],
               'cells': [],
               'header': dict(values=['Variable Name', 'Misformatted Value', 'Count'])
               # 'header': dict(values=['Submission Date', 'Variable', 'Misformatted Value'])
               }],
        label='Select Lab',
        method='update'
    )]
    # set initial blank value for lab selector

    # fill out all data for plot traces - every variable for every lab
    for lab_data in lab_data_list:
        lab_name = lab_data.lab_name
        lab_misformat_df = lab_data.misformatting
        lab_misformat_df['Total Submission Count'] = 1
        lab_misformat_df = lab_misformat_df.groupby(time_var, as_index=False).sum()

        lab_missing_df: pd.DataFrame = lab_data.missingness
        lab_missing_df['Total Submission Count'] = 1
        lab_missing_df = lab_missing_df.groupby([grouping_var, time_var], as_index=False).sum()

        cell_df = lab_data.misformat_values

        # x variable will just be the time variable
        x_val = lab_misformat_df[time_var]
        lab_data_length = len(x_val)
        xargs = []
        yargs = []
        cells = []
        base = []
        #header = []
        for i, c in enumerate(columns):
            # 3 x values correspond to valid, misformat, and missing traces. blank x val corresponds to data table cells
            xargs.append(x_val)
            xargs.append(x_val)
            xargs.append(x_val)
            xargs.append([])

            # get data for y values
            var_misformat = update_time_plot_y(lab_misformat_df, c).reset_index(drop=True)
            var_missing = update_time_plot_y(lab_missing_df, c).reset_index(drop=True)
            normal_submissions = lab_misformat_df['Total Submission Count'] - (var_misformat + var_missing)

            # append y data (valid submissions, misformatted, missing) and bases for stacked bar chart
            yargs.append(normal_submissions)
            base_0 = [0]*lab_data_length
            base.append(base_0)

            yargs.append(var_misformat)
            base_1 = normal_submissions
            base.append(base_1)

            yargs.append(var_missing)
            base_2 = normal_submissions+var_misformat
            base.append(base_2)

            # 4th empty trace for data table
            yargs.append([])
            base.append([])

            df_for_cells = update_cells(cell_df, c)
            celldata = []
            for col in df_for_cells.columns:
                celldata.append(df_for_cells[col].tolist())

            # add 3 blank cell data to make sure table data lines up after 3 barchart traces (valid, misformat, missing)
            cells.append(dict())
            cells.append(dict())
            cells.append(dict())
            # add real cell data
            cells.append(dict(values=celldata))

        # add all data to dropdown list
        dropdown_labs.append(dict(
            args=[{'x': xargs,
                   'y': yargs,
                   'cells': cells,
                   'base': base,
                   'header': dict(values=['Variable Name', 'Misformatted Value', 'Count'])
                   # 'header': dict(values=['Submission Date', 'Variable', 'Misformatted Value'])
                   }],
            label=lab_name,
            method='update'
        ))

    ## VARIABLE SELECTION DROPDOWN
    # create variable selection dropdown list with blank first option
    dropdown_vars = [dict(
        args=[{'visible': [False] * (4 * len(columns))}],
        label='Select Variable',
        method='restyle'
    )]

    for i, c in enumerate(columns):
        vis = [False]*(4*len(columns))

        bounds=[4*i, 4*i+1, 4*i+2, 4*i+3]
        for v in bounds:
            vis[v] = True
        dropdown_vars.append(dict(
            args=[{'visible': vis
                   }],
            label=c,
            method='restyle'
        ))

    # start with one lab (I will use the first one in the list, since that is how the dropdowns are initialized)
    usedf = pd.DataFrame(columns=['Submission Date'])
    # Create the figure.
    # fig = go.Figure()
    fig = make_subplots(rows=2, cols=1,
                        specs=[[{'type': 'scatter'}],
                               [{'type': 'table'}]])

    print('generating traces...')
    # Add traces for each column
    for i, c in enumerate(columns):
        vis = False
        trace_normal = go.Bar(x=usedf['Submission Date'], y=[],
                              offsetgroup=0,
                              base=0,
                              marker_color='cornflowerblue',
                              showlegend=True,
                              visible=vis,
                              name='valid submissions'
                              )

        trace_misformat = go.Bar(x=usedf['Submission Date'], y=[],
                                 offsetgroup=0,
                                 base=0,
                                 marker_color='red',
                                 showlegend=True,
                                 visible=vis,
                                 name='misformatted submissions')

        trace_missing = go.Bar(x=usedf['Submission Date'], y=[],
                               offsetgroup=0,
                               base=0,
                               marker_color='black',
                               showlegend=True,
                               visible=vis,
                               name='missing submissions')

        # Add that trace to the figure
        fig.add_trace(trace_normal, row=1, col=1)
        fig.add_trace(trace_misformat, row=1, col=1)
        fig.add_trace(trace_missing, row=1, col=1)

        # Create and add second trace for data table
        # trace2 = go.Table(header=dict(values=['Submission Date', 'Variable', 'Misformatted Value']),
        trace2 = go.Table(header=dict(values=['Variable Name', 'Misformatted Value', 'Count']),
                          cells=dict(),
                          columnwidth=[350, 650, 100],
                          visible=False
                          )

        fig.add_trace(trace2, row=2, col=1)
    # Add the trace and update a few parameters for the axes.
    # fig.update_xaxes(dict(range=['2023-01-01', '2023-08-08']), title='Date', autorange=False)
    fig.update_yaxes(title='number of results submitted', rangemode='nonnegative')  # , fixedrange = True)
    fig.update_layout(
        title_text='Plotly Missing+Misformatting Test',
        # hovermode = 'x',
        height=1200,
        width=1000,
        title_y=0.99,
        margin=dict(t=140),

    )

    # Add the dropdowns
    # Note: I've seen odd behavior with adding the dropdown first and then the buttons
    fig.update_layout(
        updatemenus=[
            # Dropdown menu for choosing the lab
            dict(
                buttons=dropdown_labs,
                direction='down',
                showactive=True,
                x=0.0,
                xanchor='left',
                y=1.10,
                yanchor='top',
                active=0
            ),
            # and for the variables
            dict(
                buttons=dropdown_vars,
                direction='down',
                showactive=True,
                x=0.0,
                xanchor='left',
                y=1.05,
                yanchor='top',
                active=0
            )
        ]
    )

    fig.write_html(f"{config.output_folder}/misformatting_and_missing.html")
    return fig

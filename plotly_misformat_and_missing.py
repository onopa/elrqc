import numpy as np
import plotly.graph_objects as go
import glob
import plotly
from plotly.subplots import make_subplots
import lrp_format_check_functions
import pandas as pd
from data_loader import *


# Import data:
df_misf = import_misformatting('df')
df_misf['Submission Date'] = pd.to_datetime(df_misf['Submission Date'])

df_miss = import_missingness('df')
df_miss['Submission Date'] = pd.to_datetime(df_miss['Submission Date'])


misformatted_df = import_misformat_values('df')


#df = df.loc[df['Submission Date'] > '2023-01-01']
#misformatted_df = misformatted_df.loc[misformatted_df['Submission Date'] > '2023-01-01']

# Create functions to update the values

# For the time series plot
# Since there are actually N traces in the time plot (only 1 visible), I will need to send N data sets back from each function
def updateTimePlotX(lab):
    use = df_misf.loc[df_misf['Lab Name'] == lab]
    return use['Submission Date']


def updateTimePlotY(labdf, var):
    return labdf[var]


def updateCells(celldf, var):
    use = celldf.loc[(celldf['variable'] == var)]
    return use.drop('Lab Name', axis=1)

# I am going to create the dropdown list here and then add it to the figure below
# I will need to update the x and y data for the time series plot
# Even though some data will not change, I will need to specify everything in this dropdown menu

# Identify the labs to use
availableLabs = df_misf['Lab Name'].unique().tolist()

columns = list(df_misf.columns)
# drop 2 unnecessary columns
columns.pop(columns.index('Submission Date'))
columns.pop(columns.index('Lab Name'))
columns.pop(columns.index('Total Submission Count'))

# create list for the country dropdown
dropdown_labs = []

# set initial blank value for lab selector
dropdown_labs.append(dict(
        args=[{'x': [],
               'y': [],
               'cells': [],
               'header': dict(values=['Submission Date', 'Variable', 'Misformatted Value'])
               }],
        label='Select Lab',
        method='update'
    ))

# Create dropdowns for labs
for lab_choice in availableLabs:
    print(lab_choice)
    lab_misformat_df = df_misf.loc[df_misf['Lab Name'] == lab_choice]
    lab_missing_df = df_miss.loc[df_miss['Lab Name'] == lab_choice]

    total_submissions = lab_misformat_df['Total Submission Count'].reset_index(drop=True)

    cell_df = misformatted_df.loc[(misformatted_df['Lab Name'] == lab_choice)]
    x_val = lab_misformat_df['Submission Date']
    lab_data_length = len(x_val)
    xargs = []
    yargs = []
    cells = []
    base = []
    #header = []
    for i, c in enumerate(columns):
        xargs.append(x_val)
        xargs.append(x_val)
        xargs.append(x_val)
        xargs.append([])

        var_misformat = updateTimePlotY(lab_misformat_df, c).reset_index(drop=True)
        var_missing = updateTimePlotY(lab_missing_df, c).reset_index(drop=True)

        normal_submissions = total_submissions - (var_misformat + var_missing)

        yargs.append(normal_submissions)

        base_0 = [0]*lab_data_length
        base.append(base_0)

        yargs.append(var_misformat)

        base_1 = normal_submissions
        base.append(base_1)

        yargs.append(var_missing)

        base_2 = normal_submissions+var_misformat
        base.append(base_2)

        yargs.append([])
        base.append([])

        df_for_cells = updateCells(cell_df, c)
        celldata = []
        for col in df_for_cells.columns:
            celldata.append(df_for_cells[col].tolist())
        cells.append(dict())
        cells.append(dict())
        cells.append(dict())
        cells.append(dict(values=celldata))

    dropdown_labs.append(dict(
        args=[{'x': xargs,
               'y': yargs,
               'cells': cells,
               'base': base,
               'header': dict(values=['Submission Date', 'Variable', 'Misformatted Value'])
               }],
        label=lab_choice,
        method='update'
    ))

# Create dropdown for variable selection
dropdown_vars = []

# Add initial blank selection
dropdown_vars.append(dict(
    args=[{'visible': [False]*(4*len(columns))}],
    label='Select Variable',
    method='restyle'
))


for i, c in enumerate(columns):
    print(i)
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
lab = availableLabs[0]
usedf = df_misf.loc[df_misf['Lab Name'] == lab]

# Create the figure.
#fig = go.Figure()
fig = make_subplots(rows=2, cols=1,
                    specs=[[{'type': 'scatter'}],
                           [{'type': 'table'}]])

# Add traces for each column
for i, c in enumerate(columns):
    vis = False

    trace_normal = go.Bar(x=usedf['Submission Date'], y=[],
                   offsetgroup=0,
                   base=0,
                   marker_color='blue',
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
    trace2 = go.Table(header=dict(values=['Submission Date', 'Variable', 'Misformatted Value']),
                      cells=dict(),
                      visible=False
                      )

    fig.add_trace(trace2, row=2, col=1)
# Add the trace and update a few parameters for the axes.
#fig.update_xaxes(dict(range=['2023-01-01', '2023-08-08']), title='Date', autorange=False)
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
# Note: I've seen odd behavior with adding the dropdown first and then the buttons. (e.g., the dropdown turns into many buttons)
fig.update_layout(
    updatemenus=[
        # Dropdown menu for choosing the lab
        dict(
            buttons=dropdown_labs,
            direction='down',
            showactive=True,
            x=0.0,
            xanchor='left',
            y=1.11,
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
            y=1.06,
            yanchor='top',
            active=0
        )
    ]
)


fig.show()


fig.write_html("./viz/plotly_misformatting_and_missing.html")

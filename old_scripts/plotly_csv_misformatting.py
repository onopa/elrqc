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
df_miss = import_missingness('df')

# df = pd.DataFrame()
# dflist = []
# for labfile in glob.glob('../processed_csvs/csv_misformatting/*.csv'):
#     lfdf = pd.read_csv(labfile)
#     dflist.append(lfdf)
#
# for labfile in glob.glob('../processed_csvs/hhie_misformatting/*.csv'):
#     lfdf_hhie = pd.read_csv(labfile)
#     #lfdf_hhie.drop('Lab Name', axis=1, inplace=True)
#     dflist.append(lfdf_hhie)
#
# df = pd.concat(dflist)

# cols_to_drop = [k for k in df_csv.columns if k not in df_hhie.columns]
# print(cols_to_drop)
# df_csv.drop(cols_to_drop, axis=1)
#df = df.fillna(0)

df_misf['Submission Date'] = pd.to_datetime(df_misf['Submission Date'])
df_miss['Submission Date'] = pd.to_datetime(df_miss['Submission Date'])

# misformatted_list = []
# for misformat_file in glob.glob('../processed_csvs/csv_misformat_values/*.csv'):
#     mfdf_csv = pd.read_csv(misformat_file, dtype='str')
#     misformatted_list.append(mfdf_csv)
#
# for misformat_file in glob.glob('../processed_csvs/hhie_misformat_values/*.csv'):
#     mfdf_hhie = pd.read_csv(misformat_file)
#     misformatted_list.append(mfdf_hhie)
#misformatted_df = pd.concat(misformatted_list)

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

# create list for the country dropdown
dropdown_labs = []

# set initial blank value for lab selector
dropdown_labs.append(dict(
        args=[{'x': [],
               'y': [],
               'cells': [],
               'header': ['Submission Date', 'variable', 'value']
               }],
        label='Select Lab',
        method='update'
    ))

# Create dropdowns for labs
for lab_choice in availableLabs:
    print(lab_choice)
    lab_df = df_misf.loc[df_misf['Lab Name'] == lab_choice]
    cell_df = misformatted_df.loc[(misformatted_df['Lab Name'] == lab_choice)]
    x_val = lab_df['Submission Date']
    xargs = []
    yargs = []
    cells = []
    base = []
    #header = []
    for i, c in enumerate(columns):
        #print(c)
        xargs.append(x_val)
        xargs.append([])

        yargs.append(updateTimePlotY(lab_df, c))
        yargs.append([])

        df_for_cells = updateCells(cell_df, c)
        celldata = []
        for col in df_for_cells.columns:
            celldata.append(df_for_cells[col].tolist())
        cells.append(dict())
        cells.append(dict(values=celldata))

    dropdown_labs.append(dict(
        args=[{'x': xargs,
               'y': yargs,
               'cells': cells,
               'header': ['Submission Date', 'variable', 'value'],
               }],
        label=lab_choice,
        method='update'
    ))

# Create dropdown for variable selection
dropdown_vars = []

# Add initial blank selection
dropdown_vars.append(dict(
    args=[{'visible': False}],
    label='Select Variable',
    method='restyle'
))


for i, c in enumerate(columns):
    print(i)
    Fs = [False]*(2*len(columns)-1)
    vis = [True]+Fs
    #vis = [False] * len(columns)
    #bounds = [i]
    bounds=[2*i, 2*i+1]
    for v in bounds:
        vis[v] = True
    dropdown_vars.append(dict(
        args=[{'visible': vis,
               'marker.color': ['blue'] + ['red']*(len(columns))
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
    visible = False
#    if (i == 0):
#        visible = True
    #tracename = c
    tracename = "selected variable"
    markercolor = 'red'
    if i == 0:
        markercolor = 'blue'
        tracename = "total submissions"


    # Create the scatter trace
    # trace = go.Scatter(x=usedf['Submission Date'], y=usedf['dummy'],
    #                    mode='markers',  # Set the mode the lines (rather than markers) to show a line.
    #                    opacity=1,
    #                    marker_color=markercolor,
    #                    #fill='tozeroy',  # This will fill between the line and y=0.
    #                    showlegend=True,
    #                    name=c,
    #                    hovertemplate='Date: %{x}<br>Number: %{y}<extra></extra>',
    #                    # Note: the <extra></extra> removes the trace label.
    #                    visible=visible,
    #                    )

    trace = go.Bar(x=usedf['Submission Date'], y=[],
                   offsetgroup=0,
                   base=0,
                   marker_color=markercolor,
                   showlegend=True,
                   visible=visible,
                   name=c
                   )

    # Add that trace to the figure
    fig.add_trace(trace, row=1, col=1)

    # Create and add second trace for data table
    trace2 = go.Table(header=dict(values=['Submission Date', 'variable', 'value']),
                      cells=dict(values=[['test', 'test', c]]),
                      visible=visible
                      )

    fig.add_trace(trace2, row=2, col=1)
# Add the trace and update a few parameters for the axes.
#fig.update_xaxes(dict(range=['2023-01-01', '2023-08-08']), title='Date', autorange=False)
fig.update_yaxes(title='number of results submitted', rangemode='nonnegative')  # , fixedrange = True)
fig.update_layout(
    title_text='Plotly Misformatting Test',
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


fig.write_html("C:/Users/alexander.onopa/workspace/lrp/plotly_misformatting.html")

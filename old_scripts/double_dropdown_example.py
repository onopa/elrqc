import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

dfA = pd.DataFrame({'group_name': ['group A', 'group A', 'group A', 'group A'], 'Submission Date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'], 'var1': [1, 1, 1, 1], 'var2': [2, 2, 2, 2], 'var3': [3, 3, 3, 3]})
dfB = pd.DataFrame({'group_name': ['group B', 'group B', 'group B', 'group B'], 'Submission Date': ['2023-01-05', '2023-01-06', '2023-01-07', '2023-01-08'], 'var1': [11, 12, 13, 14], 'var2': [12, 16, 14, 13], 'var3': [13, 12, 11, 12]})
dfC = pd.DataFrame({'group_name': ['group C', 'group C', 'group C', 'group C'], 'Submission Date': ['2023-01-09', '2023-01-10', '2023-01-11', '2023-01-12'], 'var1': [21, 15, 22, 16], 'var2': [22, 23, 21, 20], 'var3': [23, 23, 16, 16]})
df = pd.concat([dfA, dfB, dfC])
df['Submission Date'] = pd.to_datetime(df['Submission Date'])

# dummy extra variable for testing - behavior changes when the number of columns is odd vs even(??)
df['dummy variable 4'] = [4, 4, 4, 4, 14, 14, 14, 14, 25, 25, 25, 25]

# dfs for table data used in subplot 2
tabdf1 = pd.DataFrame({'group_name': ['group A', 'group A', 'group A', 'group A'],
                       'Submission Date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'],
                       'variable': ['var1', 'var2', 'var3', 'var4'],
                       'value': ['A val 1', 'A val 2', 'A val 3', 'A val 4']})

tabdf2 = pd.DataFrame({'group_name': ['group B', 'group B', 'group B', 'group B'],
                       'Submission Date': ['2022-06-01', '2022-07-11', '2022-08-01', '2022-09-01'],
                       'variable': ['var1', 'var2', 'var3', 'var4'],
                       'value': ['B val 1', 'B val 2', 'B val 3', 'B val 4']})

tabdf3 = pd.DataFrame({'group_name': ['group C', 'group C', 'group C', 'group C'],
                       'Submission Date': ['2022-01-01', '2022-02-01', '2022-03-01', '2022-04-01'],
                       'variable': ['var1', 'var2', 'var3', 'var4'],
                       'value': ['C val 1', 'C val 2', 'C val 3', 'C val 4']})

table_df = pd.concat([tabdf1, tabdf2, tabdf3])


# fns for building data update dropdown.
def updateTimePlotY(group_df, var):
    return group_df[var]

def updateCells(tab_group_df, var):
    cells_output = []
    use = tab_group_df.loc[tab_group_df['variable'] == var]
    for col in use.columns:
        cells_output.append(use[col].tolist())
    return cells_output


availableGroups = df['group_name'].unique().tolist()
columns = list(df.columns)
# drop columns not used for iteration
columns.pop(columns.index('Submission Date'))
columns.pop(columns.index('group_name'))

# list for the data update dropdown
dropdown_group_selector = []

# Create initial blank value to force group selection
dropdown_group_selector.append(dict(
    args=[{'x': [],
           'y': [],
           'cells': [],
           }],
    label='Select Group',
    method='update'
))

# Fill out rest of dropdown with x/y/cell data for each group
for group_choice in availableGroups:
    # subset data for table cells
    table_df_group = table_df.loc[table_df['group_name'] == group_choice]
    # subset data for scatterplot
    group_subset_df = df.loc[df['group_name'] == group_choice]
    # select column for x data
    x_data = group_subset_df['Submission Date']
    # lists used for update method
    xargs = []
    yargs = []
    cells = []

    for i, c in enumerate(columns):
        # x and y data for scatter
        xargs.append(x_data)
        yargs.append(updateTimePlotY(group_subset_df, c))

        # table data
        celldata = updateCells(table_df_group, c)
        cells.append(dict(values=celldata))

    dropdown_group_selector.append(dict(
        args=[{'x': xargs,
               'y': yargs,
               'cells': cells,
               }],
        label=group_choice,
        method='update'
    ))

# Create dropdown for variable selection
dropdown_var_selector = []

# Create initial blank selection to force initial choice
dropdown_var_selector.append(dict(
        args=[{'visible': False}],
        label='Select Variable',
        method='restyle'
    ))

# Fill out variable selection dropdown
for i, c in enumerate(columns):
    # why does this work...
    vis = ([False] * (len(columns)))
    vis[i] = True

    # ...instead of this?
    # vis = ([False] * 2 * len(columns))
    # for v in [2*i, 2*i+1]:
    #     vis[v] = True

    dropdown_var_selector.append(dict(
        args=[{'visible': vis}],
        label=c,
        method='restyle'
    ))

# data to initialize the traces
group = availableGroups[0]
usedf = df.loc[df['group_name'] == group]
usedf_tab = table_df.loc[table_df['group_name'] == group]

fig = make_subplots(rows=2, cols=1,
                    specs=[[{'type': 'scatter'}],
                           [{'type': 'table'}]])

# Add traces for each column
for i, c in enumerate(columns):
    vis = False
    # if i == 0:
    #     vis = True

    # Create scatter trace
    trace = go.Scatter(x=usedf['Submission Date'], y=usedf[c],
                       mode='markers',
                       opacity=1,
                       marker_color='blue',
                       showlegend=True,
                       hovertemplate='Date: %{x}<br>Number: %{y}<extra></extra>',
                       visible=vis,
                       name=c
                       )

    # Add that trace to the figure
    fig.add_trace(trace, row=1, col=1)

    # get data for table trace
    initial_cells = updateCells(usedf_tab, c)
    # Create and add second trace for data table
    trace2 = go.Table(header=dict(values=['group', 'submission date', 'variable', 'value']),
                      cells=dict(values=initial_cells),
                      visible=vis,
                      name='table' + str(i)
                      )

    fig.add_trace(trace2, row=2, col=1)

# update a few parameters for the axes
fig.update_xaxes(title='Date')
fig.update_yaxes(title='value', rangemode='nonnegative')  # , fixedrange = True)
fig.update_layout(
    title_text='double dropdown issue',
    # hovermode = 'x',
    height=1000,
    width=850,
    title_y=0.99,
    margin=dict(t=140)
)

# Add the two dropdowns
fig.update_layout(
    updatemenus=[
        # Dropdown menu for choosing the group
        dict(
            buttons=dropdown_group_selector,
            direction='down',
            showactive=True,
            x=0.0,
            xanchor='left',
            y=1.11,
            yanchor='top'
        ),
        # and for the variables
        dict(
            buttons=dropdown_var_selector,
            direction='down',
            showactive=True,
            x=0.0,
            xanchor='left',
            y=1.06,
            yanchor='top'
        )
    ]

)

fig.show()
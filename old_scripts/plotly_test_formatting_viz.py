import pandas as pd
import plotly.graph_objects as go
import glob


# Import data:
df = pd.DataFrame()
for labfile in glob.glob('../misformatting/A*.csv'):
    lfdf = pd.read_csv(labfile)
    df = pd.concat([df, lfdf])

df['Date_reported'] = pd.to_datetime(df['Submission Date'])


# Create functions to update the values

# For the time series plot
# Since there are actually N traces in the time plot (only 1 visible), I will need to send N data sets back from each function
def updateTimePlotX(lab):
    use = df.loc[df['lab_name'] == lab]
    return use['Date_reported']


def updateTimePlotY(lab, var):
    use = df.loc[df['lab_name'] == lab]
    return use[var]


# I am going to create the dropdown list here and then add it to the figure below
# I will need to update the x and y data for the time series plot
# Even though some data will not change, I will need to specify everything in this dropdown menu

# Identify the countries to use
# I will but The United States of America first so that it can be the default country on load (the first button)
availableLabs = df['lab_name'].unique().tolist()
availableLabs.insert(0, availableLabs.pop(availableLabs.index('Advanced Diagnostics (ADL)')))

columns = list(df.columns)

# drop 2 unnecessary columns
columns.pop(0)
columns.pop(1)
# create list for the country dropdown
dropdown_labs = []
for lab_choice in availableLabs:
    xargs = []
    yargs = []
    for i, c in enumerate(columns):
        xargs.append(updateTimePlotX(lab_choice))
        yargs.append(updateTimePlotY(lab_choice, c))
    dropdown_labs.append(dict(
        args=[{'x': xargs,
               'y': yargs}],
        label=lab_choice,
        method='update'
    ))


dropdown_vars = []
for i, c in enumerate(columns):
    vis = [False] * len(columns)
    vis[i] = True
    dropdown_vars.append(dict(
        args=[{'visible': arg}],
        label=c,
        method='restyle'
    ))
# Create the trace, using Scatter to create lines and fill the region between the line and y=0.

# start with one country (I will use the first one in the list, since that is how the dropdowns are initialized)
lab = availableLabs[0]
usedf = df.loc[df['lab_name'] == lab]

# Create the figure.
fig = go.Figure()

# Add traces for each column
#columns = ['n', 'patientTelephoneNumber_num_missing', 'symptomOnsetDate_num_missing', 'Notes_num_missing']
for i, c in enumerate(columns):
    visible = False
    if (i == 0):
        visible = True

    # Create the trace, using Scatter to create lines and fill the region between the line and y=0.
    trace = go.Scatter(x=usedf['Date_reported'], y=usedf[c],
                       mode='markers',  # Set the mode the lines (rather than markers) to show a line.
                       opacity=1,
                       marker_color='black',
                       #fill='tozeroy',  # This will fill between the line and y=0.
                       showlegend=False,
                       name='Lab Count',
                       hovertemplate='Date: %{x}<br>Number: %{y}<extra></extra>',
                       # Note: the <extra></extra> removes the trace label.
                       visible=visible
                       )

    # Add that trace to the figure
    fig.add_trace(trace)


    # Add the trace and update a few parameters for the axes.
fig.update_xaxes(title='Date')
fig.update_yaxes(title='number of results submitted',
                 rangemode='nonnegative')  # , fixedrange = True)
fig.update_layout(
    title_text='Plotly Misformatting Test',
    # hovermode = 'x',
    height=550,
    width=850,
    title_y=0.97,
    margin=dict(t=140)
)

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
            y=1.3,
            yanchor='top'
        ),

        dict(
            buttons=dropdown_vars,
            direction='down',
            showactive=True,
            x=0.0,
            xanchor='left',
            y=1.2,
            yanchor='top'
        )
    ]
)

fig.show()
fig.write_html("C:/Users/alexander.onopa/workspace/lrp/plotly_misformatting.html")
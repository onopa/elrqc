import numpy as np
import pandas as pd
from bokeh.plotting import *
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, CustomJS, Select, RadioButtonGroup, Div, HoverTool
import glob
import datetime


df = pd.DataFrame()
for labfile in glob.glob('../misformatting/*.csv'):
    lfdf = pd.read_csv(labfile)
    df = pd.concat([df, lfdf])

df['Submission Date'] = pd.to_datetime(df['Submission Date'])

availableLabs= df['lab_name'].unique().tolist()

lab = 'Advanced Diagnostics (ADL)'
usedf = df.loc[df['lab_name'] == lab]

source = ColumnDataSource(
    data = dict(
        x = usedf['Submission Date'],
        y = usedf['Total Submission Count'], # included for the tooltips

    )
)

allSource = ColumnDataSource(
    data = dict(
        Date_reported = df['Submission Date'],
        n = df['Total Submission Count'],
        patientAddressZip_num_missing = df['patientAddressZip_num_missing'],
        specimenReceivedDate_num_missing = df['specimenReceivedDate_num_missing'],
        lab_name = df['lab_name']
    )
)


# define the tools you want to use
TOOLS = "pan,wheel_zoom,box_zoom,reset,save"

# define the tooltip
hover_tool = HoverTool(
    tooltips=[
        ( 'Date',   '@x{%F}'),
        ( 'Count',  '@y{int}' ),
    ],
    formatters={
        '@x': 'datetime', # use 'datetime' formatter for '@x' field
    },
    # display a tooltip whenever the cursor is vertically in line with a glyph
    #mode = 'vline'
)

# set limits
xlow = datetime.datetime.strptime('2020-03-01', '%Y-%m-%d')
xhigh = datetime.datetime.today()

ylow = 0
yhigh = 1.1*np.nanmax(usedf['n'])
# create a new plot
f = figure(tools = TOOLS,
    width = 800,
    height = 400,
    x_range = [xlow, xhigh],
    y_range = [ylow, yhigh],
    x_axis_label = 'Date',
    y_axis_label = 'labs submitted'
)
f.tools.append(hover_tool)


# fill the area
#f.varea(x = 'x', y1 = 'y', y2 = 0,
#    source = source,
#    color = 'black',
#    alpha = 0.5
#        )

# draw the points
f.scatter('x', 'y',
       source = source,
       color = 'black',
       alpha = 1,
       #line_width = 2
       )

# create the dropdown menu
# (Note: Bokeh call's this select, because of the html nomenclature; there is also a difference Bokeh Dropdown)
select = Select(title = '',
    value = lab,
    options = df['lab_name'].unique().tolist(),
    width = 200,
    margin = (5, 5, 5, 80)
)

# create some radio buttons
radio = RadioButtonGroup(
    labels = ['Total Submissions', 'Patient Address Zip Missing', 'Specimen Received Date Missing'],
    active = 0,
    width = 100,
    margin = (5, 5, 5, 80)
)

# Javascript code for the callback
callback = CustomJS(
    args=dict(
        source=source,
        allSource=allSource,
        select=select,
        radio=radio,
        ranges=dict(
            x=f.x_range,
            y=f.y_range
        )
    ),
    code=
    """
        // get the value from the dropdown
        var lab_name = select.value;

        //convert the value from the radio button to a key
        var key = null;
        if (radio.active == 0) key = 'n';
        if (radio.active == 1) key = 'patientAddressZip_num_missing';
        if (radio.active == 2) key = 'specimenReceivedDate_num_missing'; 

        // filter the full data set to include only those from that country
        if (key){
            var x = allSource.data.Date_reported.filter(function(d,i){return allSource.data.lab_name[i] == lab_name});
            var y = allSource.data[key].filter(function(d,i){return allSource.data.lab_name[i] == lab_name});

            //update the data in the plot
            source.data.x = x;
            source.data.y = y;
            source.change.emit();

            //reset the axis limits
            //note that this ... syntax may not work on all browsers
            ranges.x.start = Math.min(...x); 
            ranges.x.end =  Math.max(...x);
            ranges.y.start = Math.max(Math.min(...y),0); 
            ranges.y.end =  Math.max(...y);
        }

    """
)

#attach the callback
select.js_on_change('value', callback)
radio.js_on_change('active', callback)

show(column([
    Div(text = 'Bokeh Lab Missingness'),
    select,
    radio,
    f]
))
import pandas as pd
import numpy as np

# COVID-19 cases and deaths as a function of time for multiple countries
# df = pd.read_csv('data/WHO-COVID-19-global-data.csv') # in case the WHO server goes down
df = pd.read_csv('https://covid19.who.int/WHO-COVID-19-global-data.csv')

# convert the date column to datetime objects for easier plotting and manipulation later on
df['Date_reported'] = pd.to_datetime(df['Date_reported'])

# limit to date that the blog was written
endDate = np.datetime64('2022-01-27')
df = df.loc[df['Date_reported'] < endDate]

# get all the available countries from the data
availableCountries = df['Country'].unique().tolist()

# I'll also want to take rolling means over 7 day intervals for each country
rollingAve = 7

# The DataFrame is already sorted by country, so I will just go through each country and append to a list
r1 = []
r2 = []
r3 = []
r4 = []
for c in availableCountries:
    usedf =  df.loc[df['Country'] == c]
    r1 += usedf['New_cases'].rolling(rollingAve).mean().to_list()
    r2 += usedf['New_deaths'].rolling(rollingAve).mean().to_list()
    r3 += usedf['Cumulative_cases'].rolling(rollingAve).mean().to_list()
    r4 += usedf['Cumulative_deaths'].rolling(rollingAve).mean().to_list()
df['New_cases_rolling'] = np.nan_to_num(r1)
df['New_deaths_rolling'] = np.nan_to_num(r2)
df['Cumulative_cases_rolling'] = np.nan_to_num(r3)
df['Cumulative_deaths_rolling'] = np.nan_to_num(r4)

df



from bokeh.plotting import *
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, CustomJS, Select, RadioButtonGroup, Div, HoverTool

output_notebook()
# un-comment the line below to output to an html file
output_file('bokeh_COVID.html')

# start with one country (with Bokeh, I can choose any country to start with)
country = 'United States of America'
usedf = df.loc[df['Country'] == country]

# create a ColumnDataSource containing only the data I want to plot
# (Note: I only need to convert the pandas DataFrame to a ColumnDataSource because I want to manipulate it later in Javascript)
source = ColumnDataSource(
    data=dict(
        x=usedf['Date_reported'],
        y=usedf['New_cases_rolling'],  # included for the tooltips

    )
)

# create a ColumnDataSource containing all the necessary data so that I can send it to javascript for filtering
allSource = ColumnDataSource(
    data=dict(
        Date_reported=df['Date_reported'],
        New_cases_rolling=df['New_cases_rolling'],
        New_deaths_rolling=df['New_deaths_rolling'],
        Cumulative_cases_rolling=df['Cumulative_cases_rolling'],
        Cumulative_deaths_rolling=df['Cumulative_deaths_rolling'],
        Country=df['Country']
    )
)

# define the tools you want to use
TOOLS = "pan,wheel_zoom,box_zoom,reset,save"

# define the tooltip
hover_tool = HoverTool(
    tooltips=[
        ('Date', '@x{%F}'),
        ('Count', '@y{int}'),
    ],
    formatters={
        '@x': 'datetime',  # use 'datetime' formatter for '@x' field
    },
    # display a tooltip whenever the cursor is vertically in line with a glyph
    # mode = 'vline'
)

# create a new plot
f = figure(tools=TOOLS,
           width=800,
           height=400,
           x_range=[np.nanmin(usedf['Date_reported']), np.nanmax(usedf['Date_reported'])],
           y_range=[max(np.nanmin(usedf['New_cases_rolling']), 0), np.nanmax(usedf['New_cases_rolling'])],
           x_axis_label='Date',
           y_axis_label='COVID-19 Count (' + str(rollingAve) + '-day rolling average)'
           )
f.tools.append(hover_tool)

# fill the area
f.varea(x='x', y1='y', y2=0,
        source=source,
        color='black',
        alpha=0.5
        )

# draw the line
f.line('x', 'y',
       source=source,
       color='black',
       alpha=1,
       line_width=2
       )

# create the dropdown menu
# (Note: Bokeh call's this select, because of the html nomenclature; there is also a difference Bokeh Dropdown)
select = Select(title='',
                value=country,
                options=df['Country'].unique().tolist(),
                width=200,
                margin=(5, 5, 5, 80)
                )

# create some radio buttons
radio = RadioButtonGroup(
    labels=['Daily Cases', 'Daily Deaths', 'Cumulative Cases', 'Cumulative Deaths'],
    active=0,
    width=100,
    margin=(5, 5, 5, 80)
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
        var country = select.value;

        //convert the value from the radio button to a key
        var key = null;
        if (radio.active == 0) key = 'New_cases_rolling';
        if (radio.active == 1) key = 'New_deaths_rolling';
        if (radio.active == 2) key = 'Cumulative_cases_rolling';
        if (radio.active == 3) key = 'Cumulative_deaths_rolling';      

        // filter the full data set to include only those from that country
        if (key){
            var x = allSource.data.Date_reported.filter(function(d,i){return allSource.data.Country[i] == country});
            var y = allSource.data[key].filter(function(d,i){return allSource.data.Country[i] == country});

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

# attach the callback
select.js_on_change('value', callback)
radio.js_on_click(callback)

show(column([
    Div(text='<h1>Bokeh COVID-19 Data Explorer</h1>'),
    select,
    radio,
    f]
))


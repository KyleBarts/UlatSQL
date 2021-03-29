
from reading import Reading
from base import Session
from station import Station
from datetime import datetime, timedelta
from time import time as timer
import dash
import dash_core_components as dcc
import plotly.graph_objs as go
import dash_html_components as html
import pandas as pd
import plotly.express as px
import fetchFunctions as Fetcher
import processFunctions as Processer
import requests
#import queryTest as queryTest



station_details = pd.read_csv('StationDetails2.csv', dtype=object, names=['Station Name','Serial'])
station_details = station_details.astype(str)

reading_details = pd.read_csv('reading_details.csv')
parameter_names = reading_details['parameter_name']
print(reading_details)

stations = Fetcher.fetchAllStations()
print(stations)

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

df = pd.read_csv('https://plotly.github.io/datasets/country_indicators.csv')

available_indicators = df['Indicator Name'].unique()

station_names = stations['location']

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

index_page = html.Div([
    dcc.Link('Go to POTEKA Data viewer', href='/poteka'),
    html.Br(),
    dcc.Link('Go to EarthNetworks Data Viewer', href='/earthnetworks'),
])



poteka_layout = html.Div([
    html.Div([
        #Station Select Dropdown
        html.Div([ 
                dcc.Dropdown(
                    id='poteka-station-select',
                    options=[{'label': i, 'value': i} for i in station_names],
                    value='Station names',
                    placeholder="Select POTEKA Station "
                )
            ],
            style={'width': '49%', 'display': 'inline-block'}),
        #Start & End Date Text Inputs
        html.Div([
                dcc.Input(
                id="poteka-input-start-date",
                type="text",
                placeholder="input start date (ex. '2020-05-15 07:45:00') ",
                style={'width': '49%', 'display': 'inline-block'}
                ),
                dcc.Input(
                id="poteka-input-end-date",
                type="text",
                placeholder="input end date",
                style={'width': '49%', 'display': 'inline-block'}
                )
            ], style={'width': '49%', 'float': 'right', 'display': 'inline-block'}),
        #Parameter Select Dropdown
        html.Div([
                dcc.Dropdown(
                    id='poteka-reading-select',
                    options=[{'label': i, 'value': i} for i in parameter_names],
                    value='reading names',
                    placeholder="Select Parameter to View",
                    style={'width': '49%','display': 'inline-block'}
                    
                ),
                html.Button('Submit', 
                id='poteka_submit-val', 
                n_clicks=0,
                style={'width': '25%', 'display': 'inline-block'})
            ], style={'width': '100%', 'display': 'inline-block'})


    ], style={
        'borderBottom': 'thin lightgrey solid',
        'backgroundColor': 'rgb(250, 250, 250)',
        'padding': '25px 5px'
    }),
    #Graph 
    html.Div([
        dcc.Graph(id='x-time-series')
    ], style={'display': 'inline-block', 'width': '100%'})
])

@app.callback(
    dash.dependencies.Output('x-time-series', 'figure'),
    [dash.dependencies.Input('poteka-submit-val', 'n_clicks'),
    dash.dependencies.State('poteka-station-select', 'value'),
    dash.dependencies.State('poteka-reading-select', 'value'),
    dash.dependencies.State('poteka-input-start-date', 'value'),
    dash.dependencies.State('poteka-input-end-date', 'value')])
def update_output(n_clicks, station_name, reading_name, start_date, end_date):
    out_string = 'The input value was "{}" and the button has been clicked {} times'.format(
        station_name,
        n_clicks
    )
    print(out_string)
    station_data = stations.loc[stations['location'] == station_name]
    station_id = station_data['station_id'].item()
    print(station_id)

    parameter_data = reading_details.loc[reading_details['parameter_name'] == reading_name]
    parameter_id = parameter_data['parameter_id'].item()
    parameter_type = parameter_data['parameter_type'].item()
    print(parameter_id)
    print(parameter_type)

    if None not in (station_name, reading_name, start_date, end_date):
        print('COMPLETE')
        station_list = []
        reading_list = []

        station_list.append(station_id)
        reading_list.append(parameter_id)
        if(parameter_type=='weather'):
            response = Fetcher.genericFetchFunction(start_date,end_date,station_list,reading_list)
            events = response[1]

            events['reading'] = events['reading'].astype(float)
            fig = px.scatter(events, x='datetime_read', y='reading')
        if(parameter_type=='health'):
            response = Fetcher.genericHealthFetchFunction(start_date,end_date,station_list,reading_list)
            events = response[1]
            fig = px.scatter(events, x='datetime_read', y='health')
        events = response[1]
        print(events)
        #Apply Conversion for Temp


        #events['reading'] = events['reading'].to_numeric()
        print(events)
        #fig = px.scatter(events, x='datetime_read', y='reading')
        #fig.update_traces(mode='lines+markers')
        fig.update_yaxes(title=reading_name)
        fig.update_xaxes(title='Date & Time (PST)')

        

        return fig
mapbox_access_token = open(".mapbox_token").read()

layout = go.Layout(
        title="My Dash Graph",
        height=800
        )
earthnetworks_fig = go.Figure(layout=layout)
earthnetworks_fig.add_trace(go.Scattermapbox(
lat=[],
lon=[],
mode='markers',
marker=go.scattermapbox.Marker(
    size=5,
    color='rgb(255,255,0)',
    opacity=0.7
),
text=[],
hoverinfo='text'
))

earthnetworks_fig.update_layout(
    title='PAGASA Lightning Events',
    autosize=True,
    hovermode='closest',
    showlegend=False,
    mapbox=dict(
        accesstoken=mapbox_access_token,
        bearing=0,
        center=dict(
            lat=14.57773,
            lon=121.034
        ),
        pitch=0,
        zoom=4,
        style='dark'
    ),
)

earthnetworks_layout = html.Div([
    html.Div([
        #Station Select Dropdown

        #Start & End Date Text Inputs
        html.Div([
                dcc.Input(
                id="earthnetworks-input-start-date",
                type="text",
                placeholder="input start date (ex. '2020-05-15 07:45:00') ",
                style={'width': '49%', 'display': 'inline-block'}
                ),
                dcc.Input(
                id="earthnetworks-input-end-date",
                type="text",
                placeholder="input end date",
                style={'width': '49%', 'display': 'inline-block'}
                )
            ], style={'width': '49%', 'float': 'left', 'display': 'inline-block'}),
        #Parameter Select Dropdown
        html.Div([
                dcc.Checklist(id='earthnetworks-vpoteka-match',
                    options=[
                        {'label': 'Match V-POTEKA Data', 'value': True},
                    ],
                    value=[],

                ), 
                html.Button('Submit', 
                id='earthnetworks-submit-val', 
                n_clicks=0,
                style={'width': '25%', 'display': 'inline-block'})
            ], style={'width': '100%', 'display': 'inline-block'})


    ], style={
        'borderBottom': 'thin lightgrey solid',
        'backgroundColor': 'rgb(250, 250, 250)',
        'padding': '25px 5px'
    }),
    #Graph 
    html.Div([
        
        dcc.Graph(id='earthnetworks-map', figure=earthnetworks_fig)
    ], style={'display': 'inline-block', 'width': '100%'})
])

@app.callback(
    dash.dependencies.Output('earthnetworks-map', 'figure'),
    [dash.dependencies.Input('earthnetworks-submit-val', 'n_clicks'),
    dash.dependencies.State('earthnetworks-input-start-date', 'value'),
    dash.dependencies.State('earthnetworks-input-end-date', 'value'),
    dash.dependencies.State('earthnetworks-vpoteka-match', 'value')])
def earthnetworks_update_output(n_clicks, start_date, end_date, match):
    out_string = 'The input value was "{}" and the button has been clicked {} times'.format(
        start_date,
        n_clicks
    )
    print(out_string)
    if None not in (start_date, end_date):
        payload = {'start_date': start_date,
                'end_date':end_date,
                'flash_type':0
            }
        pagasa_response = requests.get(
            'http://192.168.6.179:8080/earthnetworks',
            params=payload,
        )
        
        pagasa_json = pagasa_response.content
        pagasa_df = pd.read_json(pagasa_json,orient='records')
        #print(pagasa_df)
        
        en_data_lat = pagasa_df.latitude
        en_data_lon = pagasa_df.longitude
        en_data_time = pagasa_df.lightning_time
        fig = go.Figure()

        fig.add_trace(go.Scattermapbox(
        lat=en_data_lat,
        lon=en_data_lon,
        mode='markers',
        marker=go.scattermapbox.Marker(
            size=5,
            color='rgb(255,255,0)',
            opacity=0.7
        ),
        text=en_data_time,
        hoverinfo='text'
        ))

        if(len(match)>0):
            v_payload = {'start_date': start_date,
                'end_date':end_date
            }
            v_response = requests.get(
                'http://192.168.6.179:8080/vpoteka',
                params=v_payload,
            )
            vpoteka_json = v_response.content
            vpoteka_df = pd.read_json(vpoteka_json,orient='records')

            print(vpoteka_df)
            #First collect all seconds where vpoteka lightning occurs
            timestamps = vpoteka_df['datetime_read'].astype(str).tolist()
            print(timestamps)
            #Filter Pagasa Data according to collected seconds
            matched_pagasa_df = pagasa_df[pagasa_df.lightning_time.isin(timestamps)]

            matched_data_lat = matched_pagasa_df.latitude
            matched_data_lon = matched_pagasa_df.longitude
            matched_data_time = matched_pagasa_df.lightning_time

            fig.add_trace(go.Scattermapbox(
                lat=matched_data_lat,
                lon=matched_data_lon,
                mode='markers',
                marker=go.scattermapbox.Marker(
                    size=5,
                    color='rgb(255,0,0)',
                    opacity=0.7
                ),
                text=matched_data_time,
                hoverinfo='text'
                ))



            

        fig.update_layout(
            title='PAGASA Lightning Events',
            autosize=True,
            hovermode='closest',
            showlegend=False,
            mapbox=dict(
                accesstoken=mapbox_access_token,
                bearing=0,
                center=dict(
                    lat=14.57773,
                    lon=121.034
                ),
                pitch=0,
                zoom=4,
                style='dark'
            ),
        )


    return fig
    
# Update the index
@app.callback(dash.dependencies.Output('page-content', 'children'),
              [dash.dependencies.Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/poteka':
        return poteka_layout
    elif pathname == '/earthnetworks':
        return earthnetworks_layout
    else:
        return index_page
    # You could also return a 404 "URL not found" page here


if __name__ == '__main__':
    app.run_server(debug=False, dev_tools_ui=None, dev_tools_hot_reload=True,dev_tools_props_check=False, port=80, host='0.0.0.0')
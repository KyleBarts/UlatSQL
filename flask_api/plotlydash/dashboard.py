
from reading import Reading
from base import Session
from station import Station
from datetime import datetime, timedelta
from time import time as timer
import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.express as px
import fetchFunctions as Fetcher
import processFunctions as Processer
#import queryTest as queryTest

station_details = pd.read_csv('../../StationDetails2.csv', dtype=object, names=['Station Name','Serial'])
station_details = station_details.astype(str)

reading_details = pd.read_csv('../../reading_details.csv')
parameter_names = reading_details['parameter_name']
print(reading_details)
stations = Fetcher.fetchAllStations()
print(stations)
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']


def init_dashboard(server):
    




    dash_app = dash.Dash(server=server,routes_pathname_prefix='/dashapp/',external_stylesheets=external_stylesheets)

    station_names = stations['location']

    app.layout = html.Div([
        html.Div([
            html.Div([
                    dcc.Dropdown(
                        id='station-select',
                        options=[{'label': i, 'value': i} for i in station_names],
                        value='Station names',
                        placeholder="Select POTEKA Station "
                    )
                ],
                style={'width': '49%', 'display': 'inline-block'}),
                # html.Div([
                #     dcc.Dropdown(
                #         id='crossfilter-xaxis-column',
                #         options=[{'label': i, 'value': i} for i in available_indicators],
                #         value='Fertility rate, total (births per woman)'
                #     ),
                #     dcc.RadioItems(
                #         id='crossfilter-xaxis-type',
                #         options=[{'label': i, 'value': i} for i in ['Linear', 'Log']],
                #         value='Linear',
                #         labelStyle={'display': 'inline-block'}
                #     )
                # ],
                # style={'width': '49%', 'display': 'inline-block'}),

            html.Div([
                    dcc.Input(
                    id="input-start-date",
                    type="text",
                    placeholder="input start date (ex. '2020-05-15 07:45:00') ",
                    style={'width': '49%', 'display': 'inline-block'}
                    ),
                    dcc.Input(
                    id="input-end-date",
                    type="text",
                    placeholder="input end date",
                    style={'width': '49%', 'display': 'inline-block'}
                    )
                    # dcc.Dropdown(
                    #     id='month',
                    #     options=[{'label': i, 'value': i} for i in available_indicators],
                    #     value='Life expectancy at birth, total (years)'
                    # ),
                    # dcc.RadioItems(
                    #     id='crossfilter-yaxis-type',
                    #     options=[{'label': i, 'value': i} for i in ['Linear', 'Log']],
                    #     value='Linear',
                    #     labelStyle={'display': 'inline-block'}
                    # )
                ], style={'width': '49%', 'float': 'right', 'display': 'inline-block'}),
            html.Div([
                    dcc.Dropdown(
                        id='reading-select',
                        options=[{'label': i, 'value': i} for i in parameter_names],
                        value='reading names',
                        placeholder="Select Parameter to View",
                        style={'width': '49%','display': 'inline-block'}
                        
                    ),
                    html.Button('Submit', 
                    id='submit-val', 
                    n_clicks=0,
                    style={'width': '25%', 'display': 'inline-block'})
                ], style={'width': '100%', 'display': 'inline-block'})


        ], style={
            'borderBottom': 'thin lightgrey solid',
            'backgroundColor': 'rgb(250, 250, 250)',
            'padding': '25px 5px'
        }),
        # html.Div([

        #     html.Button('Submit', id='submit-val', n_clicks=0),
        # ], style={'display': 'inline-block','width': '100%'}),
        # html.Div([
        #     dcc.Graph(
        #         id='crossfilter-indicator-scatter',
        #         hoverData={'points': [{'customdata': 'Japan'}]}
        #     )
        # ], style={'width': '49%', 'display': 'inline-block', 'padding': '0 20'}),
        html.Div([
            dcc.Graph(id='x-time-series')
        ], style={'display': 'inline-block', 'width': '100%'})
    ])

    init_callbacks(dash_app)

    return dash_app.server


def init_callbacks(dash_app):

    @app.callback(
        dash.dependencies.Output('x-time-series', 'figure'),
        [dash.dependencies.Input('submit-val', 'n_clicks'),
        dash.dependencies.State('station-select', 'value'),
        dash.dependencies.State('reading-select', 'value'),
        dash.dependencies.State('input-start-date', 'value'),
        dash.dependencies.State('input-end-date', 'value')])
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

        

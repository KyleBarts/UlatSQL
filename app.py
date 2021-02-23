
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
import queryTest as queryTest

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
    print(parameter_id)

    if None not in (station_name, reading_name, start_date, end_date):
        print('COMPLETE')
        station_list = []
        reading_list = []

        station_list.append(station_id)
        reading_list.append(parameter_id)
        response = Fetcher.genericFetchFunction(start_date,end_date,station_list,reading_list)
        events = response[1]
        print(events)
        #Apply Conversion for Temp
        if(parameter_id==5):
            events['reading'] = events['reading'].apply(Processer.convert_temp)
        if(parameter_id==6):
            events['reading'] = events['reading'].apply(Processer.convert_pressure)

        #events['reading'] = events['reading'].to_numeric()
        print(events)
        fig = px.scatter(events, x='datetime_read', y='reading')
        fig.update_traces(mode='lines+markers')
        fig.update_yaxes(title=reading_name)
        fig.update_xaxes(title='Date & Time (PST)')

        

        return fig

    

# def create_time_series(dff, axis_type, title):

#     fig = px.scatter(dff, x='Reading', y='Value')

#     fig.update_traces(mode='lines+markers')

#     fig.update_xaxes(showgrid=False)

#     fig.update_yaxes(type='linear' if axis_type == 'Linear' else 'log')

#     fig.add_annotation(x=0, y=0.85, xanchor='left', yanchor='bottom',
#                        xref='paper', yref='paper', showarrow=False, align='left',
#                        bgcolor='rgba(255, 255, 255, 0.5)', text=title)

#     fig.update_layout(height=225, margin={'l': 20, 'b': 30, 'r': 10, 't': 10})

#     return fig
# @app.callback(
#     dash.dependencies.Output('crossfilter-indicator-scatter', 'figure'),
#     [dash.dependencies.Input('crossfilter-xaxis-column', 'value'),
#      dash.dependencies.Input('crossfilter-yaxis-column', 'value'),
#      dash.dependencies.Input('crossfilter-xaxis-type', 'value'),
#      dash.dependencies.Input('crossfilter-yaxis-type', 'value'),
#      dash.dependencies.Input('crossfilter-year--slider', 'value')])
# def update_graph(xaxis_column_name, yaxis_column_name,
#                  xaxis_type, yaxis_type,
#                  year_value):
#     dff = df[df['Year'] == year_value]

#     fig = px.scatter(x=dff[dff['Indicator Name'] == xaxis_column_name]['Value'],
#             y=dff[dff['Indicator Name'] == yaxis_column_name]['Value'],
#             hover_name=dff[dff['Indicator Name'] == yaxis_column_name]['Country Name']
#             )

#     fig.update_traces(customdata=dff[dff['Indicator Name'] == yaxis_column_name]['Country Name'])

#     fig.update_xaxes(title=xaxis_column_name, type='linear' if xaxis_type == 'Linear' else 'log')

#     fig.update_yaxes(title=yaxis_column_name, type='linear' if yaxis_type == 'Linear' else 'log')

#     fig.update_layout(margin={'l': 40, 'b': 40, 't': 10, 'r': 0}, hovermode='closest')

#     return fig


# def create_time_series(dff, axis_type, title):

#     fig = px.scatter(dff, x='Year', y='Value')

#     fig.update_traces(mode='lines+markers')

#     fig.update_xaxes(showgrid=False)

#     fig.update_yaxes(type='linear' if axis_type == 'Linear' else 'log')

#     fig.add_annotation(x=0, y=0.85, xanchor='left', yanchor='bottom',
#                        xref='paper', yref='paper', showarrow=False, align='left',
#                        bgcolor='rgba(255, 255, 255, 0.5)', text=title)

#     fig.update_layout(height=225, margin={'l': 20, 'b': 30, 'r': 10, 't': 10})

#     return fig


# @app.callback(
#     dash.dependencies.Output('x-time-series', 'figure'),
#     [dash.dependencies.Input('crossfilter-indicator-scatter', 'hoverData'),
#      dash.dependencies.Input('crossfilter-xaxis-column', 'value'),
#      dash.dependencies.Input('crossfilter-xaxis-type', 'value')])
# def update_y_timeseries(hoverData, xaxis_column_name, axis_type):
#     country_name = hoverData['points'][0]['customdata']
#     dff = df[df['Country Name'] == country_name]
#     dff = dff[dff['Indicator Name'] == xaxis_column_name]
#     title = '<b>{}</b><br>{}'.format(country_name, xaxis_column_name)
#     return create_time_series(dff, axis_type, title)


# @app.callback(
#     dash.dependencies.Output('y-time-series', 'figure'),
#     [dash.dependencies.Input('crossfilter-indicator-scatter', 'hoverData'),
#      dash.dependencies.Input('crossfilter-yaxis-column', 'value'),
#      dash.dependencies.Input('crossfilter-yaxis-type', 'value')])
# def update_x_timeseries(hoverData, yaxis_column_name, axis_type):
#     dff = df[df['Country Name'] == hoverData['points'][0]['customdata']]
#     dff = dff[dff['Indicator Name'] == yaxis_column_name]
#     return create_time_series(dff, axis_type, yaxis_column_name)

# @app.callback(
#     dash.dependencies.Output('y-time-series', 'figure'),
#     [dash.dependencies.Input('crossfilter-indicator-scatter', 'hoverData'),
#      dash.dependencies.Input('crossfilter-yaxis-column', 'value'),
#      dash.dependencies.Input('crossfilter-yaxis-type', 'value')])
# def update_x_timeseries(hoverData, yaxis_column_name, axis_type):
#     dff = df[df['Country Name'] == hoverData['points'][0]['customdata']]
#     dff = dff[dff['Indicator Name'] == yaxis_column_name]
#     return create_time_series(dff, axis_type, yaxis_column_name)


if __name__ == '__main__':
    app.run_server(debug=True, port=8080, host='0.0.0.0')
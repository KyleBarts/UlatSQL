import plotly.graph_objects as go
import pandas as pd
from datetime import datetime

mapbox_access_token = open(".mapbox_token").read()

df_geolocated = pd.read_csv("./outputs/geolocated.csv", encoding='iso-8859-1')
df_geolocated['gradient']=df_geolocated.lightning_time.apply(lambda x: 'rgb(0,0,255)')
print(df_geolocated)
site_lat_geo = df_geolocated.latitude
site_lon_geo = df_geolocated.longitude
locations_name_geo = df_geolocated.lightning_time
gradient_geo = df_geolocated.gradient

timestamps = df_geolocated.lightning_time.astype(str).tolist()
print(timestamps)


df = pd.read_csv("./outputs/Oct30_Pasig_Pagasa_Range_Lightning.csv", encoding='iso-8859-1')
#print(df)
#df = df[df.lightning_time.isin(timestamps)]

df['gradient']=df.lightning_time.apply(lambda x: 'rgb('+str(int((255/31)*(datetime.strptime(x,'%Y-%m-%d %H:%M:%S').day)))+', '+str(255-int((255/31)*(datetime.strptime(x,'%Y-%m-%d %H:%M:%S').day))) +', 0)')
print(df)
site_lat = df.latitude
site_lon = df.longitude
locations_name = df.lightning_time
gradient = df.gradient

df_match = pd.read_csv("./outputs/Oct30_Pasig_Pagasa_Lightning.csv", encoding='iso-8859-1')

#df_match = df_match[df_match.lightning_time.isin(timestamps)]

df_match['gradient']=df_match.lightning_time.apply(lambda x: 'rgb(255,255,0)')
print(df_match)
site_lat_match = df_match.latitude
site_lon_match = df_match.longitude
locations_name_match = df_match.lightning_time
gradient_match = df_match.gradient



fig = go.Figure()





fig.add_trace(go.Scattermapbox(
    lat=site_lat,
    lon=site_lon,
    mode='markers',
    marker=go.scattermapbox.Marker(
        size=5,
        color=gradient,
        opacity=0.7
    ),
    text=locations_name,
    hoverinfo='text'
))

fig.add_trace(go.Scattermapbox(
    lat=site_lat_match,
    lon=site_lon_match,
    mode='markers',
    marker=go.scattermapbox.Marker(
        size=5,
        color=gradient_match,
        opacity=0.7
    ),
    text=locations_name_match,
    hoverinfo='text'
))

fig.add_trace(go.Scattermapbox(
    lat=site_lat_geo,
    lon=site_lon_geo,
    mode='markers',
    marker=go.scattermapbox.Marker(
        size=5,
        color=gradient_geo,
        opacity=0.7
    ),
    text=locations_name_geo,
    hoverinfo='text'
))



fig.update_layout(
    title='PAGASA Lightning Events for May 27, 2020 18:30 to 19:30',
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

#fig.show()
fig.write_html('MapTest.html', auto_open=True)
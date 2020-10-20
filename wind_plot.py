import plotly.graph_objects as go
import pandas as pd
from plotly.subplots import make_subplots

# Create figure with secondary y-axis
fig = make_subplots(specs=[[{"secondary_y": True}]])

# rain_df = pd.read_csv("Xavier ERA May.csv")
# lightning_df = pd.read_csv("Xavier Lightning May.csv")
# wind_df = pd.read_csv("Xavier Wind May.csv")

humidity_df = pd.read_csv("PAGASA Science Garden Humidity April.csv")
rain_df = pd.read_csv("PAGASA Science Garden Rain April.csv")
temp_df = pd.read_csv("PAGASA Science Garden Temp April.csv")

# Add traces


fig.add_trace(
    go.Scatter(x=humidity_df['datetime_read'], y=humidity_df['reading'], name="Humidity Data",marker_color='red'),
    secondary_y=False,
)

fig.add_trace(
    go.Scatter(x=rain_df['datetime_read'], y=rain_df['reading'], name="Rain Data",marker_color='blue'),
    secondary_y=True,
)
fig.add_trace(
    go.Scatter(x=temp_df['datetime_read'], y=temp_df['reading'], name="Temp Data",marker_color='green'),
    secondary_y=True,
)

# Add figure title
fig.update_layout(
    title_text="PAGASA Science Garden Rain, Humidity, and Temp Plot"
)

# Set x-axis title
fig.update_xaxes(title_text="Date & Time (PST)")

# Set y-axes titles
fig.update_yaxes(title_text="<b>primary</b> Humidity", secondary_y=False)
fig.update_yaxes(title_text="<b>secondary</b>  Rain", secondary_y=True)

fig.write_html('Triple PAGASA ScienceGarden Plot.html', auto_open=True)
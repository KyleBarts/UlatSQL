import plotly.graph_objects as go
import pandas as pd
from plotly.subplots import make_subplots

# Create figure with secondary y-axis
fig = make_subplots(specs=[[{"secondary_y": True}]])

# rain_df = pd.read_csv("Xavier ERA May.csv")
# lightning_df = pd.read_csv("Xavier Lightning May.csv")
# wind_df = pd.read_csv("Xavier Wind May.csv")

rain_df = pd.read_csv("./outputs/LegazpiRain.csv")
#lightning_df = pd.read_csv("UPLB Lightning May.csv")
wind_df = pd.read_csv("./outputs/LegazpiWind.csv")
# Add traces
fig.add_trace(
    go.Scatter(x=rain_df['datetime_read'], y=rain_df['reading'], name="Rain Data"),
    secondary_y=False,
)

# fig.add_trace(
#     go.Scatter(x=lightning_df['datetime_read'], y=lightning_df['count'], name="Lightning Data"),
#     secondary_y=True,
# )

fig.add_trace(
    go.Scatter(x=wind_df['datetime_read'], y=wind_df['reading'], name="Wind Data"),
    secondary_y=True,
)


# Add figure title
fig.update_layout(
    title_text="Legazpi Wind and Rain Plot"
)

# Set x-axis title
fig.update_xaxes(title_text="Date & Time (PST)")

# Set y-axes titles
fig.update_yaxes(title_text="<b>primary</b> Effective Rain Amount", secondary_y=False)
fig.update_yaxes(title_text="<b>secondary</b> Wind Speeds", secondary_y=True)

fig.write_html('Double Legazpi Plot.html', auto_open=True)
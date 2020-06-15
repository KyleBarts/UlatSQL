import pandas as pd
import plotly.express as px

df = pd.read_csv("testing_lightning.csv")

# fig = px.line(df, x="Date & Time (PST)", y="reading", color ="Station Name",
#               line_group="station_id", hover_name="Station Name")

fig = px.line(df, x="datetime_read", y="count", color ="Station Name",
              line_group="station_id", hover_name="Station Name")
#fig.show()
fig.write_html('tmp.html', auto_open=True)
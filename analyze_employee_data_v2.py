import preswald
import pandas as pd
import plotly.express as px

def render():
    preswald.text("# 📊 Callitrichidae Analysis V2")

    # Load your data
    df = pd.read_csv("data/employee_data.csv")

    # Simple chart to confirm it's working
    fig = px.scatter_geo(
        df,
        lat="decimalLatitude",
        lon="decimalLongitude",
        hover_name="speciesQueried",
        color="country",
        title="Geographical Distribution"
    )
    preswald.plotly(fig)

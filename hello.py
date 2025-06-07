import pandas as pd
import plotly.express as px
import preswald

def render():
    preswald.text("# 👋 Hello from hello.py")
    preswald.text("This is your Hello page content.")
    # Load data
    df = pd.read_csv('data/employee_data.csv')

    # Geographic Scatter Plot
    fig = px.scatter_geo(
        df,
        lat="decimalLatitude",
        lon="decimalLongitude",
        hover_name="speciesQueried",
        color="country",
        title="Geographical Distribution of Callitrichidae Specimens",
        projection="natural earth"
    )

    # Density Heatmap
    hotspot_map = px.density_mapbox(
        df,
        lat="decimalLatitude",
        lon="decimalLongitude",
        radius=8,
        zoom=3,
        color_continuous_scale="Viridis",
        mapbox_style="carto-positron",
        title="Spatial Density Analysis of Callitrichidae Populations"
    )

    # Species Richness
    richness = df.groupby("country")["speciesQueried"].nunique().reset_index(name="richness")
    richness_plot = px.bar(
        richness.sort_values("richness", ascending=False),
        x="country",
        y="richness",
        title="Callitrichidae Species Richness by Geographic Region"
    )

    # Treemap
    df_treemap = df.groupby(["country", "speciesQueried"]).size().reset_index(name="count")
    treemap_plot = px.treemap(
        df_treemap,
        path=["country", "speciesQueried"],
        values="count",
        color="count",
        title="Taxonomic and Geographic Distribution of Callitrichidae Specimens"
    )

    # Life Stage Plot
    life_stage_trend = df[df["lifeStage"].notna() & (df["lifeStage"].str.lower() != "unknown")]
    life_stage_plot_data = life_stage_trend.groupby(["year", "lifeStage"]).size().reset_index(name="count")
    life_stage_plot = px.line(
        life_stage_plot_data,
        x="year",
        y="count",
        color="lifeStage",
        title="Ontogenetic Distribution of Callitrichidae Observations: Temporal Analysis"
    )

    # Display visualizations and explanations
    preswald.text("## Spatial Distribution Analysis")
    preswald.plotly(fig)

    preswald.text("## Population Density Assessment")
    preswald.plotly(hotspot_map)

    preswald.text("## Species Richness Evaluation")
    preswald.plotly(richness_plot)

    preswald.text("## Taxonomic Distribution by Geographic Region")
    preswald.plotly(treemap_plot)

    preswald.text("## Ontogenetic Temporal Distribution")
    preswald.plotly(life_stage_plot)

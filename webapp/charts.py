import plotly.graph_objs as go
import plotly.express as px
import pandas as pd


def partner_orgasms_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if df.empty:
        fig.update_layout(title="Partner Orgasms Over Time (no results)")
        return fig

    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["total_org_partner"],
            mode="lines+markers",
            name="Daily",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["trend"],
            mode="lines",
            line={"dash": "dash"},
            name="30-day trend",
        )
    )
    fig.update_layout(
        title="Partner Orgasms Over Time",
        xaxis_title="Date",
        yaxis_title="Total Partner Orgasms",
        width=1000,
        height=500,
    )
    return fig

def rating_histogram_chart(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        fig = go.Figure()
        fig.update_layout(title="Rating Distribution (no results)")
        return fig

    fig = px.histogram(
        df,
        x="rating",
        nbins=5,
        title="Rating Distribution",
        labels={"rating": "Rating", "count": "Entries"},
    )
    fig.update_layout(width=1000, height=400)
    return fig

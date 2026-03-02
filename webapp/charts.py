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


def sex_streaks_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if df.empty:
        fig.update_layout(title="Sex Streaks Over Time (no results)")
        return fig

    shown: set[str] = set()
    for _, row in df.iterrows():
        name = "Sex streak" if row["type"] == "sex" else "No-sex streak"
        show_legend = name not in shown
        shown.add(name)
        fig.add_trace(
            go.Bar(
                x=[row["start_date"]],
                y=[row["signed_length"]],
                base=0,
                width=[max(int(row["length"]), 1) * 86400000],
                name=name,
                marker_color="royalblue" if row["type"] == "sex" else "firebrick",
                hovertemplate="Start: %{x}<br>Length: " + str(row["length"]) + " day(s)<extra></extra>",
                showlegend=show_legend,
            )
        )

    max_abs = int(df["length"].max()) if not df.empty else 1
    fig.update_layout(
        title="Sex Streaks Over Time",
        xaxis_title="Date",
        yaxis_title="Streak Length (days)",
        barmode="overlay",
        width=1000,
        height=500,
    )
    fig.update_yaxes(
        range=[-max_abs - 1, max_abs + 1],
        tickvals=list(range(-max_abs, max_abs + 1)),
        ticktext=[str(abs(v)) for v in range(-max_abs, max_abs + 1)],
    )
    return fig


def position_frequency_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if df.empty:
        fig.update_layout(title="Frequency of Sex Positions (no results)")
        return fig

    chart_df = df.sort_values("count", ascending=True)
    fig = px.bar(
        chart_df,
        x="count",
        y="position",
        orientation="h",
        title="Frequency of Sex Positions",
        labels={"count": "Frequency", "position": "Position"},
        text="count",
    )
    fig.update_layout(width=1000, height=500)
    return fig


def position_combinations_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if df.empty:
        fig.update_layout(title="Position Combination Frequency (no results)")
        return fig

    chart_df = df.sort_values("count", ascending=False).head(15)
    fig = px.bar(
        chart_df,
        x="combination",
        y="count",
        title="Position Combination Frequency (Top 15)",
        labels={"combination": "Combination", "count": "Count"},
    )
    fig.update_layout(width=1000, height=500)
    return fig


def location_room_sankey_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if df.empty:
        fig.update_layout(title="Frequency of Location/Room Combinations (no results)")
        return fig

    locations = sorted(df["location"].unique().tolist())
    rooms = sorted(df["room"].unique().tolist())
    labels = locations + rooms
    idx = {label: i for i, label in enumerate(labels)}

    fig = go.Figure(
        go.Sankey(
            node={"pad": 15, "thickness": 20, "label": labels},
            link={
                "source": [idx[v] for v in df["location"]],
                "target": [idx[v] for v in df["room"]],
                "value": [int(v) for v in df["count"]],
            },
        )
    )
    fig.update_layout(
        title="Frequency of Location/Room Combinations",
        width=1000,
        height=550,
    )
    return fig

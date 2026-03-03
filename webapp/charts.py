import pandas as pd
import plotly.express as px
import plotly.graph_objs as go


def partner_orgasms_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if df.empty:
        fig.update_layout(title="Partner Orgasms Over Time (no results)")
        return fig

    fig.add_trace(
        go.Scatter(x=df["date"], y=df["total_org_partner"], mode="lines+markers", name="Daily")
    )
    fig.add_trace(
        go.Scatter(x=df["date"], y=df["trend"], mode="lines", line={"dash": "dash"}, name="30-day trend")
    )
    fig.update_layout(
        title="Partner Orgasms Over Time",
        xaxis_title="Date",
        yaxis_title="Total Partner Orgasms",
        autosize=True,
        height=500,
        margin={"l": 40, "r": 20, "t": 60, "b": 40},
    )
    fig.update_xaxes(rangeslider={"visible": True})
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
    fig.update_layout(autosize=True, height=400, margin={"l": 40, "r": 20, "t": 60, "b": 40})
    return fig


def sex_streaks_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if df.empty:
        fig.update_layout(title="Sex Streaks Over Time (no results)")
        return fig

    chart_df = df.copy()
    chart_df["start_date"] = pd.to_datetime(chart_df["start_date"])

    shown: set[str] = set()
    for _, row in chart_df.iterrows():
        name = "Sex streak" if row["type"] == "sex" else "No-sex streak"
        show_legend = name not in shown
        shown.add(name)

        width_days = max(int(row["length"]), 1)
        bar_center = row["start_date"] + pd.Timedelta(days=width_days / 2)
        end_date = row["start_date"] + pd.Timedelta(days=width_days)

        fig.add_trace(
            go.Bar(
                x=[bar_center],
                y=[row["signed_length"]],
                base=0,
                width=[width_days * 86400000],
                name=name,
                marker_color="royalblue" if row["type"] == "sex" else "firebrick",
                hovertemplate=(
                    f"Start: {row['start_date'].date()}<br>"
                    f"End: {(end_date - pd.Timedelta(days=1)).date()}<br>"
                    f"Length: {width_days} day(s)<extra></extra>"
                ),
                showlegend=show_legend,
            )
        )

    max_abs = int(df["length"].max()) if not df.empty else 1
    fig.update_layout(
        title="Sex Streaks Over Time",
        xaxis_title="Date",
        yaxis_title="Streak Length (days)",
        barmode="overlay",
        bargap=0,
        autosize=True,
        height=500,
        margin={"l": 40, "r": 20, "t": 60, "b": 40},
    )
    fig.update_xaxes(rangeslider={"visible": True})
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
    fig.update_layout(autosize=True, height=500, margin={"l": 40, "r": 20, "t": 60, "b": 60})
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
    fig.update_layout(autosize=True, height=500, margin={"l": 40, "r": 20, "t": 60, "b": 80})
    return fig


def position_upset_chart(
    df: pd.DataFrame,
    start_date: str | None = None,
    end_date: str | None = None,
) -> go.Figure:
    if df.empty:
        fig = go.Figure()
        fig.update_layout(title="Position Combination UpSet View (no results)")
        return fig

    try:
        from plotly_upset.plotting import plot_upset
    except ModuleNotFoundError:
        fig = go.Figure()
        fig.update_layout(title="Position Combination UpSet View (plotly_upset not installed)")
        return fig

    fig = plot_upset(
        dataframes=[df],
        legendgroups=[""],
        exclude_zeros=True,
        sorted_x="d",
        row_heights=[0.4, 0.6],
        column_widths=[0.1, 0.9],
        vertical_spacing=0.0,
        horizontal_spacing=0.15,
        marker_size=14,
    )

    fig.update_layout(yaxis4={"tickfont": {"size": 12}})

    subtitle = None
    if start_date and end_date:
        subtitle = f"Data from {start_date} to {end_date}"

    title_text = "Frequency of Sex Position Combinations"
    if subtitle:
        title_text += f"<br><sub>{subtitle}</sub>"

    fig.update_layout(
        title={"text": title_text, "y": 0.95, "x": 0.5, "xanchor": "center"},
        autosize=True,
        height=500,
        margin={"l": 40, "r": 20, "t": 60, "b": 40},
    )
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
        autosize=True,
        height=550,
        margin={"l": 20, "r": 20, "t": 60, "b": 20},
    )
    return fig

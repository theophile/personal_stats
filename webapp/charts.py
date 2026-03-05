import pandas as pd
import plotly.express as px
import plotly.graph_objs as go


def _title_with_subtitle(title: str, subtitle: str | None = None):
    if not subtitle:
        return title
    return {
        "text": title,
        "subtitle": {
            "text": subtitle,
            "font": {"color": "gray", "size": 13},
        },
    }


def _add_milestones(fig: go.Figure, milestones: list[tuple[str, str]] | None = None) -> None:
    if not milestones:
        return

    for date, label in milestones:
        try:
            x_value = pd.to_datetime(date).to_pydatetime()
        except (TypeError, ValueError):
            continue

        fig.add_shape(
            type="line",
            x0=x_value,
            x1=x_value,
            xref="x",
            y0=0,
            y1=1,
            yref="paper",
            line={"width": 2, "dash": "dot", "color": "darkgreen"},
        )
        fig.add_annotation(
            x=x_value,
            y=1,
            xref="x",
            yref="paper",
            text=label,
            showarrow=False,
            xanchor="left",
            yanchor="bottom",
            font={"size": 11, "color": "darkgreen"},
            bgcolor="rgba(255,255,255,0.75)",
        )


def partner_orgasms_chart(
    df: pd.DataFrame,
    milestones: list[tuple[str, str]] | None = None,
    include_trend: bool = True,
    title: str = "Orgasms Over Time",
    subtitle: str | None = None,
) -> go.Figure:
    fig = go.Figure()
    if df.empty:
        fig.update_layout(title=_title_with_subtitle(f"{title} (no results)", subtitle))
        _add_milestones(fig, milestones)
        return fig

    people = df["person"].unique().tolist() if "person" in df.columns else ["Daily"]
    for person in people:
        series = df[df["person"] == person] if "person" in df.columns else df
        y_col = "orgasms" if "orgasms" in series.columns else "total_org_partner"
        fig.add_trace(
            go.Scatter(
                x=series["date"],
                y=series[y_col],
                mode="lines+markers",
                name=f"{person} orgasms per session",
            )
        )
        if include_trend and "trend" in series.columns:
            fig.add_trace(
                go.Scatter(
                    x=series["date"],
                    y=series["trend"],
                    mode="lines",
                    line={"dash": "dash"},
                    name=f"{person} 30-day rolling average",
                )
            )
    fig.update_layout(
        title=_title_with_subtitle(title, subtitle),
        xaxis_title="Date",
        yaxis_title="Orgasms",
        legend_title_text="Series",
        autosize=True,
        height=500,
        margin={"l": 40, "r": 20, "t": 60, "b": 40},
    )
    fig.update_xaxes(rangeslider={"visible": True})
    _add_milestones(fig, milestones)
    return fig


def rating_histogram_chart(df: pd.DataFrame, subtitle: str | None = None) -> go.Figure:
    if df.empty:
        fig = go.Figure()
        fig.update_layout(title=_title_with_subtitle("Rating Distribution (no results)", subtitle))
        return fig

    fig = px.histogram(
        df,
        x="rating",
        nbins=5,
        title="Rating Distribution",
        labels={"rating": "Rating", "count": "Entries"},
    )
    fig.update_layout(
        title=_title_with_subtitle("Rating Distribution", subtitle),
        autosize=True,
        height=400,
        margin={"l": 40, "r": 20, "t": 60, "b": 40},
    )
    return fig


def sex_streaks_chart(
    df: pd.DataFrame,
    milestones: list[tuple[str, str]] | None = None,
    subtitle: str | None = None,
) -> go.Figure:
    fig = go.Figure()
    if df.empty:
        fig.update_layout(title=_title_with_subtitle("Sex Streaks Over Time (no results)", subtitle))
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
        bar_center_iso = bar_center.to_pydatetime().isoformat()
        end_date = row["start_date"] + pd.Timedelta(days=width_days)

        fig.add_trace(
            go.Bar(
                x=[bar_center_iso],
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
        title=_title_with_subtitle("Sex Streaks Over Time", subtitle),
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
    _add_milestones(fig, milestones)
    return fig


def duration_violin_chart(df: pd.DataFrame, subtitle: str | None = None) -> go.Figure:
    fig = go.Figure()
    if df.empty:
        fig.update_layout(title=_title_with_subtitle("Duration Distribution by Person (no results)", subtitle))
        return fig

    fig = px.violin(
        df,
        x="partner",
        y="duration",
        box=True,
        points="all",
        title="Duration Distribution by Person",
        labels={"partner": "Person", "duration": "Duration (minutes)"},
    )
    fig.update_layout(
        title=_title_with_subtitle("Duration Distribution by Person", subtitle),
        autosize=True,
        height=500,
        margin={"l": 40, "r": 20, "t": 60, "b": 80},
    )
    return fig


def rolling_anomaly_chart(
    df: pd.DataFrame,
    milestones: list[tuple[str, str]] | None = None,
    subtitle: str | None = None,
) -> go.Figure:
    fig = go.Figure()
    if df.empty:
        fig.update_layout(title=_title_with_subtitle("Orgasm Anomaly Detection (no results)", subtitle))
        _add_milestones(fig, milestones)
        return fig

    fig.add_trace(go.Scatter(x=df["date"], y=df["value"], mode="lines+markers", name="Daily orgasms per session"))
    fig.add_trace(go.Scatter(x=df["date"], y=df["baseline"], mode="lines", line={"dash": "dash"}, name="30-day rolling baseline"))

    anomalies = df[df["is_anomaly"] == 1]
    if not anomalies.empty:
        fig.add_trace(
            go.Scatter(
                x=anomalies["date"],
                y=anomalies["value"],
                mode="markers",
                marker={"color": "crimson", "size": 10, "symbol": "x"},
                name="Anomaly points",
                hovertemplate="Date: %{x}<br>Value: %{y}<br>Z-score: %{customdata:.2f}<extra></extra>",
                customdata=anomalies["zscore"],
            )
        )

    fig.update_layout(
        title=_title_with_subtitle("Orgasm Anomaly Detection", subtitle),
        xaxis_title="Date",
        yaxis_title="Daily orgasms",
        legend_title_text="Series",
        autosize=True,
        height=500,
        margin={"l": 40, "r": 20, "t": 60, "b": 40},
    )
    fig.update_xaxes(rangeslider={"visible": True})
    _add_milestones(fig, milestones)
    return fig


def position_association_chart(df: pd.DataFrame, subtitle: str | None = None) -> go.Figure:
    fig = go.Figure()
    if df.empty:
        fig.update_layout(title=_title_with_subtitle("Position Association Rules (no results)", subtitle))
        return fig

    chart_df = df.head(20).copy()
    chart_df["rule"] = chart_df["antecedent"] + " → " + chart_df["consequent"]
    fig = px.bar(
        chart_df,
        x="rule",
        y="lift",
        color="confidence",
        title="Position Association Rules (Top 20 by lift)",
        labels={"rule": "Rule", "lift": "Lift", "confidence": "Confidence"},
        hover_data=["support", "count"],
    )
    fig.update_layout(
        title=_title_with_subtitle("Position Association Rules (Top 20 by lift)", subtitle),
        autosize=True,
        height=500,
        margin={"l": 40, "r": 20, "t": 60, "b": 100},
        legend_title_text="Confidence",
    )
    return fig


def position_frequency_chart(df: pd.DataFrame, subtitle: str | None = None) -> go.Figure:
    fig = go.Figure()
    if df.empty:
        fig.update_layout(title=_title_with_subtitle("Frequency of Sex Positions (no results)", subtitle))
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
    fig.update_layout(
        title=_title_with_subtitle("Frequency of Sex Positions", subtitle),
        autosize=True,
        height=500,
        margin={"l": 40, "r": 20, "t": 60, "b": 60},
    )
    return fig


def position_combinations_chart(df: pd.DataFrame, subtitle: str | None = None) -> go.Figure:
    fig = go.Figure()
    if df.empty:
        fig.update_layout(title=_title_with_subtitle("Position Combination Frequency (no results)", subtitle))
        return fig

    chart_df = df.sort_values("count", ascending=False).head(15)
    fig = px.bar(
        chart_df,
        x="combination",
        y="count",
        title="Position Combination Frequency (Top 15)",
        labels={"combination": "Combination", "count": "Count"},
    )
    fig.update_layout(
        title=_title_with_subtitle("Position Combination Frequency (Top 15)", subtitle),
        autosize=True,
        height=500,
        margin={"l": 40, "r": 20, "t": 60, "b": 80},
    )
    return fig


def position_upset_chart(
    df: pd.DataFrame,
    start_date: str | None = None,
    end_date: str | None = None,
    subtitle: str | None = None,
) -> go.Figure:
    if df.empty:
        fig = go.Figure()
        fig.update_layout(title=_title_with_subtitle("Position Combination UpSet View (no results)", subtitle))
        return fig

    try:
        from plotly_upset.plotting import plot_upset
    except ModuleNotFoundError:
        fig = go.Figure()
        fig.update_layout(title=_title_with_subtitle("Position Combination UpSet View (plotly_upset not installed)", subtitle))
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

    date_subtitle = None
    if start_date and end_date:
        date_subtitle = f"Data from {start_date} to {end_date}"

    final_subtitle = subtitle or date_subtitle

    title_text = "Frequency of Sex Position Combinations"
    fig.update_layout(
        title=_title_with_subtitle(title_text, final_subtitle),
        autosize=True,
        height=500,
        margin={"l": 40, "r": 20, "t": 60, "b": 40},
    )
    return fig


def location_room_sankey_chart(df: pd.DataFrame, subtitle: str | None = None) -> go.Figure:
    fig = go.Figure()
    if df.empty:
        fig.update_layout(title=_title_with_subtitle("Frequency of Location/Room Combinations (no results)", subtitle))
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
        title=_title_with_subtitle("Frequency of Location/Room Combinations", subtitle),
        autosize=True,
        height=550,
        margin={"l": 20, "r": 20, "t": 60, "b": 20},
    )
    return fig

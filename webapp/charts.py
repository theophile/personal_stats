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

    # Labels are rotated 90° and anchored just inside the top of their vertical
    # line, so each label is unambiguously paired with its own line and cannot
    # collide with siblings or the title/subtitle block above.
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
            y=0.98,
            xref="x",
            yref="paper",
            text=label,
            showarrow=False,
            xanchor="right",
            yanchor="top",
            textangle=-90,
            font={"size": 11, "color": "darkgreen"},
            bgcolor="rgba(255,255,255,0.75)",
        )


def partner_orgasms_chart(
    df: pd.DataFrame,
    milestones: list[tuple[str, str]] | None = None,
    include_trend: bool = True,
    title: str = "Orgasms Over Time",
    subtitle: str | None = None,
    normalize_year: bool = False,
    trend_kind: str = "rolling_30",
) -> go.Figure:
    fig = go.Figure()
    if df.empty:
        fig.update_layout(title=_title_with_subtitle(f"{title} (no results)", subtitle))
        if not normalize_year:
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
            trend_label = "LOESS" if trend_kind == "loess" else "30-day rolling mean"
            fig.add_trace(
                go.Scatter(
                    x=series["date"],
                    y=series["trend"],
                    mode="lines",
                    line={"dash": "dash"},
                    name=f"{person} {trend_label}",
                )
            )
    fig.update_layout(
        title=_title_with_subtitle(title, subtitle),
        xaxis_title="Date" if not normalize_year else "Month / Day",
        yaxis_title="Orgasms",
        legend_title_text="Series",
        autosize=True,
        height=500,
        margin={"l": 40, "r": 20, "t": 60, "b": 40},
    )
    if normalize_year:
        fig.update_xaxes(tickformat="%b %d")
    else:
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

def year_in_review_chart(
    stats: dict,
    subtitle: str | None = None,
    custom_title: str | None = None,
    rename_map: dict[str, str] | None = None,
) -> go.Figure:
    """Render a Rendezvous Report stats card as a Plotly figure.

    Parameters
    ----------
    rename_map : real_name -> display_name mapping for pseudonymisation.
                 Applied to every place a person name appears.
    """
    fig = go.Figure()

    if not stats:
        fig.update_layout(title=custom_title or "Rendezvous Report (no data)")
        return fig

    # ── pseudonym helper ──────────────────────────────────────────────────────
    _rn = rename_map or {}
    def display(name: str) -> str:
        return _rn.get(name, name)

    def remap_dict(d: dict) -> dict:
        """Re-key a {real_name: value} dict using display names."""
        return {display(k): v for k, v in d.items()}

    # ── duration formatter ────────────────────────────────────────────────────
    def fmt_dur(minutes: int) -> str:
        m = int(minutes)
        if m <= 0:
            return "—"
        d, rem = divmod(m, 1440)
        h, mn  = divmod(rem, 60)
        parts  = []
        if d:  parts.append(f"{d}d")
        if h:  parts.append(f"{h}h")
        if mn or not parts: parts.append(f"{mn}m")
        return " ".join(parts)

    # ── pull stats ────────────────────────────────────────────────────────────
    n           = stats.get("n_sessions", 0)
    spw         = stats.get("sessions_per_week", 0)
    total_min   = stats.get("total_minutes", 0)
    avg_min     = stats.get("avg_minutes", 0)

    top_pos     = stats.get("top_position") or "—"
    top_pos_ct  = stats.get("top_position_count", 0)
    n_pos       = stats.get("n_distinct_positions", 0)
    n_combos    = stats.get("n_distinct_combos", 0)

    top_place   = stats.get("top_place") or "—"
    top_place_ct= stats.get("top_place_count", 0)

    sex_streak  = stats.get("longest_sex_streak", 0)
    rest_streak = stats.get("longest_no_sex_streak", 0)

    top_dow     = stats.get("top_day_of_week") or "—"
    top_dow_ct  = stats.get("top_day_of_week_count", 0)
    top_month   = stats.get("top_month") or "—"
    top_month_ct= stats.get("top_month_count", 0)
    least_month = stats.get("least_month") or "—"
    least_month_ct = stats.get("least_month_count", 0)

    date_min    = stats.get("date_min") or "—"
    date_max    = stats.get("date_max") or "—"

    # Apply pseudonyms to all person-keyed orgasm dicts
    by_total    = remap_dict(stats.get("orgasms_by_person_total") or {})
    by_avg      = remap_dict(stats.get("orgasms_by_person_avg") or {})
    by_max      = remap_dict(stats.get("orgasms_by_person_max") or {})
    people      = sorted(by_total.keys())

    # ── colour palette ────────────────────────────────────────────────────────
    BG       = "#0f172a"   # deep navy
    ACCENT   = "#f59e0b"   # amber
    TEXT     = "#f1f5f9"   # near-white
    MUTED    = "#94a3b8"   # slate-400
    DIVIDER  = "#334155"   # slate-700
    TBL_BG   = "#134e4a"   # teal-900  (sub-table bg band)
    TBL_HEAD = "#2dd4bf"   # teal-400  (sub-table headings)
    TBL_VAL  = "#f0fdfa"   # teal-50   (sub-table values)
    FONT     = "Inter, system-ui, sans-serif"

    # ── layout constants ──────────────────────────────────────────────────────
    COL = [0.10, 0.35, 0.60]     # x-centres of the 3 stat columns
    TABLE_X = 0.775              # left edge of table column (paper coords)
    TABLE_W = 0.225              # width of table column
    ROWS = [0.820, 0.620, 0.420, 0.220]  # tile row y-centres

    # ── tile definitions: (col, row, emoji, value, label) ────────────────────
    tiles_final = [
        # col 0 — activity
        (0, 0, "🗓️",  str(n),              "sessions"),
        (0, 1, "📈",  f"{spw}×",           "avg sessions per week"),
        (0, 2, "⏱️",  fmt_dur(total_min),  "total time"),
        (0, 3, "⚡",  fmt_dur(avg_min),    "avg duration"),
        # col 1 — patterns
        (1, 0, "🔥",  f"{sex_streak}d",    "longest active streak"),
        (1, 1, "😮‍💨", f"{rest_streak}d", "longest break"),
        (1, 2, "☀️",  top_month,            f"most active month  ({top_month_ct}×)"),
        (1, 3, "🌙",  least_month,          f"quietest month  ({least_month_ct}×)"),
        # col 2 — positions & places
        (2, 0, "🎯",  str(n_pos),          "positions tried"),
        (2, 1, "🔀",  str(n_combos),       "unique combos"),
        (2, 2, "🏆",  top_pos,              f"top position  ({top_pos_ct}×)"),
        (2, 3, "📍",  top_place,            f"favorite place  ({top_place_ct}×)"),
    ]

    # ── tile annotations: emoji left of value, label below ───────────────────
    for (ci, ri, icon, val, label) in tiles_final:
        y = ROWS[ri]
        x = COL[ci]
        # emoji — sits to the left of the value, vertically centred on value+label midpoint
        fig.add_annotation(
            x=x - 0.055, y=y + 0.012,
            xref="paper", yref="paper",
            text=icon,
            showarrow=False, xanchor="center", yanchor="middle",
            font={"size": 20, "family": FONT},
        )
        # value
        fig.add_annotation(
            x=x + 0.020, y=y + 0.055,
            xref="paper", yref="paper",
            text=f"<b>{val}</b>",
            showarrow=False, xanchor="center", yanchor="middle",
            font={"size": 18, "color": ACCENT, "family": FONT},
        )
        # label
        fig.add_annotation(
            x=x + 0.020, y=y - 0.030,
            xref="paper", yref="paper",
            text=label,
            showarrow=False, xanchor="center", yanchor="middle",
            font={"size": 9, "color": MUTED, "family": FONT},
        )

    # ── per-person sub-table (4th column) ────────────────────────────────────
    if people:
        # "Orgasm Stats" label sits ABOVE the teal background, in the header zone
        fig.add_annotation(
            x=TABLE_X + TABLE_W / 2, y=0.975,
            xref="paper", yref="paper",
            text="<b>Orgasm Stats</b>",
            showarrow=False, xanchor="center", yanchor="middle",
            font={"size": 12, "color": TBL_HEAD, "family": FONT},
        )

        # Teal background rectangle — starts below the label
        fig.add_shape(
            type="rect",
            xref="paper", yref="paper",
            x0=TABLE_X - 0.01, x1=1.01,
            y0=0.10, y1=0.945,
            fillcolor=TBL_BG,
            line={"width": 0},
            opacity=0.45,
            layer="below",
        )

        row_labels = ["total", "avg / session", "peak"]
        row_y      = [0.76, 0.53, 0.30]

        n_people  = len(people)
        label_frac = 0.30
        data_frac  = (1.0 - label_frac) / n_people if n_people else 1.0
        label_x    = TABLE_X + TABLE_W * label_frac * 0.5
        person_xs  = [
            TABLE_X + TABLE_W * (label_frac + data_frac * (i + 0.5))
            for i in range(n_people)
        ]

        # Row header labels
        for lbl, y in zip(row_labels, row_y):
            fig.add_annotation(
                x=label_x, y=y,
                xref="paper", yref="paper",
                text=lbl,
                showarrow=False, xanchor="center", yanchor="middle",
                font={"size": 9, "color": MUTED, "family": FONT},
            )

        # Person name column headers (inside teal band, but top of it)
        for px, name in zip(person_xs, people):
            display_name = name if len(name) <= 10 else name[:9] + "…"
            fig.add_annotation(
                x=px, y=0.905,
                xref="paper", yref="paper",
                text=f"<b>{display_name}</b>",
                showarrow=False, xanchor="center", yanchor="middle",
                font={"size": 11, "color": TBL_HEAD, "family": FONT},
            )

        # Data cells
        for px, name in zip(person_xs, people):
            for val, y in zip(
                [str(by_total.get(name, 0)), str(by_avg.get(name, 0)), str(by_max.get(name, 0))],
                row_y,
            ):
                fig.add_annotation(
                    x=px, y=y,
                    xref="paper", yref="paper",
                    text=f"<b>{val}</b>",
                    showarrow=False, xanchor="center", yanchor="middle",
                    font={"size": 16, "color": TBL_VAL, "family": FONT},
                )

        # Horizontal dividers inside teal section
        for y_line in [0.855, 0.645, 0.420, 0.175]:
            fig.add_shape(
                type="line",
                xref="paper", yref="paper",
                x0=TABLE_X - 0.005, x1=1.005,
                y0=y_line, y1=y_line,
                line={"color": "#0f4f4a", "width": 1},
            )

    # ── vertical dividers between stat columns ────────────────────────────────
    divider_xs = [0.225, 0.475, TABLE_X - 0.015]
    for x_div in divider_xs:
        fig.add_shape(
            type="line", xref="paper", yref="paper",
            x0=x_div, x1=x_div, y0=0.10, y1=0.97,
            line={"color": DIVIDER, "width": 1},
        )

    # ── title ─────────────────────────────────────────────────────────────────
    report_title = custom_title or "✦ Rendezvous Report ✦"
    date_range   = f"{date_min}  –  {date_max}"
    title_text   = (
        f"<b>{report_title}</b>"
        f"<br><span style='font-size:12px; color:{MUTED}'>{date_range}</span>"
        + (f"<br><span style='font-size:11px; color:{MUTED}'>{subtitle}</span>" if subtitle else "")
    )

    fig.update_layout(
        title={
            "text": title_text,
            "x": 0.5, "xanchor": "center",
            "font": {"size": 20, "color": TEXT, "family": FONT},
        },
        paper_bgcolor=BG,
        plot_bgcolor=BG,
        xaxis={"visible": False, "range": [0, 1]},
        yaxis={"visible": False, "range": [0, 1]},
        height=560,
        margin={"l": 20, "r": 20, "t": 110, "b": 20},
    )
    return fig

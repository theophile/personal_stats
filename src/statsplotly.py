import pandas as pd
import numpy as np
import plotly.graph_objs as go
import plotly.express as px
import networkx as nx
from plotly.subplots import make_subplots
from plotly_upset.plotting import plot_upset
from collections import Counter
from src.intimassy2 import ASCDatabase, Entry

# --- Style and Layout Configuration ---
CHART_LAYOUT = {
    "width": 1000,
    "height": 600,
    "margin": dict(l=40, r=40, t=80, b=40),
    "legend": dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
}


def apply_chart_style(fig, title=None, x_axis_title=None, y_axis_title=None):
    """Applies a consistent style to a Plotly figure."""

    fig.update_layout(
        title=dict(text=title, x=0.5, y=0.95), **CHART_LAYOUT  # Center title
    )

    if x_axis_title:
        fig.update_xaxes(title_text=x_axis_title)
    if y_axis_title:
        fig.update_yaxes(title_text=y_axis_title)
    return fig


def add_marker_dates(
    fig,
    marker_dates,
    y_min,
    y_max,
    label_offset=1.5,
    line_color="green",
    label_font_size=10,
):
    """Adds vertical marker lines with labels."""
    fig.update_yaxes(range=[y_min, y_max + label_offset + 2])

    for marker_date in marker_dates:
        marker_datetime = pd.to_datetime(marker_date)
        fig.add_shape(
            type="line",
            x0=marker_datetime,
            x1=marker_datetime,
            y0=y_min,
            y1=y_max,
            line=dict(color=line_color, width=2, dash="dot"),
            xref="x",
            yref="y",
        )
        fig.add_annotation(
            x=marker_datetime,
            y=y_max + label_offset,
            text=marker_datetime.strftime("%m/%d/%y"),
            showarrow=False,
            font=dict(size=label_font_size, color=line_color),
            xanchor="center",
            yanchor="bottom",
            xref="x",
            yref="y",
        )


# --- Data Processing Functions ---


def count_positions_and_combinations(data, db):
    """Counts position occurrences and combinations."""
    position_combinations = []
    position_counter = Counter()
    for entry in data:
        entry_obj = Entry(db, entry[0])
        positions = [6 if pos == 7 else pos for pos in entry_obj.position_ids]
        position_combinations.append(
            tuple(sorted(positions))
        )  # Sorted tuples for consistency
        position_counter.update(positions)
    return position_counter, position_combinations


def get_date_range(df, date_column="date"):
    first_date = df[date_column].min()
    last_date = df[date_column].max()
    return first_date, last_date


def generate_date_ticks(start_date, end_date, num_ticks=10):
    """Generates date ticks for the x-axis."""
    return pd.date_range(start=start_date, end=end_date, periods=num_ticks)


def generate_numeric_ticks(min_val, max_val, num_ticks=6):
    """Generates numeric ticks for the y-axis."""
    return np.linspace(min_val, max_val, num=num_ticks).tolist()


# --- Plotting Functions ---


def create_line_chart(
    df,
    x_col,
    y_col,
    trend_col=None,
    title=None,
    x_axis_title=None,
    y_axis_title=None,
    marker_dates=None,
    output_file=None,
):
    """Creates a line chart with optional trend line and marker dates."""
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df[x_col],
            y=df[y_col],
            mode="lines+markers",
            name=y_axis_title,
            line=dict(color="royalblue"),
        )
    )
    if trend_col:
        fig.add_trace(
            go.Scatter(
                x=df[x_col],
                y=df[trend_col],
                mode="lines",
                name="Trend",
                line=dict(color="firebrick", dash="dash"),
            )
        )

    fig = apply_chart_style(fig, title, x_axis_title, y_axis_title)

    if marker_dates:
        add_marker_dates(
            fig, marker_dates, df[y_col].min() - 1, df[y_col].max() + 1, label_offset=2
        )

    if output_file:
        save_and_show_plot(fig, output_file)
    return fig


def create_bar_chart(
    df, x_col, y_col, title=None, x_axis_title=None, y_axis_title=None, output_file=None
):
    """Creates a horizontal bar chart."""
    fig = px.bar(df, x=x_col, y=y_col, orientation="h", text_auto=True)
    fig.update_traces(
        textfont_size=12, textangle=0, textposition="outside", cliponaxis=False
    )
    fig.update_layout(
        yaxis=dict(tickfont=dict(family="Noto Color Emoji", size=12))
    )  # Emoji font for y-axis ticks

    fig = apply_chart_style(fig, title, x_axis_title, y_axis_title)

    if output_file:
        save_and_show_plot(fig, output_file)
    return fig


def create_streak_chart(
    streaks_df, title=None, x_axis_title=None, y_axis_title=None, output_file=None
):
    """Creates a streak chart."""
    fig = go.Figure()
    legend_entries = []
    for _, row in streaks_df.iterrows():
        name = "Sex Streak" if row["length"] > 0 else "No-Sex Streak"
        fig.add_bar(
            x=[row["start_date"]],
            y=[row["length"]],
            marker_color=row["color"],
            width=86400000 * abs(row["length"]),  # Width in milliseconds for 1 day
            offset=0,
            name=name,
            showlegend=name not in legend_entries,
        )
        legend_entries.append(name)

    first_date, last_date = get_date_range(streaks_df, "start_date")
    max_length = max(abs(streaks_df["length"]))

    fig = apply_chart_style(fig, title, x_axis_title, y_axis_title)

    fig.update_layout(
        xaxis_title=x_axis_title,
        yaxis_title=y_axis_title,
        xaxis=dict(
            tickvals=generate_date_ticks(first_date, last_date),
            tickformat="%m/%d/%y",
            range=[first_date, last_date],
        ),
        yaxis=dict(
            title="Streak Length (in days)",
            tickvals=list(range(-max_length, max_length + 1)),
            ticktext=[str(abs(x)) for x in range(-max_length, max_length + 1)],
        ),
    )

    if output_file:
        save_and_show_plot(fig, output_file)
    return fig


def save_and_show_plot(fig, output_file):
    """Saves and shows the plot."""
    fig.write_image(output_file, format="png", engine="kaleido")
    fig.show()


# --- Chart-Specific Functions (Refactored) ---


def track_partner_orgasms_plotly(
    db,
    start_date,
    end_date,
    marker_dates=None,
    output_file="partner_orgasms_plotly.png",
):
    query = """SELECT date, total_org_partner FROM entries WHERE date BETWEEN ? AND ?"""
    df = pd.read_sql_query(query, db.conn, params=(start_date, end_date))
    df["date"] = pd.to_datetime(df["date"], format="%Y.%m.%d")

    daily_orgasms = df.groupby("date", as_index=False)["total_org_partner"].sum()

    if daily_orgasms.empty:
        print("No data available for the specified date range.")
        return

    daily_orgasms["trend"] = (
        daily_orgasms["total_org_partner"].rolling(window=30, min_periods=1).mean()
    )

    first_date, last_date = get_date_range(daily_orgasms)
    min_orgasms, max_orgasms = (
        daily_orgasms["total_org_partner"].min(),
        daily_orgasms["total_org_partner"].max(),
    )

    fig = create_line_chart(
        daily_orgasms,
        "date",
        "total_org_partner",
        trend_col="trend",
        title="Partner Orgasms Over Time",
        x_axis_title="Date",
        y_axis_title="Total Orgasms",
        marker_dates=marker_dates,
        output_file=output_file,
    )
    fig.update_xaxes(
        tickvals=generate_date_ticks(first_date, last_date), tickformat="%m/%d/%y"
    )
    fig.update_yaxes(
        tickvals=generate_numeric_ticks(min_orgasms - 1.5, max_orgasms + 2)
    )

    return fig  # Return the figure object


def track_sex_streaks_plotly(
    db, start_date, end_date, output_file="streaks_plotly.png"
):
    query = """SELECT date FROM entries WHERE date BETWEEN ? AND ? ORDER BY date"""
    df = pd.read_sql_query(
        query,
        db.conn,
        params=(start_date.replace("-", "."), end_date.replace("-", ".")),
    )
    df["date"] = pd.to_datetime(df["date"].str.replace(".", "-", regex=False))

    if df.empty:
        print("No data available within the specified date range.")
        return

    first_date, last_date = get_date_range(df)
    full_date_range = pd.date_range(start=first_date, end=last_date)
    streaks_df = pd.DataFrame(full_date_range, columns=["date"])
    streaks_df["sex_occurred"] = streaks_df["date"].isin(df["date"]).astype(int)

    streaks = []
    current_streak = {"type": None, "start": None, "length": 0}

    for _, row in streaks_df.iterrows():
        sex_occurred = row["sex_occurred"]
        date = row["date"]

        if current_streak["type"] is None:
            current_streak["type"] = sex_occurred
            current_streak["start"] = date
            current_streak["length"] = 1
        elif current_streak["type"] == sex_occurred:
            current_streak["length"] += 1
        else:
            streaks.append(current_streak)
            current_streak = {"type": sex_occurred, "start": date, "length": 1}

    if current_streak["type"] is not None:
        streaks.append(current_streak)

    plot_data = []
    for streak in streaks:
        color = "royalblue" if streak["type"] == 1 else "firebrick"
        end_date = streak["start"] + pd.Timedelta(days=streak["length"])
        length = streak["length"] if streak["type"] == 1 else -streak["length"]
        plot_data.append(
            {
                "start_date": streak["start"],
                "end_date": end_date,
                "length": length,
                "color": color,
            }
        )

    plot_df = pd.DataFrame(plot_data)

    fig = create_streak_chart(
        plot_df,
        title="Sex Streaks Over Time",
        x_axis_title="Date",
        y_axis_title="Streak Length (in days)",
        output_file=output_file,
    )

    return fig


def sex_position_counter_bar_chart(
    db,
    start_date="2024.01.01",
    end_date="2024.12.31",
    output_file="sex_position_barchart.png",
):
    query = """SELECT * FROM entries WHERE date BETWEEN ? AND ?"""
    result = db.cursor.execute(query, (start_date, end_date)).fetchall()
    position_counter, _ = count_positions_and_combinations(result, db)

    position_names = [db.fetch_position_name(pos_id) for pos_id in position_counter]
    frequencies = list(position_counter.values())

    position_df = pd.DataFrame({"Position": position_names, "Frequency": frequencies})
    position_df = position_df.sort_values(by="Frequency", ascending=True).reset_index(
        drop=True
    )

    fig = create_bar_chart(
        position_df,
        x_col="Frequency",
        y_col="Position",
        title="Frequency of Sex Positions",
        output_file=output_file,
    )
    return fig


def sex_position_frequency_upset_plot(
    db, start_date="2024.01.01", end_date="2024.12.31", max_positions=6, min_instances=1
):
    query = """SELECT * FROM entries WHERE date BETWEEN ? AND ?"""
    result = db.cursor.execute(query, (start_date, end_date)).fetchall()
    position_counter, _ = count_positions_and_combinations(result, db)
    top_positions = [pos for pos, _ in position_counter.most_common(max_positions)]

    binary_matrix = []
    for entry in result:
        entry_obj = Entry(db, entry[0])
        positions = entry_obj.position_ids
        binary_row = tuple(1 if pos in positions else 0 for pos in top_positions)
        binary_matrix.append(binary_row)

    for i in range(len(top_positions)):
        top_positions[i] = db.fetch_position_name(top_positions[i])

    if min_instances > 1:
        counts = Counter(binary_matrix)
        binary_matrix = [
            item for item in binary_matrix if counts[item] >= min_instances
        ]

    df = pd.DataFrame(binary_matrix, columns=top_positions)
    df = df[df.sum().sort_values(ascending=True).keys()]

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

    fig.show()

    fig.update_layout(
        yaxis4=dict(tickfont=dict(family="Noto Color Emoji", size=16))
    )

    subtitle = f"Data from {start_date} to {end_date}"  # Format the subtitle
    fig.update_layout(
        title=dict(
            text=f"Frequency of Sex Position Combinations<br><sub>{subtitle}</sub>",
            y=0.95, # Adjust vertical position if necessary
            x=0.5, # center the title
            xanchor='center'
        )
    )

    fig.update_layout(
        width=1400,
        height=800,
    )

    save_and_show_plot(fig, "sex_position_frequency_upset_plot.png")  # Save and show

    return fig


def location_room_frequency_sankey_plot(
    db, start_date="2024.01.01", end_date="2024.12.31", max_locations=6
):
    query = """SELECT * FROM entries WHERE date BETWEEN ? AND ?"""
    result = db.cursor.execute(query, (start_date, end_date)).fetchall()
    location_room_counter = {}

    for entry in result:
        entry_obj = Entry(db, entry[0])
        place_names = entry_obj.places

        locations = []
        rooms = []

        for place in place_names:
            place_id = list(db.place_mapping.values()).index(place)
            if place_id <= 9:
                rooms.append(place)
            else:
                locations.append(place)

        if not locations or not rooms:
            missing_info = []
            if not locations:
                missing_info.append("location")
            if not rooms:
                missing_info.append("room")
            print(
                f"Warning: Entry on {entry_obj.date} is missing {', '.join(missing_info)}."
            )

        for location in locations:
            if location not in location_room_counter:
                location_room_counter[location] = {}
            for room in rooms:
                if room not in location_room_counter[location]:
                    location_room_counter[location][room] = 0
                location_room_counter[location][room] += 1

    labels = list(
        set(location_room_counter.keys()).union(*location_room_counter.values())
    )
    label_index = {label: i for i, label in enumerate(labels)}

    source = []
    target = []
    value = []

    label_counts = {label: 0 for label in labels}

    for location, rooms in location_room_counter.items():
        for room, count in rooms.items():
            source.append(label_index[location])
            target.append(label_index[room])
            value.append(count)
            label_counts[location] += count
            label_counts[room] += count

    labels = [f"{label} ({label_counts[label]})" for label in labels]

    fig = go.Figure(
        go.Sankey(
            node=dict(
                pad=15, thickness=20, line=dict(color="black", width=0.5), label=labels
            ),
            link=dict(source=source, target=target, value=value),
        )
    )

    fig = apply_chart_style(
        fig, title="Frequency of Location/Room Combinations"
    )  # Apply style
    fig.update_layout(
        font=dict(size=14), width=1000, height=600
    )  # Other layout updates

    save_and_show_plot(fig, "location_room_frequency_sankey_plot.png")  # Save and show

    return fig

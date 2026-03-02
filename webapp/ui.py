from __future__ import annotations

from nicegui import ui
from plotly.graph_objs import Figure
from webapp.charts import (
    location_room_sankey_chart,
    partner_orgasms_chart,
    position_combinations_chart,
    position_upset_chart,
    position_frequency_chart,
    rating_histogram_chart,
    sex_streaks_chart,
)
from webapp.config import DEFAULT_DB_PATH
from webapp.services import DataSourceError, SearchFilters, StatsService


class PersonalStatsApp:
    def __init__(self, service: StatsService):
        self.service = service
        self.table = None
        self.plot_container = None
        self.rating_plot_container = None
        self.streak_plot_container = None
        self.position_plot_container = None
        self.position_combo_plot_container = None
        self.location_room_plot_container = None
        self.position_upset_plot_container = None
        self.status_label = None
        self.entries_metric = None
        self.partner_metric = None
        self.my_metric = None

    def build(self) -> None:
        ui.label("Personal Stats Webapp").classes("text-2xl font-bold")
        ui.label("Read-only browser + interactive charting for immutable export DB")
        ui.label(f"Configured DB path: {DEFAULT_DB_PATH}").classes("text-sm text-gray-500")

        self.status_label = ui.label("").classes("text-red-600")

        with ui.row().classes("w-full gap-4"):
            self.entries_metric = ui.card().classes("p-4").tight()
            with self.entries_metric:
                ui.label("Entries")
                ui.label("0").classes("text-2xl")
            self.partner_metric = ui.card().classes("p-4").tight()
            with self.partner_metric:
                ui.label("Partner Orgasms")
                ui.label("0").classes("text-2xl")
            self.my_metric = ui.card().classes("p-4").tight()
            with self.my_metric:
                ui.label("My Orgasms")
                ui.label("0").classes("text-2xl")

        partner_choices = {"": "Any"}
        position_choices = {"": "Any"}
        place_choices = {"": "Any"}
        try:
            partner_choices |= {str(pid): name for pid, name in self.service.partner_options()}
            position_choices |= {str(pid): name for pid, name in self.service.position_options()}
            place_choices |= {str(pid): name for pid, name in self.service.place_options()}
        except DataSourceError as exc:
            self._set_status(str(exc))

        with ui.card().classes("w-full"):
            ui.label("Filters").classes("text-lg")
            with ui.row().classes("items-end"):
                start_date = ui.input("Start date (YYYY.MM.DD)", value="2024.01.01")
                end_date = ui.input("End date (YYYY.MM.DD)", value="2024.12.31")
                note_keyword = ui.input("Note keyword contains", placeholder="optional")
                partner = ui.select(partner_choices, label="Partner", value="")
                position = ui.select(position_choices, label="Position", value="")
                place = ui.select(place_choices, label="Place", value="")

                def current_filters() -> SearchFilters:
                    return SearchFilters(
                        start_date=start_date.value or None,
                        end_date=end_date.value or None,
                        note_keyword=note_keyword.value or None,
                        partner_id=int(partner.value) if partner.value else None,
                        position_id=int(position.value) if position.value else None,
                        place_id=int(place.value) if place.value else None,
                    )

                ui.button("Run Search", on_click=lambda: self.refresh_all(current_filters()))
                ui.button("Export Table CSV", on_click=lambda: self.export_csv(current_filters()))
                ui.button("Export Chart PNG", on_click=lambda: self.export_chart_png(current_filters()))
                ui.button("Export Report JSON", on_click=lambda: self.export_report_json(current_filters()))

        with ui.card().classes("w-full"):
            ui.label("Entries")
            self.table = ui.table(
                columns=[
                    {"name": "entry_id", "label": "Entry ID", "field": "entry_id"},
                    {"name": "date", "label": "Date", "field": "date"},
                    {"name": "duration", "label": "Duration", "field": "duration"},
                    {"name": "rating", "label": "Rating", "field": "rating"},
                    {"name": "total_org", "label": "My Orgasms", "field": "total_org"},
                    {"name": "total_org_partner", "label": "Partner Orgasms", "field": "total_org_partner"},
                    {"name": "partners", "label": "Partners", "field": "partners"},
                    {"name": "positions", "label": "Positions", "field": "positions"},
                    {"name": "places", "label": "Places", "field": "places"},
                ],
                rows=[],
                pagination=20,
            ).classes("w-full")

        with ui.card().classes("w-full"):
            ui.label("Chart: Partner Orgasms")
            self.plot_container = ui.column().classes("w-full")

        with ui.card().classes("w-full"):
            ui.label("Chart: Rating Distribution")
            self.rating_plot_container = ui.column().classes("w-full")

        with ui.card().classes("w-full"):
            ui.label("Chart: Sex Streaks")
            self.streak_plot_container = ui.column().classes("w-full")

        with ui.card().classes("w-full"):
            ui.label("Chart: Position Frequency")
            self.position_plot_container = ui.column().classes("w-full")

        with ui.card().classes("w-full"):
            ui.label("Chart: Position Combinations")
            self.position_combo_plot_container = ui.column().classes("w-full")

        with ui.card().classes("w-full"):
            ui.label("Chart: Position UpSet")
            self.position_upset_plot_container = ui.column().classes("w-full")

        with ui.card().classes("w-full"):
            ui.label("Chart: Location/Room Links")
            self.location_room_plot_container = ui.column().classes("w-full")

        default_filters = SearchFilters(start_date="2024.01.01", end_date="2024.12.31")
        self.refresh_all(default_filters)

    def _metric_value_label(self, card) -> ui.label:
        return card.default_slot.children[1]

    def _update_metrics(self, filters: SearchFilters) -> None:
        metrics = self.service.summary_metrics(filters)
        self._metric_value_label(self.entries_metric).set_text(str(metrics["entries"]))
        self._metric_value_label(self.partner_metric).set_text(str(metrics["total_partner_orgasms"]))
        self._metric_value_label(self.my_metric).set_text(str(metrics["total_my_orgasms"]))

    def _set_status(self, message: str) -> None:
        self.status_label.text = message
        self.status_label.update()

    def refresh_all(self, filters: SearchFilters) -> None:
        self.refresh_entries(filters)
        self.refresh_charts(filters)
        try:
            self._update_metrics(filters)
        except DataSourceError as exc:
            self._set_status(str(exc))

    def refresh_entries(self, filters: SearchFilters) -> None:
        try:
            rows = self.service.search_entries(filters)
            self.table.rows = rows
            self.table.update()
            self._set_status(f"Loaded {len(rows)} row(s).")
        except DataSourceError as exc:
            self.table.rows = []
            self.table.update()
            self._set_status(str(exc))

    def refresh_charts(self, filters: SearchFilters) -> None:
        self.refresh_partner_org_chart(filters)
        self.refresh_rating_chart(filters)
        self.refresh_streak_chart(filters)
        self.refresh_position_chart(filters)
        self.refresh_position_combinations_chart(filters)
        self.refresh_position_upset_chart(filters)
        self.refresh_location_room_chart(filters)

    def refresh_partner_org_chart(self, filters: SearchFilters) -> None:
        try:
            df = self.service.partner_orgasms_timeseries(filters)
            fig = partner_orgasms_chart(df)
        except DataSourceError as exc:
            fig = Figure()
            fig.update_layout(title="Partner Orgasms Over Time (data source error)")
            self._set_status(str(exc))

        self.plot_container.clear()
        with self.plot_container:
            ui.plotly(fig)

    def refresh_rating_chart(self, filters: SearchFilters) -> None:
        try:
            df = self.service.ratings_dataframe(filters)
            fig = rating_histogram_chart(df)
        except DataSourceError as exc:
            fig = Figure()
            fig.update_layout(title="Rating Distribution (data source error)")
            self._set_status(str(exc))

        self.rating_plot_container.clear()
        with self.rating_plot_container:
            ui.plotly(fig)


    def refresh_streak_chart(self, filters: SearchFilters) -> None:
        try:
            df = self.service.sex_streaks_dataframe(filters)
            fig = sex_streaks_chart(df)
        except DataSourceError as exc:
            fig = Figure()
            fig.update_layout(title="Sex Streaks Over Time (data source error)")
            self._set_status(str(exc))

        self.streak_plot_container.clear()
        with self.streak_plot_container:
            ui.plotly(fig)

    def refresh_position_chart(self, filters: SearchFilters) -> None:
        try:
            df = self.service.position_frequency_dataframe(filters)
            fig = position_frequency_chart(df)
        except DataSourceError as exc:
            fig = Figure()
            fig.update_layout(title="Frequency of Sex Positions (data source error)")
            self._set_status(str(exc))

        self.position_plot_container.clear()
        with self.position_plot_container:
            ui.plotly(fig)

    def refresh_position_combinations_chart(self, filters: SearchFilters) -> None:
        try:
            df = self.service.position_combinations_dataframe(filters)
            fig = position_combinations_chart(df)
        except DataSourceError as exc:
            fig = Figure()
            fig.update_layout(title="Position Combination Frequency (data source error)")
            self._set_status(str(exc))

        self.position_combo_plot_container.clear()
        with self.position_combo_plot_container:
            ui.plotly(fig)

    def refresh_position_upset_chart(self, filters: SearchFilters) -> None:
        try:
            df = self.service.position_upset_dataframe(filters)
            fig = position_upset_chart(df)
        except DataSourceError as exc:
            fig = Figure()
            fig.update_layout(title="Position Combination UpSet View (data source error)")
            self._set_status(str(exc))

        self.position_upset_plot_container.clear()
        with self.position_upset_plot_container:
            ui.plotly(fig)

    def refresh_location_room_chart(self, filters: SearchFilters) -> None:
        try:
            df = self.service.location_room_sankey_dataframe(filters)
            fig = location_room_sankey_chart(df)
        except DataSourceError as exc:
            fig = Figure()
            fig.update_layout(title="Frequency of Location/Room Combinations (data source error)")
            self._set_status(str(exc))

        self.location_room_plot_container.clear()
        with self.location_room_plot_container:
            ui.plotly(fig)

    def export_csv(self, filters: SearchFilters) -> None:
        try:
            csv_path = self.service.export_entries_csv(filters)
            ui.download(str(csv_path), filename="entries_export.csv")
            self._set_status(f"CSV export ready: {csv_path}")
        except DataSourceError as exc:
            self._set_status(str(exc))

    def export_report_json(self, filters: SearchFilters) -> None:
        try:
            report_path = self.service.export_report_json(filters)
            ui.download(str(report_path), filename="report_export.json")
            self._set_status(f"Report JSON export ready: {report_path}")
        except DataSourceError as exc:
            self._set_status(str(exc))

    def export_chart_png(self, filters: SearchFilters) -> None:
        try:
            df = self.service.partner_orgasms_timeseries(filters)
            fig = partner_orgasms_chart(df)
            png_path = self.service.temp_export_path("partner_orgasms_chart_", ".png")
            fig.write_image(str(png_path), format="png", engine="kaleido")
            ui.download(str(png_path), filename="partner_orgasms_chart.png")
            self._set_status(f"Chart PNG export ready: {png_path}")
        except DataSourceError as exc:
            self._set_status(str(exc))
        except Exception as exc:
            self._set_status(f"Chart export failed: {exc}")

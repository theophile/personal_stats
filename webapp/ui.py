from __future__ import annotations

from nicegui import ui

from webapp.charts import partner_orgasms_chart
from webapp.config import DEFAULT_DB_PATH
from webapp.services import DataSourceError, SearchFilters, StatsService


class PersonalStatsApp:
    def __init__(self, service: StatsService):
        self.service = service
        self.table = None
        self.plot_container = None
        self.status_label = None

    def build(self) -> None:
        ui.label("Personal Stats Webapp").classes("text-2xl font-bold")
        ui.label("Read-only browser + interactive charting for immutable export DB")
        ui.label(f"Configured DB path: {DEFAULT_DB_PATH}").classes("text-sm text-gray-500")

        self.status_label = ui.label("").classes("text-red-600")

        with ui.card().classes("w-full"):
            ui.label("Filters").classes("text-lg")
            with ui.row().classes("items-end"):
                start_date = ui.input("Start date (YYYY.MM.DD)", value="2024.01.01")
                end_date = ui.input("End date (YYYY.MM.DD)", value="2024.12.31")
                note_keyword = ui.input("Note keyword contains", placeholder="optional")

                def current_filters() -> SearchFilters:
                    return SearchFilters(
                        start_date=start_date.value or None,
                        end_date=end_date.value or None,
                        note_keyword=note_keyword.value or None,
                    )

                ui.button(
                    "Run Search",
                    on_click=lambda: self.refresh_entries(current_filters()),
                )
                ui.button(
                    "Update Chart",
                    on_click=lambda: self.refresh_chart(current_filters()),
                )

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

        default_filters = SearchFilters(start_date="2024.01.01", end_date="2024.12.31")
        self.refresh_entries(default_filters)
        self.refresh_chart(default_filters)

    def _set_status(self, message: str) -> None:
        self.status_label.text = message
        self.status_label.update()

    def refresh_entries(self, filters: SearchFilters) -> None:
        try:
            rows = self.service.search_entries(filters)
            self.table.rows = rows
            self.table.update()
            self._set_status("")
        except DataSourceError as exc:
            self.table.rows = []
            self.table.update()
            self._set_status(str(exc))

    def refresh_chart(self, filters: SearchFilters) -> None:
        try:
            df = self.service.partner_orgasms_timeseries(filters)
            fig = partner_orgasms_chart(df)
            self._set_status("")
        except DataSourceError as exc:
            from plotly.graph_objs import Figure

            fig = Figure()
            fig.update_layout(title="Partner Orgasms Over Time (data source error)")
            self._set_status(str(exc))

        self.plot_container.clear()
        with self.plot_container:
            ui.plotly(fig)

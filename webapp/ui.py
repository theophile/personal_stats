from __future__ import annotations

from datetime import datetime
from typing import Any

from nicegui import app, ui
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
        self.start_date = None
        self.end_date = None
        self.note_keyword = None
        self.partner = None
        self.position_ids = None
        self.place = None

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
        position_choices: dict[int, str] = {}
        place_choices = {"": "Any"}
        try:
            partner_choices |= {str(pid): name for pid, name in self.service.partner_options()}
            position_choices = {pid: name for pid, name in self.service.position_options()}
            place_choices |= {str(pid): name for pid, name in self.service.place_options()}
        except DataSourceError as exc:
            self._set_status(str(exc))

        with ui.card().classes("w-full"):
            ui.label("Filters").classes("text-lg")
            with ui.row().classes("w-full gap-3 items-start flex-wrap"):
                self.start_date = ui.date(value="2024-01-01", mask="YYYY-MM-DD").props("label='Start date'").classes("w-full md:w-[14rem]")
                self.end_date = ui.date(value="2024-12-31", mask="YYYY-MM-DD").props("label='End date'").classes("w-full md:w-[14rem]")
                with ui.column().classes("w-full md:flex-1 gap-2"):
                    self.note_keyword = ui.input("Note keyword contains", placeholder="optional").classes("w-full")
                    self.partner = ui.select(partner_choices, label="Partner", value="").classes("w-full")
                    self.position_ids = ui.select(
                        position_choices,
                        label="Positions",
                        value=[],
                        multiple=True,
                    ).props("use-chips clearable").classes("w-full")
                    self.place = ui.select(place_choices, label="Place", value="").classes("w-full")

            with ui.row().classes("w-full gap-2 flex-wrap"):
                ui.button("Run Search", on_click=lambda: self.run_search(save=True))
                ui.button("Reset Filters", on_click=self.reset_filters)
                ui.button("Export Table CSV", on_click=lambda: self.export_csv(self.current_filters()))
                ui.button("Export Chart PNG", on_click=lambda: self.export_chart_png(self.current_filters()))
                ui.button("Export Report JSON", on_click=lambda: self.export_report_json(self.current_filters()))

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
            self.table.on("rowClick", self.show_entry_dialog)

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

        initial_filters = self._filters_from_storage()
        self._apply_filters(initial_filters)
        self.refresh_all(initial_filters)


    @staticmethod
    def _to_db_date(value: str | None) -> str | None:
        if not value:
            return None
        try:
            return datetime.strptime(value, "%Y-%m-%d").strftime("%Y.%m.%d")
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_ui_date(value: str | None) -> str | None:
        if not value:
            return None
        try:
            return datetime.strptime(value, "%Y.%m.%d").strftime("%Y-%m-%d")
        except (TypeError, ValueError):
            return None

    def _default_filters(self) -> SearchFilters:
        return SearchFilters(start_date="2024.01.01", end_date="2024.12.31")

    def current_filters(self) -> SearchFilters:
        selected_positions = [int(v) for v in (self.position_ids.value or [])]
        return SearchFilters(
            start_date=self._to_db_date(self.start_date.value),
            end_date=self._to_db_date(self.end_date.value),
            note_keyword=self.note_keyword.value or None,
            partner_id=int(self.partner.value) if self.partner.value else None,
            position_ids=selected_positions or None,
            place_id=int(self.place.value) if self.place.value else None,
        )

    def _serialize_filters(self, filters: SearchFilters) -> dict[str, Any]:
        return {
            "start_date": filters.start_date,
            "end_date": filters.end_date,
            "note_keyword": filters.note_keyword,
            "partner_id": filters.partner_id,
            "position_ids": filters.position_ids or [],
            "place_id": filters.place_id,
        }

    def _filters_from_storage(self) -> SearchFilters:
        raw = app.storage.user.get("search_filters")
        if not isinstance(raw, dict):
            return self._default_filters()

        def _to_int(value):
            if value in (None, ""):
                return None
            return int(value)

        try:
            start_date = raw.get("start_date") or self._default_filters().start_date
            end_date = raw.get("end_date") or self._default_filters().end_date
            position_ids = [int(v) for v in raw.get("position_ids", [])]
            return SearchFilters(
                start_date=start_date,
                end_date=end_date,
                note_keyword=raw.get("note_keyword") or None,
                partner_id=_to_int(raw.get("partner_id")),
                position_ids=position_ids or None,
                place_id=_to_int(raw.get("place_id")),
            )
        except (TypeError, ValueError):
            return self._default_filters()

    def _save_filters(self, filters: SearchFilters) -> None:
        app.storage.user["search_filters"] = self._serialize_filters(filters)

    def _apply_filters(self, filters: SearchFilters) -> None:
        self.start_date.value = self._to_ui_date(filters.start_date)
        self.end_date.value = self._to_ui_date(filters.end_date)
        self.note_keyword.value = filters.note_keyword or ""
        self.partner.value = str(filters.partner_id) if filters.partner_id is not None else ""
        self.position_ids.value = [int(v) for v in (filters.position_ids or [])]
        self.place.value = str(filters.place_id) if filters.place_id is not None else ""

        self.start_date.update()
        self.end_date.update()
        self.note_keyword.update()
        self.partner.update()
        self.position_ids.update()
        self.place.update()

    def run_search(self, save: bool = True) -> None:
        filters = self.current_filters()
        if save:
            self._save_filters(filters)
        self.refresh_all(filters)

    def reset_filters(self) -> None:
        filters = self._default_filters()
        self._apply_filters(filters)
        self._save_filters(filters)
        self.refresh_all(filters)

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

    def _render_plotly(self, container, fig: Figure) -> None:
        container.clear()
        with container:
            ui.plotly(fig).classes("w-full").style("min-height: 26rem")

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

    def _event_row(self, e) -> dict:
        args = getattr(e, "args", None)
        if isinstance(args, dict):
            row = args.get("row", {})
            return row if isinstance(row, dict) else {}
        if isinstance(args, (list, tuple)):
            for item in args:
                if isinstance(item, dict) and "entry_id" in item:
                    return item
            return next((item for item in args if isinstance(item, dict)), {})
        return {}

    def show_entry_dialog(self, e) -> None:
        row = self._event_row(e)
        with ui.dialog() as dialog, ui.card().classes("w-[42rem] max-w-[95vw]"):
            ui.label(f"Entry #{row.get('entry_id', '')}").classes("text-xl font-semibold")
            with ui.grid(columns=2).classes("w-full gap-x-6 gap-y-2"):
                ui.label("Date:")
                ui.label(str(row.get("date") or ""))
                ui.label("Duration:")
                ui.label(str(row.get("duration") or ""))
                ui.label("Rating:")
                ui.label(str(row.get("rating") or ""))
                ui.label("My Orgasms:")
                ui.label(str(row.get("total_org") or 0))
                ui.label("Partner Orgasms:")
                ui.label(str(row.get("total_org_partner") or 0))
                ui.label("Partners:")
                ui.label(str(row.get("partners") or ""))
                ui.label("Positions:")
                ui.label(str(row.get("positions") or ""))
                ui.label("Places:")
                ui.label(str(row.get("places") or ""))
            ui.separator()
            ui.label("Description").classes("font-medium")
            ui.markdown(str(row.get("note") or "(No description)"))
            with ui.row().classes("justify-end w-full"):
                ui.button("Close", on_click=dialog.close)
        dialog.open()

    def refresh_charts(self, filters: SearchFilters) -> None:
        for refresh_chart in (
            self.refresh_partner_org_chart,
            self.refresh_rating_chart,
            self.refresh_streak_chart,
            self.refresh_position_chart,
            self.refresh_position_combinations_chart,
            self.refresh_position_upset_chart,
            self.refresh_location_room_chart,
        ):
            try:
                refresh_chart(filters)
            except Exception as exc:
                self._set_status(f"Chart refresh failed: {exc}")

    def refresh_partner_org_chart(self, filters: SearchFilters) -> None:
        try:
            df = self.service.partner_orgasms_timeseries(filters)
            fig = partner_orgasms_chart(df)
        except DataSourceError as exc:
            fig = Figure()
            fig.update_layout(title="Partner Orgasms Over Time (data source error)")
            self._set_status(str(exc))

        self._render_plotly(self.plot_container, fig)

    def refresh_rating_chart(self, filters: SearchFilters) -> None:
        try:
            df = self.service.ratings_dataframe(filters)
            fig = rating_histogram_chart(df)
        except DataSourceError as exc:
            fig = Figure()
            fig.update_layout(title="Rating Distribution (data source error)")
            self._set_status(str(exc))

        self._render_plotly(self.rating_plot_container, fig)

    def refresh_streak_chart(self, filters: SearchFilters) -> None:
        try:
            df = self.service.sex_streaks_dataframe(filters)
            fig = sex_streaks_chart(df)
        except Exception as exc:
            fig = Figure()
            fig.update_layout(title="Sex Streaks Over Time (chart error)")
            self._set_status(str(exc))

        self._render_plotly(self.streak_plot_container, fig)

    def refresh_position_chart(self, filters: SearchFilters) -> None:
        try:
            df = self.service.position_frequency_dataframe(filters)
            fig = position_frequency_chart(df)
        except Exception as exc:
            fig = Figure()
            fig.update_layout(title="Frequency of Sex Positions (chart error)")
            self._set_status(str(exc))

        self._render_plotly(self.position_plot_container, fig)

    def refresh_position_combinations_chart(self, filters: SearchFilters) -> None:
        try:
            df = self.service.position_combinations_dataframe(filters)
            fig = position_combinations_chart(df)
        except Exception as exc:
            fig = Figure()
            fig.update_layout(title="Position Combination Frequency (chart error)")
            self._set_status(str(exc))

        self._render_plotly(self.position_combo_plot_container, fig)

    def refresh_position_upset_chart(self, filters: SearchFilters) -> None:
        try:
            df = self.service.position_upset_dataframe(filters)
            fig = position_upset_chart(df, filters.start_date, filters.end_date)
        except Exception as exc:
            fig = Figure()
            fig.update_layout(title="Position Combination UpSet View (chart error)")
            self._set_status(str(exc))

        self._render_plotly(self.position_upset_plot_container, fig)

    def refresh_location_room_chart(self, filters: SearchFilters) -> None:
        try:
            df = self.service.location_room_sankey_dataframe(filters)
            fig = location_room_sankey_chart(df)
        except Exception as exc:
            fig = Figure()
            fig.update_layout(title="Frequency of Location/Room Combinations (chart error)")
            self._set_status(str(exc))

        self._render_plotly(self.location_room_plot_container, fig)

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

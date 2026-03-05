from __future__ import annotations

from datetime import datetime
import asyncio

from nicegui import ui
from plotly.graph_objs import Figure
from webapp.charts import (
    location_room_sankey_chart,
    duration_violin_chart,
    partner_orgasms_chart,
    position_association_chart,
    position_combinations_chart,
    position_upset_chart,
    position_frequency_chart,
    rating_histogram_chart,
    rolling_anomaly_chart,
    sex_streaks_chart,
)
from webapp.config import DEFAULT_DB_PATH
from webapp.services import DataSourceError, SearchFilters, StatsService


_LAST_FILTERS: SearchFilters | None = None
_LAST_MILESTONES: list[tuple[str, str]] = []
_LAST_CHART_SPECS: list[dict[str, object]] = []


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
        self.duration_plot_container = None
        self.anomaly_plot_container = None
        self.association_plot_container = None
        self.milestones: list[tuple[str, str]] = []
        self.milestone_list_container = None
        self.milestone_list_label = None
        self.person_metric_cards: dict[str, object] = {}
        self.chart_output_container = None
        self.chart_specs: list[dict[str, object]] = []
        self.people_choices: dict[str, str] = {}

    def _entry_table_columns(self, person_choices: dict[str, str]) -> list[dict[str, str]]:
        columns = [
            {"name": "entry_id", "label": "Entry\nID", "field": "entry_id"},
            {"name": "date", "label": "Date", "field": "date"},
            {"name": "duration", "label": "Duration\n(min)", "field": "duration"},
            {"name": "rating", "label": "Rating", "field": "rating"},
            {"name": "partners", "label": "People\nInvolved", "field": "partners"},
            {"name": "positions", "label": "Positions", "field": "positions"},
            {"name": "places", "label": "Places", "field": "places"},
        ]
        for pid, name in sorted(person_choices.items(), key=lambda item: item[1].lower()):
            key = f"person_orgasms__{pid}"
            columns.insert(4, {"name": key, "label": f"{name}\nOrgasms", "field": key})
        return columns

    def _apply_chart_spec_cache(self) -> None:
        global _LAST_CHART_SPECS
        _LAST_CHART_SPECS = [dict(s) for s in self.chart_specs]

    def _display_date(self, value: str | None) -> str:
        if not value:
            return "(any date)"
        for pattern in ("%Y.%m.%d", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(value, pattern)
                return dt.strftime("%B %d, %Y").replace(" 0", " ")
            except ValueError:
                continue
        return value

    def _chart_subtitle(
        self,
        chart_label: str,
        filters: SearchFilters,
        people_ids: list[int],
        person_choices: dict[str, str],
    ) -> str:
        start = self._display_date(filters.start_date)
        end = self._display_date(filters.end_date)
        subtitle = f"{chart_label} from {start} to {end}"
        if people_ids:
            people = ", ".join(person_choices.get(str(pid), str(pid)) for pid in people_ids)
            subtitle += f" for sessions involving: {people}"
        if filters.position_ids:
            position_map = dict(self.service.position_options())
            positions = ", ".join(position_map.get(int(pid), str(pid)) for pid in filters.position_ids)
            subtitle += f" with positions: {positions}"
        return subtitle

    def build(self) -> None:
        global _LAST_MILESTONES, _LAST_CHART_SPECS
        self.milestones = list(_LAST_MILESTONES)
        self.chart_specs = [dict(s) for s in _LAST_CHART_SPECS]

        ui.label("Personal Stats Webapp").classes("text-2xl font-bold")
        ui.label("Read-only browser + interactive charting for immutable export DB")
        ui.label(f"Configured DB path: {DEFAULT_DB_PATH}").classes("text-sm text-gray-500")

        self.status_label = ui.label("").classes("text-red-600")

        person_choices: dict[str, str] = {}

        with ui.row().classes("w-full gap-4 flex-wrap") as metrics_row:
            self.entries_metric = ui.card().classes("p-4").tight()
            with self.entries_metric:
                ui.label("Entries")
                ui.label("0").classes("text-2xl")

        partner_choices = {}
        position_choices: dict[int, str] = {}
        place_choices = {"": "Any"}
        try:
            partner_choices |= {str(pid): name for pid, name in self.service.people_options()}
            person_choices = dict(partner_choices)
            self.people_choices = dict(person_choices)
            position_choices = {pid: name for pid, name in self.service.position_options()}
            place_choices |= {str(pid): name for pid, name in self.service.place_options()}
        except DataSourceError as exc:
            self._set_status(str(exc))

        for _, name in sorted(person_choices.items(), key=lambda item: item[1].lower()):
            with metrics_row:
                card = ui.card().classes("p-4").tight()
                with card:
                    ui.label(f"{name} Orgasms")
                    ui.label("0").classes("text-2xl")
            self.person_metric_cards[name] = card

        with ui.card().classes("w-full"):
            ui.label("Filters").classes("text-lg")
            with ui.row().classes("w-full gap-3 items-start flex-wrap"):
                start_date = ui.date(value="2024-01-01", mask="YYYY-MM-DD").props("label='Start date'").classes("w-full md:w-[14rem]")
                end_date = ui.date(value="2024-12-31", mask="YYYY-MM-DD").props("label='End date'").classes("w-full md:w-[14rem]")
                with ui.column().classes("w-full md:flex-1 gap-2"):
                    note_keyword = ui.input("Note keyword contains", placeholder="optional").classes("w-full")
                    people = ui.select(partner_choices, label="People", value=[], multiple=True).props("use-chips clearable").classes("w-full")
                    position_ids = ui.select(
                        position_choices,
                        label="Positions",
                        value=[],
                        multiple=True,
                    ).props("use-chips clearable").classes("w-full")
                    place = ui.select(place_choices, label="Place", value="").classes("w-full")

            def _to_db_date(value: str | None) -> str | None:
                if not value:
                    return None
                return datetime.strptime(value, "%Y-%m-%d").strftime("%Y.%m.%d")

            def current_filters() -> SearchFilters:
                selected_positions = [int(v) for v in (position_ids.value or [])]
                return SearchFilters(
                    start_date=_to_db_date(start_date.value),
                    end_date=_to_db_date(end_date.value),
                    note_keyword=note_keyword.value or None,
                    person_ids=[int(v) for v in (people.value or [])] or None,
                    position_ids=selected_positions or None,
                    place_id=int(place.value) if place.value else None,
                )

            async def save_and_refresh() -> None:
                filters = current_filters()
                global _LAST_FILTERS
                _LAST_FILTERS = filters
                await self.refresh_all_async(filters)

            async def reset_filters() -> None:
                global _LAST_FILTERS
                _LAST_FILTERS = SearchFilters(start_date="2024.01.01", end_date="2024.12.31")
                start_date.value = "2024-01-01"
                end_date.value = "2024-12-31"
                note_keyword.value = ""
                people.value = []
                position_ids.value = []
                place.value = ""
                start_date.update()
                end_date.update()
                note_keyword.update()
                people.update()
                position_ids.update()
                place.update()
                await self.refresh_all_async(_LAST_FILTERS)

            with ui.dialog() as milestone_dialog, ui.card().classes("w-[34rem] max-w-[95vw]"):
                ui.label("Add Milestone").classes("text-lg font-semibold")
                milestone_date = ui.date(mask="YYYY-MM-DD").props("label='Milestone date'").classes("w-full")
                milestone_label = ui.input("Milestone label", placeholder="e.g., Started supplement").classes("w-full")

                async def submit_milestone() -> None:
                    normalized_date = self._normalize_ui_date(milestone_date.value)
                    label = str(milestone_label.value or "").strip()
                    if not normalized_date or not label:
                        self._set_status("Milestone date and label are required.")
                        return

                    self.milestones.append((normalized_date, label))
                    self.milestones = sorted(self.milestones, key=lambda x: x[0])
                    self._persist_milestones()
                    self._render_milestone_list()
                    self._set_status(f"Added milestone: {normalized_date} - {label}")
                    milestone_dialog.close()
                    await self.refresh_charts_async(current_filters())

                with ui.row().classes("w-full justify-end gap-2"):
                    ui.button("Cancel", on_click=milestone_dialog.close)
                    ui.button("Submit", on_click=submit_milestone)

            async def clear_milestones_action() -> None:
                await self.clear_milestones_async(current_filters)

            with ui.row().classes("w-full gap-2 flex-wrap"):
                ui.button("Add Milestone", on_click=milestone_dialog.open)
                ui.button("Clear Milestones", on_click=clear_milestones_action)

            with ui.row().classes("w-full") as milestone_list_row:
                self.milestone_list_container = milestone_list_row
                self.milestone_list_label = ui.label("").classes("text-sm text-gray-600")

            with ui.row().classes("w-full gap-2 flex-wrap"):
                ui.button("Run Search", on_click=save_and_refresh)
                ui.button("Reset Filters", on_click=reset_filters)
                ui.button("Export Table CSV", on_click=lambda: self.export_csv(current_filters()))
                ui.button("Export Chart PNG", on_click=lambda: self.export_chart_png(current_filters()))
                ui.button("Export Report JSON", on_click=lambda: self.export_report_json(current_filters()))

        with ui.card().classes("w-full"):
            ui.label("Entries")
            self.table = ui.table(
                columns=self._entry_table_columns(person_choices),
                rows=[],
                pagination=20,
            ).classes("w-full")
            self.table.on("rowClick", self.show_entry_dialog)

        chart_types = {
            "orgasms": "Orgasms Over Time",
            "ratings": "Rating Distribution",
            "duration": "Duration Distribution by Person",
            "anomaly": "Orgasm Anomaly Detection",
            "streaks": "Sex Streaks",
            "position_frequency": "Position Frequency",
            "position_combos": "Position Combinations",
            "position_association": "Position Association Rules",
            "position_upset": "Position UpSet",
            "location_room": "Location/Room Links",
        }
        chart_tips = {
            "orgasms": "Time series of orgasms per selected person.",
            "ratings": "Distribution of event ratings.",
            "duration": "Duration spread grouped by people.",
            "anomaly": "Flags unusual orgasm spikes against rolling baseline.",
            "streaks": "Consecutive sex/no-sex day runs.",
            "position_frequency": "How often each position appears.",
            "position_combos": "Most common position combinations.",
            "position_association": "Association rules for positions.",
            "position_upset": "Set-intersection view of top positions.",
            "location_room": "Location-to-room co-occurrence links.",
        }

        with ui.card().classes("w-full"):
            ui.label("Chart Builder").classes("text-lg")
            chart_type = ui.select(chart_types, label="Chart Type", value="orgasms").classes("w-full md:w-[24rem]")
            with chart_type:
                ui.tooltip("Select a chart type to configure and add.")
            chart_people = ui.select(person_choices, label="People", value=[], multiple=True).props("use-chips clearable").classes("w-full")
            include_trend = ui.switch("Include trend line", value=True)
            trend_kind = ui.select({"rolling_30": "30-day rolling mean"}, label="Trend calculation", value="rolling_30").classes("w-full md:w-[20rem]")
            alias_inputs: dict[int, object] = {}
            alias_container = ui.column().classes("w-full gap-2")

            tip_label = ui.label(chart_tips["orgasms"]).classes("text-sm text-gray-600")

            def update_tip() -> None:
                tip_label.set_text(chart_tips.get(chart_type.value, ""))
                tip_label.update()

            chart_type.on_value_change(lambda _: update_tip())

            def update_trend_controls() -> None:
                show_trend = chart_type.value == "orgasms"
                include_trend.set_visibility(show_trend)
                trend_kind.set_visibility(show_trend and bool(include_trend.value))

            chart_type.on_value_change(lambda _: update_trend_controls())
            include_trend.on_value_change(lambda _: update_trend_controls())
            update_trend_controls()

            def render_alias_inputs() -> None:
                alias_container.clear()
                alias_inputs.clear()
                with alias_container:
                    if chart_type.value != "orgasms":
                        return
                    ui.label("Optional display-name overrides (for anonymized charts)").classes("text-sm text-gray-600")
                    for selected in (chart_people.value or []):
                        pid = int(selected)
                        name = person_choices.get(str(pid), str(pid))
                        alias_inputs[pid] = ui.input(
                            label=f"Display name for {name}",
                            placeholder="leave blank to use original name",
                        ).classes("w-full md:w-[24rem]")

            chart_type.on_value_change(lambda _: render_alias_inputs())
            chart_people.on_value_change(lambda _: render_alias_inputs())
            render_alias_inputs()

            def add_chart() -> None:
                aliases = {
                    int(pid): str(inp.value).strip()
                    for pid, inp in alias_inputs.items()
                    if str(inp.value or "").strip()
                }
                spec = {
                    "type": chart_type.value,
                    "people": [int(v) for v in (chart_people.value or [])],
                    "include_trend": bool(include_trend.value) if chart_type.value == "orgasms" else False,
                    "trend_kind": trend_kind.value if chart_type.value == "orgasms" else None,
                    "person_aliases": aliases,
                }
                self.chart_specs.append(spec)
                self._apply_chart_spec_cache()
                self.render_chart_specs(current_filters(), person_choices)
                self._set_status("Added chart.")

            ui.button("Add Chart", on_click=add_chart)

        with ui.column().classes("w-full gap-4") as chart_out:
            self.chart_output_container = chart_out

        initial_filters = _LAST_FILTERS or SearchFilters(start_date="2024.01.01", end_date="2024.12.31")
        start_date.value = datetime.strptime(initial_filters.start_date, "%Y.%m.%d").strftime("%Y-%m-%d") if initial_filters.start_date else None
        end_date.value = datetime.strptime(initial_filters.end_date, "%Y.%m.%d").strftime("%Y-%m-%d") if initial_filters.end_date else None
        note_keyword.value = initial_filters.note_keyword or ""
        people.value = [str(v) for v in (initial_filters.person_ids or [])]
        position_ids.value = initial_filters.position_ids or []
        place.value = str(initial_filters.place_id) if initial_filters.place_id else ""
        start_date.update()
        end_date.update()
        note_keyword.update()
        people.update()
        position_ids.update()
        place.update()
        self._render_milestone_list()
        self.refresh_all(initial_filters)
        self.render_chart_specs(initial_filters, person_choices)

    def _normalize_ui_date(self, value) -> str | None:
        if not value:
            return None
        if isinstance(value, str):
            try:
                return datetime.strptime(value[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
            except ValueError:
                return None
        if isinstance(value, dict):
            year = value.get("year")
            month = value.get("month")
            day = value.get("day")
            if year and month and day:
                try:
                    return datetime(int(year), int(month), int(day)).strftime("%Y-%m-%d")
                except ValueError:
                    return None
        return None

    def _render_milestone_list(self) -> None:
        if self.milestone_list_label is None or self.milestone_list_container is None:
            return
        if not self.milestones:
            self.milestone_list_container.set_visibility(False)
            return
        label = " | ".join(f"{date} - {text}" for date, text in self.milestones)
        self.milestone_list_label.set_text(f"Milestones: {label}")
        self.milestone_list_container.set_visibility(True)


    def _persist_milestones(self) -> None:
        global _LAST_MILESTONES
        _LAST_MILESTONES = list(self.milestones)


    def clear_milestones(self, current_filters_cb) -> None:
        """Synchronous compatibility wrapper."""
        ui.run_task(self.clear_milestones_async(current_filters_cb))

    async def clear_milestones_async(self, current_filters_cb) -> None:
        self.milestones = []
        self._persist_milestones()
        self._render_milestone_list()
        self._set_status("Cleared milestones.")
        await self.refresh_charts_async(current_filters_cb())

    def _metric_value_label(self, card) -> ui.label:
        return card.default_slot.children[1]

    def _update_metrics(self, filters: SearchFilters) -> None:
        metrics = self.service.summary_metrics(filters)
        by_person = self.service.summary_metrics_by_person(filters)
        self._metric_value_label(self.entries_metric).set_text(str(metrics["entries"]))
        for person, card in self.person_metric_cards.items():
            self._metric_value_label(card).set_text(str(by_person.get(person, 0)))

    def render_chart_specs(self, filters: SearchFilters, person_choices: dict[str, str]) -> None:
        if self.chart_output_container is None:
            return
        self.chart_output_container.clear()
        with self.chart_output_container:
            for index, spec in enumerate(self.chart_specs, start=1):
                with ui.card().classes("w-full"):
                    title = {
                        "orgasms": "Orgasms Over Time",
                        "ratings": "Rating Distribution",
                        "duration": "Duration Distribution by Person",
                        "anomaly": "Orgasm Anomaly Detection",
                        "streaks": "Sex Streaks",
                        "position_frequency": "Position Frequency",
                        "position_combos": "Position Combinations",
                        "position_association": "Position Association Rules",
                        "position_upset": "Position UpSet",
                        "location_room": "Location/Room Links",
                    }.get(str(spec.get("type")), str(spec.get("type")))
                    ui.label(f"Chart {index}: {title}").classes("text-lg")
                    people_ids = spec.get("people", []) or []
                    people_text = ", ".join(person_choices.get(str(pid), str(pid)) for pid in people_ids) or "All people"
                    ui.label(f"Parameters: people={people_text}; trend={spec.get('include_trend')}; trend_kind={spec.get('trend_kind')}").classes("text-sm text-gray-600")
                    fig = self._build_chart(filters, spec, person_choices)
                    ui.plotly(fig).classes("w-full").style("min-height: 26rem")

    def _build_chart(self, filters: SearchFilters, spec: dict[str, object], person_choices: dict[str, str]) -> Figure:
        chart_type = spec.get("type")
        people_ids = [int(v) for v in (spec.get("people") or [])]
        try:
            if chart_type == "orgasms":
                df = self.service.orgasms_by_person_timeseries(filters, people_ids)
                aliases = spec.get("person_aliases") if isinstance(spec.get("person_aliases"), dict) else {}
                if not df.empty and aliases:
                    rename_map = {
                        person_choices.get(str(pid), str(pid)): alias
                        for pid, alias in aliases.items()
                        if str(alias).strip()
                    }
                    if rename_map:
                        df = df.copy()
                        df["person"] = df["person"].apply(lambda p: rename_map.get(str(p), str(p)))
                subtitle = self._chart_subtitle("Orgasms per session", filters, people_ids, person_choices)
                return partner_orgasms_chart(
                    df,
                    milestones=self.milestones,
                    include_trend=bool(spec.get("include_trend", True)),
                    subtitle=subtitle,
                )
            if chart_type == "ratings":
                subtitle = self._chart_subtitle("Rating distribution", filters, people_ids, person_choices)
                return rating_histogram_chart(self.service.ratings_dataframe(filters), subtitle=subtitle)
            if chart_type == "duration":
                subtitle = self._chart_subtitle("Session duration", filters, people_ids, person_choices)
                return duration_violin_chart(self.service.duration_by_partner_dataframe(filters), subtitle=subtitle)
            if chart_type == "anomaly":
                subtitle = self._chart_subtitle("Orgasm anomaly detection", filters, people_ids, person_choices)
                return rolling_anomaly_chart(
                    self.service.partner_orgasms_anomaly_dataframe(filters),
                    milestones=self.milestones,
                    subtitle=subtitle,
                )
            if chart_type == "streaks":
                subtitle = self._chart_subtitle("Sex streaks", filters, people_ids, person_choices)
                return sex_streaks_chart(
                    self.service.sex_streaks_dataframe(filters),
                    milestones=self.milestones,
                    subtitle=subtitle,
                )
            if chart_type == "position_frequency":
                subtitle = self._chart_subtitle("Position frequency", filters, people_ids, person_choices)
                return position_frequency_chart(
                    self.service.position_frequency_dataframe(filters, require_people=people_ids),
                    subtitle=subtitle,
                )
            if chart_type == "position_combos":
                subtitle = self._chart_subtitle("Position combinations", filters, people_ids, person_choices)
                return position_combinations_chart(
                    self.service.position_combinations_dataframe(filters, require_people=people_ids),
                    subtitle=subtitle,
                )
            if chart_type == "position_association":
                subtitle = self._chart_subtitle("Position association rules", filters, people_ids, person_choices)
                return position_association_chart(
                    self.service.position_association_rules_dataframe(filters, require_people=people_ids),
                    subtitle=subtitle,
                )
            if chart_type == "position_upset":
                subtitle = self._chart_subtitle("Position UpSet", filters, people_ids, person_choices)
                return position_upset_chart(
                    self.service.position_upset_dataframe(filters, require_people=people_ids),
                    filters.start_date,
                    filters.end_date,
                    subtitle=subtitle,
                )
            if chart_type == "location_room":
                subtitle = self._chart_subtitle("Location/Room links", filters, people_ids, person_choices)
                return location_room_sankey_chart(
                    self.service.location_room_sankey_dataframe(filters),
                    subtitle=subtitle,
                )
        except Exception as exc:
            self._set_status(f"Chart build failed: {exc}")
        fig = Figure()
        fig.update_layout(title="Chart unavailable")
        return fig

    def _set_status(self, message: str) -> None:
        self.status_label.text = message
        self.status_label.update()

    def _render_plotly(self, container, fig: Figure) -> None:
        children = getattr(container.default_slot, "children", [])
        if children:
            existing = children[0]
            if hasattr(existing, "figure"):
                existing.figure = fig
                existing.update()
                return
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

    async def refresh_all_async(self, filters: SearchFilters) -> None:
        self.refresh_entries(filters)
        await asyncio.sleep(0)
        await self.refresh_charts_async(filters)
        await asyncio.sleep(0)
        try:
            self._update_metrics(filters)
        except DataSourceError as exc:
            self._set_status(str(exc))

    def refresh_entries(self, filters: SearchFilters) -> None:
        try:
            rows = self.service.search_entries(filters)
            for row in rows:
                orgasms = row.get("person_orgasms") if isinstance(row.get("person_orgasms"), dict) else {}
                for pid, name in self.people_choices.items():
                    row[f"person_orgasms__{pid}"] = int(orgasms.get(name, 0))
            self.table.rows = rows
            self.table.update()
            self.table.props("table-header-style='white-space: pre-line;'")
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
                ui.label("People:")
                ui.label(str(row.get("partners") or ""))
                ui.label("Positions:")
                ui.label(str(row.get("positions") or ""))
                ui.label("Places:")
                ui.label(str(row.get("places") or ""))
            ui.separator()
            ui.label("Orgasms by person").classes("font-medium")
            orgasms = row.get("person_orgasms") if isinstance(row.get("person_orgasms"), dict) else {}
            for person, count in sorted(orgasms.items(), key=lambda item: item[0].lower()):
                ui.label(f"{person}: {int(count)}")
            ui.separator()
            ui.label("Description").classes("font-medium")
            ui.markdown(str(row.get("note") or "(No description)"))
            with ui.row().classes("justify-end w-full"):
                ui.button("Close", on_click=dialog.close)
        dialog.open()

    def refresh_charts(self, filters: SearchFilters) -> None:
        options = {str(pid): name for pid, name in self.service.people_options()}
        self.render_chart_specs(filters, options)

    async def refresh_charts_async(self, filters: SearchFilters) -> None:
        self.refresh_charts(filters)
        await asyncio.sleep(0)

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
            fig = partner_orgasms_chart(df, milestones=self.milestones)
            png_path = self.service.temp_export_path("partner_orgasms_chart_", ".png")
            fig.write_image(str(png_path), format="png", engine="kaleido")
            ui.download(str(png_path), filename="partner_orgasms_chart.png")
            self._set_status(f"Chart PNG export ready: {png_path}")
        except DataSourceError as exc:
            self._set_status(str(exc))
        except Exception as exc:
            self._set_status(f"Chart export failed: {exc}")

from __future__ import annotations

from datetime import datetime
import asyncio

from nicegui import ui
from plotly.graph_objs import Figure
from webapp.charts import (
    location_room_sankey_chart,
    year_in_review_chart,
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
from webapp.services import (
    DataSourceError, SearchFilters, StatsService,
    SEX_TYPE_MAPPING, INITIATOR_MAPPING, PLACE_MAPPING,
)


_LAST_FILTERS: SearchFilters | None = None
_LAST_MILESTONES: list[tuple[str, str]] = []
_LAST_CHART_SPECS: list[dict[str, object]] = []


class PersonalStatsApp:
    def __init__(self, service: StatsService):
        self.service = service
        self.table: ui.table | None = None
        self.status_label: ui.label | None = None
        self.entries_metric = None
        self.milestones: list[tuple[str, str]] = []
        self.milestone_list_container = None
        self.milestone_list_label = None
        self.person_metric_cards: dict[str, object] = {}
        self.chart_output_container = None
        self.chart_specs: list[dict[str, object]] = []
        self.people_choices: dict[str, str] = {}
        # Tracks currently selected (checked) row event_ids for multi-select ops
        self._selected_event_ids: list[int] = []
        self._selection_label = None   # set during build()
        self._ops_group = None         # set during build()
        self._current_filters_cb = None  # set during build()

    # ── Table columns ─────────────────────────────────────────────────────────

    def _entry_table_columns(self, person_choices: dict[str, str]) -> list[dict]:
        columns = [
            {"name": "entry_id",  "label": "Event\nID",       "field": "entry_id"},
            {"name": "date",      "label": "Date",             "field": "date"},
            {"name": "duration",  "label": "Duration\n(min)",  "field": "duration"},
            {"name": "rating",    "label": "Rating",           "field": "rating"},
            {"name": "partners",  "label": "People\nInvolved", "field": "partners"},
            {
                "name": "positions", "label": "Positions", "field": "positions",
                "classes": "min-w-[200px] max-w-[400px]",
                "style": "white-space: normal; word-break: break-word;",
            },
            {"name": "places",    "label": "Places",           "field": "places"},
            {"name": "sex_types", "label": "Sex Types",        "field": "sex_types"},
            {"name": "merge_confidence", "label": "Merge",     "field": "merge_confidence"},
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
        aliases: dict[int, str] | None = None,
    ) -> str:
        start = self._display_date(filters.start_date)
        end   = self._display_date(filters.end_date)
        subtitle = f"{chart_label} from {start} to {end}"
        if people_ids:
            names = [
                (aliases or {}).get(pid) or person_choices.get(str(pid), str(pid))
                for pid in people_ids
            ]
            subtitle += f" for sessions involving: {', '.join(names)}"
        if filters.position_ids:
            position_map = dict(self.service.position_options())
            positions = ", ".join(
                position_map.get(int(pid), str(pid)) for pid in filters.position_ids
            )
            subtitle += f" with positions: {positions}"
        return subtitle

    # ── Main build ────────────────────────────────────────────────────────────

    def build(self) -> None:
        global _LAST_MILESTONES, _LAST_CHART_SPECS
        self.milestones  = list(_LAST_MILESTONES)
        self.chart_specs = [dict(s) for s in _LAST_CHART_SPECS]

        ui.label("Personal Stats Webapp").classes("text-2xl font-bold")
        ui.label("Read-only browser + interactive charting for immutable export DB")
        ui.label(f"Configured DB path: {DEFAULT_DB_PATH}").classes("text-sm text-gray-500")

        self.status_label = ui.label("").classes("text-red-600")

        person_choices: dict[str, str] = {}

        with ui.row().classes("w-full gap-4 flex-wrap") as metrics_row:
            self.entries_metric = ui.card().classes("p-4").tight()
            with self.entries_metric:
                ui.label("Events")
                ui.label("0").classes("text-2xl")

        partner_choices: dict[str, str] = {}
        position_choices: dict[int, str] = {}
        place_choices: dict[str, str] = {"": "Any"}
        try:
            partner_choices |= {str(pid): name for pid, name in self.service.people_options()}
            person_choices   = dict(partner_choices)
            self.people_choices = dict(person_choices)
            position_choices = {pid: name for pid, name in self.service.position_options()}
            place_choices   |= {str(pid): name for pid, name in self.service.place_options()}
        except DataSourceError as exc:
            self._set_status(str(exc))

        for _, name in sorted(person_choices.items(), key=lambda item: item[1].lower()):
            with metrics_row:
                card = ui.card().classes("p-4").tight()
                with card:
                    ui.label(f"{name} Orgasms")
                    ui.label("0").classes("text-2xl")
            self.person_metric_cards[name] = card

        # ── Filters card ──────────────────────────────────────────────────────
        with ui.card().classes("w-full"):
            ui.label("Filters").classes("text-lg")
            with ui.row().classes("w-full gap-3 items-start flex-wrap"):
                start_date   = ui.date(value="2024-01-01", mask="YYYY-MM-DD").props("label='Start date'").classes("w-full md:w-[14rem]")
                end_date     = ui.date(value="2024-12-31", mask="YYYY-MM-DD").props("label='End date'").classes("w-full md:w-[14rem]")
                with ui.column().classes("w-full md:flex-1 gap-2"):
                    note_keyword = ui.input("Note keyword contains", placeholder="optional").classes("w-full")
                    people       = ui.select(partner_choices, label="People", value=[], multiple=True).props("use-chips clearable").classes("w-full")
                    position_ids = ui.select(position_choices, label="Positions", value=[], multiple=True).props("use-chips clearable").classes("w-full")
                    place        = ui.select(place_choices, label="Place", value="").classes("w-full")

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

            self._current_filters_cb = current_filters

            async def save_and_refresh() -> None:
                filters = current_filters()
                global _LAST_FILTERS
                _LAST_FILTERS = filters
                await self.refresh_all_async(filters)

            async def reset_filters() -> None:
                global _LAST_FILTERS
                _LAST_FILTERS = SearchFilters(start_date="2024.01.01", end_date="2024.12.31")
                start_date.value = "2024-01-01"
                end_date.value   = "2024-12-31"
                note_keyword.value = ""
                people.value       = []
                position_ids.value = []
                place.value        = ""
                for w in (start_date, end_date, note_keyword, people, position_ids, place):
                    w.update()
                await self.refresh_all_async(_LAST_FILTERS)

            # Milestone dialog
            with ui.dialog() as milestone_dialog, ui.card().classes("w-[34rem] max-w-[95vw]"):
                ui.label("Add Milestone").classes("text-lg font-semibold")
                milestone_date  = ui.date(mask="YYYY-MM-DD").props("label='Milestone date'").classes("w-full")
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
                ui.button("Add Milestone",    on_click=milestone_dialog.open)
                ui.button("Clear Milestones", on_click=clear_milestones_action)

            with ui.row().classes("w-full") as milestone_list_row:
                self.milestone_list_container = milestone_list_row
                self.milestone_list_label = ui.label("").classes("text-sm text-gray-600")

            with ui.row().classes("w-full gap-2 flex-wrap"):
                ui.button("Run Search",      on_click=save_and_refresh)
                ui.button("Reset Filters",   on_click=reset_filters)
                ui.button("Export Table CSV",  on_click=lambda: self.export_csv(current_filters()))
                ui.button("Export Chart PNG",  on_click=lambda: self.export_chart_png(current_filters()))
                ui.button("Export Report JSON", on_click=lambda: self.export_report_json(current_filters()))

        # ── Events table ──────────────────────────────────────────────────────
        with ui.card().classes("w-full"):
            with ui.row().classes("w-full items-center gap-4 flex-wrap"):
                ui.label("Events").classes("text-lg")
                self._selection_label = ui.label("").classes("text-sm text-gray-500")
                self._selection_label.set_visibility(False)
                self._ops_group = ui.row().classes("gap-2 items-center")
                self._ops_group.set_visibility(False)
                with self._ops_group:
                    _ops_btn = ui.dropdown_button("Operations", auto_close=True).props("color=orange outline")
                    with _ops_btn:
                        ui.item(
                            "Merge selected",
                            on_click=lambda: self._open_merge_dialog(current_filters),
                        )
                        ui.item(
                            "Delete selected",
                            on_click=lambda: self._open_delete_dialog(current_filters),
                        ).classes("text-red-600")

            self.table = ui.table(
                columns=self._entry_table_columns(person_choices),
                rows=[],
                row_key="entry_id",
                selection="multiple",
                pagination=20,
            ).classes("w-full")

            self.table.add_slot("header", r"""
                <q-tr :props="props">
                    <q-th auto-width />
                    <q-th v-for="col in props.cols" :key="col.name" :props="props">
                        <div v-html="col.label"
                             style="line-height:1.2; white-space:normal;
                                    text-align:center; width:100%;"></div>
                    </q-th>
                </q-tr>
            """)

            self.table.on("selection", self._on_table_selection)
            self.table.on("rowClick",  self._on_row_click)

        # ── Chart builder card ────────────────────────────────────────────────
        chart_types = {
            "orgasms":            "Orgasms Over Time",
            "ratings":            "Rating Distribution",
            "duration":           "Duration Distribution by Person",
            "anomaly":            "Orgasm Anomaly Detection",
            "streaks":            "Sex Streaks",
            "position_frequency": "Position Frequency",
            "position_combos":    "Position Combinations",
            "position_association": "Position Association Rules",
            "position_upset":     "Position UpSet",
            "location_room":      "Location/Room Links",
            "year_in_review":     "Rendezvous Report",
        }
        chart_tips = {
            "orgasms":            "Time series of orgasms per selected person.",
            "ratings":            "Distribution of event ratings.",
            "duration":           "Duration spread grouped by people.",
            "anomaly":            "Flags unusual orgasm spikes against rolling baseline.",
            "streaks":            "Consecutive sex/no-sex day runs.",
            "position_frequency": "How often each position appears.",
            "position_combos":    "Most common position combinations.",
            "position_association": "Association rules for positions.",
            "position_upset":     "Set-intersection view of top positions.",
            "location_room":      "Location-to-room co-occurrence links.",
            "year_in_review":     "A card-style stats overview. Select people to filter to sessions involving all of them.",
        }

        with ui.card().classes("w-full"):
            ui.label("Chart Builder").classes("text-lg")
            chart_type  = ui.select(chart_types, label="Chart Type", value="orgasms").classes("w-full md:w-[24rem]")
            with chart_type:
                ui.tooltip("Select a chart type to configure and add.")
            chart_people = ui.select(person_choices, label="People", value=[], multiple=True).props("use-chips clearable").classes("w-full")
            include_trend = ui.switch("Include trend line", value=True)
            trend_kind    = ui.select({"rolling_30": "30-day rolling mean", "loess": "LOESS"}, label="Trend calculation", value="rolling_30").classes("w-full md:w-[20rem]")
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
                    ui.label("Optional display-name overrides (for anonymized charts)").classes("text-sm text-gray-600")
                    for selected in (chart_people.value or []):
                        pid  = int(selected)
                        name = person_choices.get(str(pid), str(pid))
                        alias_inputs[pid] = ui.input(
                            label=f"Display name for {name}",
                            placeholder="leave blank to use original name",
                        ).classes("w-full md:w-[24rem]")

            chart_type.on_value_change(lambda _: render_alias_inputs())
            chart_people.on_value_change(lambda _: render_alias_inputs())
            render_alias_inputs()

            # Dataset overrides (orgasms chart year-overlay)
            datasets: list[dict[str, str]] = []
            dataset_section = ui.column().classes("w-full gap-2")

            def render_dataset_section() -> None:
                dataset_section.set_visibility(chart_type.value == "orgasms")

            chart_type.on_value_change(lambda _: render_dataset_section())

            with dataset_section:
                ui.label("Datasets — add two or more to overlay years on the same axis").classes("text-sm text-gray-600")
                dataset_rows_container = ui.column().classes("w-full gap-1")

                def refresh_dataset_rows() -> None:
                    dataset_rows_container.clear()
                    with dataset_rows_container:
                        for ds in datasets:
                            with ui.row().classes("w-full items-center gap-2 flex-wrap"):
                                ui.label(ds["label"]).classes("font-mono text-sm w-[8rem]")
                                ui.label(f"{ds['start_date']} → {ds['end_date']}").classes("text-sm text-gray-600")
                                def make_remove(d=ds):
                                    def remove():
                                        datasets.remove(d)
                                        refresh_dataset_rows()
                                    return remove
                                ui.button("✕", on_click=make_remove()).props("flat dense").classes("text-red-500")

                with ui.dialog() as dataset_dialog, ui.card().classes("w-[34rem] max-w-[95vw]"):
                    ui.label("Add Dataset").classes("text-lg font-semibold")
                    ds_label = ui.input("Series label", placeholder="e.g., 2024").classes("w-full")
                    ds_start = ui.date(mask="YYYY-MM-DD").props("label='Start date'").classes("w-full")
                    ds_end   = ui.date(mask="YYYY-MM-DD").props("label='End date'").classes("w-full")

                    def submit_dataset() -> None:
                        label = str(ds_label.value or "").strip()
                        start = self._normalize_ui_date(ds_start.value)
                        end   = self._normalize_ui_date(ds_end.value)
                        if not label or not start or not end:
                            self._set_status("Dataset label, start date and end date are all required.")
                            return
                        datasets.append({"label": label, "start_date": start, "end_date": end})
                        refresh_dataset_rows()
                        dataset_dialog.close()

                    with ui.row().classes("w-full justify-end gap-2"):
                        ui.button("Cancel", on_click=dataset_dialog.close)
                        ui.button("Add",    on_click=submit_dataset)

                ui.button("Add Dataset", on_click=dataset_dialog.open)

            render_dataset_section()

            # Custom title (year_in_review only)
            report_title_section = ui.column().classes("w-full gap-2")

            def render_report_title_section() -> None:
                report_title_section.set_visibility(chart_type.value == "year_in_review")

            chart_type.on_value_change(lambda _: render_report_title_section())

            with report_title_section:
                report_custom_title = ui.input(
                    "Custom report title",
                    placeholder="leave blank for default  ✦ Rendezvous Report ✦",
                ).classes("w-full md:w-[32rem]")

            render_report_title_section()

            def add_chart() -> None:
                aliases = {
                    int(pid): str(inp.value).strip()
                    for pid, inp in alias_inputs.items()
                    if str(inp.value or "").strip()
                }
                spec = {
                    "type":          chart_type.value,
                    "people":        [int(v) for v in (chart_people.value or [])],
                    "include_trend": bool(include_trend.value) if chart_type.value == "orgasms" else False,
                    "trend_kind":    trend_kind.value if chart_type.value == "orgasms" else None,
                    "person_aliases": aliases,
                    "datasets":      [dict(d) for d in datasets] if datasets else [],
                    "custom_title":  str(report_custom_title.value or "").strip() if chart_type.value == "year_in_review" else None,
                }
                self.chart_specs.append(spec)
                self._apply_chart_spec_cache()
                self.render_chart_specs(current_filters(), person_choices)
                self._set_status("Added chart.")

            ui.button("Add Chart", on_click=add_chart)

        with ui.column().classes("w-full gap-4") as chart_out:
            self.chart_output_container = chart_out

        # ── Restore last state ────────────────────────────────────────────────
        initial_filters = _LAST_FILTERS or SearchFilters(start_date="2024.01.01", end_date="2024.12.31")
        start_date.value   = datetime.strptime(initial_filters.start_date, "%Y.%m.%d").strftime("%Y-%m-%d") if initial_filters.start_date else None
        end_date.value     = datetime.strptime(initial_filters.end_date, "%Y.%m.%d").strftime("%Y-%m-%d") if initial_filters.end_date else None
        note_keyword.value = initial_filters.note_keyword or ""
        people.value       = [str(v) for v in (initial_filters.person_ids or [])]
        position_ids.value = initial_filters.position_ids or []
        place.value        = str(initial_filters.place_id) if initial_filters.place_id else ""
        for w in (start_date, end_date, note_keyword, people, position_ids, place):
            w.update()
        self._render_milestone_list()
        self.refresh_all(initial_filters)
        self.render_chart_specs(initial_filters, person_choices)

    # ── Multi-select / merge button ───────────────────────────────────────────

    def _on_table_selection(self, e) -> None:
        """Called when the table's selection changes.

        NiceGUI keeps self.table.selected in sync automatically; we read from
        there rather than trying to parse the event args, whose shape varies
        across NiceGUI versions.
        """
        import sys
        # Read the authoritative selection list maintained by NiceGUI
        selected_rows = getattr(self.table, "selected", None) or []
        print(f"[DEBUG] table.selected = {selected_rows!r}", file=sys.stderr, flush=True)

        self._selected_event_ids = [
            int(r["entry_id"])
            for r in selected_rows
            if isinstance(r, dict) and "entry_id" in r
        ]
        print(f"[DEBUG] _selected_event_ids = {self._selected_event_ids!r}", file=sys.stderr, flush=True)

        n = len(self._selected_event_ids)
        if self._ops_group:
            self._ops_group.set_visibility(n >= 1)
        if self._selection_label:
            if n >= 1:
                self._selection_label.set_text(f"{n} event(s) selected")
            self._selection_label.set_visibility(n >= 1)

    def _on_row_click(self, e) -> None:
        """Open edit dialog on row click (only when not in multi-select mode)."""
        # If multiple rows are selected, row click just changes selection;
        # only open the dialog when we're not in a multi-select flow.
        if len(self._selected_event_ids) > 1:
            return
        row = self._event_row(e)
        event_id = int(row.get("entry_id") or 0)
        if event_id:
            self.show_entry_dialog(e)

    # ── Merge dialog ──────────────────────────────────────────────────────────

    def _open_merge_dialog(self, current_filters_cb) -> None:
        import sys
        event_ids = list(self._selected_event_ids)
        print(f"[DEBUG] _open_merge_dialog called, event_ids={event_ids!r}", file=sys.stderr, flush=True)
        if len(event_ids) < 2:
            self._set_status("Select two or more events to merge.")
            return

        # Fetch details for each selected event
        event_details: list[dict] = []
        person_map   = dict(self.service.people_options())
        position_map = dict(self.service.position_options())
        for eid in sorted(event_ids):
            try:
                ev = self.service.fetch_event_for_edit(eid)
                event_details.append(ev)
            except DataSourceError as exc:
                self._set_status(str(exc))
                return

        survivor_id = min(event_ids)

        with ui.dialog() as merge_dialog, ui.card().classes("w-[62rem] max-w-[98vw] gap-3"):
            ui.label("Merge Events").classes("text-xl font-semibold")
            ui.label(
                f"Merging {len(event_ids)} events into event #{survivor_id}. "
                "All interactions and reports will be consolidated. "
                "This cannot be undone (a backup will be created first)."
            ).classes("text-sm text-gray-600")

            # Canonical date/duration controls
            all_dates = [str(ev.get("date") or "") for ev in event_details]
            all_durs  = [int(ev.get("approx_duration") or 0) for ev in event_details if ev.get("approx_duration")]
            default_date = min(all_dates) if all_dates else ""

            # Detect merge type: if any reporter_person_id appears in more than one
            # event's reports, these are same-source events (sequential interactions
            # within one session) → sum durations.  Otherwise it's a cross-source
            # duplicate recording of the same interaction → average durations.
            all_reporter_ids: list[int] = []
            for ev in event_details:
                for rpt in (ev.get("reports") or []):
                    pid = rpt.get("reporter_person_id")
                    if pid is not None:
                        all_reporter_ids.append(int(pid))
            same_source = len(all_reporter_ids) != len(set(all_reporter_ids))

            if all_durs:
                default_dur  = sum(all_durs) if same_source else int(sum(all_durs) / len(all_durs))
            else:
                default_dur = None
            dur_hint = (
                f"Sum of component durations ({' + '.join(str(d) for d in all_durs)} min) — same-source merge"
                if same_source and all_durs else
                f"Average of component durations — cross-source merge"
                if all_durs else ""
            )

            try:
                display_default_date = datetime.strptime(default_date[:10], "%Y.%m.%d").strftime("%Y-%m-%d")
            except ValueError:
                display_default_date = default_date

            with ui.row().classes("w-full gap-4 flex-wrap items-start"):
                canon_date = ui.date(
                    value=display_default_date, mask="YYYY-MM-DD"
                ).props("label='Canonical date'").classes("w-full md:w-[14rem]")
                canon_dur  = ui.number(
                    "Canonical duration (min)", value=default_dur, min=0, step=1
                ).classes("w-full md:w-[14rem]")
            if dur_hint:
                ui.label(dur_hint).classes("text-xs text-gray-400 -mt-2")

            # Preview of each event being merged
            ui.label("Events being merged:").classes("font-medium text-sm mt-2")
            for ev in event_details:
                eid = int(ev.get("event_id") or ev.get("entry_id") or 0)
                interactions = ev.get("interactions") or []
                reports      = ev.get("reports") or []
                label_parts  = [f"Event #{eid}  ·  {ev.get('date', '?')}"]
                if ev.get("approx_duration"):
                    label_parts.append(f"{ev['approx_duration']} min")
                if interactions:
                    all_pids: set[int] = set()
                    for intr in interactions:
                        all_pids.update(intr.get("participant_ids") or [])
                    names = ", ".join(
                        person_map.get(pid, f"#{pid}") for pid in sorted(all_pids)
                    )
                    label_parts.append(f"participants: {names}")
                # Show positions (union across interactions)
                all_pos_ids: set[int] = set()
                for intr in interactions:
                    all_pos_ids.update(intr.get("position_ids") or [])
                if all_pos_ids:
                    pos_names = ", ".join(
                        position_map.get(pid, f"#{pid}") for pid in sorted(all_pos_ids)
                    )
                    label_parts.append(f"positions: {pos_names}")
                # Show notes from reports
                notes = [str(r.get("note") or "").strip() for r in reports if r.get("note")]
                if notes:
                    label_parts.append(f"notes: {' | '.join(notes[:2])}")
                with ui.card().classes("w-full bg-gray-50"):
                    ui.label("  ·  ".join(label_parts)).classes("text-sm font-mono")

            merge_status = ui.label("").classes("text-sm text-red-500 mt-1")

            async def do_merge() -> None:
                try:
                    raw_d = str(canon_date.value or "").strip()
                    try:
                        db_date = datetime.strptime(raw_d[:10], "%Y-%m-%d").strftime("%Y.%m.%d")
                    except ValueError:
                        merge_status.set_text("Invalid date.")
                        return
                    dur = int(canon_dur.value) if canon_dur.value is not None else None

                    try:
                        backup_path = self.service.backup_db()
                        self._set_status(f"Backup saved: {backup_path.name}")
                    except Exception:
                        pass

                    survivor = self.service.merge_events(
                        event_ids,
                        canonical_date=db_date,
                        canonical_duration=dur,
                    )
                    merge_dialog.close()
                    self._selected_event_ids = []
                    if self._ops_group:
                        self._ops_group.classes(add="hidden")
                        self._ops_group.update()
                    if self._selection_label:
                        self._selection_label.classes(add="hidden")
                        self._selection_label.update()
                    self._set_status(f"Merged into event #{survivor}.")
                    global _LAST_FILTERS
                    self.refresh_entries(_LAST_FILTERS or SearchFilters())
                except DataSourceError as exc:
                    merge_status.set_text(str(exc))
                except Exception as exc:
                    import traceback, sys
                    print(traceback.format_exc(), file=sys.stderr)
                    merge_status.set_text(f"Unexpected error: {exc}")

            with ui.row().classes("justify-end w-full gap-2 mt-2"):
                ui.button("Cancel", on_click=merge_dialog.close).props("flat")
                ui.button("Confirm Merge", on_click=do_merge).props("color=orange")

        merge_dialog.open()

    # ── Delete dialog ─────────────────────────────────────────────────────────

    def _open_delete_dialog(self, current_filters_cb) -> None:
        event_ids = list(self._selected_event_ids)
        if not event_ids:
            self._set_status("No events selected.")
            return

        with ui.dialog() as delete_dialog, ui.card().classes("w-[36rem] max-w-[95vw] gap-3"):
            ui.label("Delete Events").classes("text-xl font-semibold text-red-700")
            ui.label(
                f"Permanently delete {len(event_ids)} event(s)? "
                "A backup will be created first, but this action cannot be undone."
            ).classes("text-sm text-gray-600")
            ui.label(
                f"Event IDs: {', '.join(str(e) for e in sorted(event_ids))}"
            ).classes("text-sm font-mono text-gray-500")

            delete_status = ui.label("").classes("text-sm text-red-500 mt-1")

            async def do_delete() -> None:
                try:
                    try:
                        backup_path = self.service.backup_db()
                        self._set_status(f"Backup saved: {backup_path.name}")
                    except Exception:
                        pass

                    self.service.delete_events(event_ids)
                    delete_dialog.close()
                    self._selected_event_ids = []
                    if self._ops_group:
                        self._ops_group.classes(add="hidden")
                        self._ops_group.update()
                    if self._selection_label:
                        self._selection_label.classes(add="hidden")
                        self._selection_label.update()
                    self._set_status(f"Deleted {len(event_ids)} event(s).")
                    global _LAST_FILTERS
                    self.refresh_entries(_LAST_FILTERS or SearchFilters())
                except DataSourceError as exc:
                    delete_status.set_text(str(exc))
                except Exception as exc:
                    import traceback, sys
                    print(traceback.format_exc(), file=sys.stderr)
                    delete_status.set_text(f"Unexpected error: {exc}")

            with ui.row().classes("justify-end w-full gap-2 mt-2"):
                ui.button("Cancel", on_click=delete_dialog.close).props("flat")
                ui.button("Delete", on_click=do_delete).props("color=negative")

        delete_dialog.open()

    def show_entry_dialog(self, e) -> None:
        row      = self._event_row(e)
        event_id = int(row.get("entry_id") or 0)
        if not event_id:
            return

        try:
            raw = self.service.fetch_event_for_edit(event_id)
        except DataSourceError as exc:
            self._set_status(str(exc))
            return

        person_map   = dict(self.service.people_options())
        position_map = dict(self.service.position_options())
        place_map    = {pid: name for pid, name in self.service.place_options()}

        reports      = raw.get("reports") or []
        interactions = raw.get("interactions") or []

        raw_date = str(raw.get("date") or "")
        try:
            display_date = datetime.strptime(raw_date[:10], "%Y.%m.%d").strftime("%Y-%m-%d")
        except ValueError:
            display_date = raw_date

        with ui.dialog() as dialog, ui.card().classes("w-[56rem] max-w-[98vw] gap-3"):
            ui.label(f"Edit Event #{event_id}").classes("text-xl font-semibold")
            if raw.get("merge_confidence"):
                ui.label(
                    f"Merge confidence: {raw['merge_confidence']}"
                ).classes("text-xs text-gray-400")

            # ── Event-level: date + duration ──────────────────────────────
            with ui.row().classes("w-full gap-4 flex-wrap items-start"):
                date_input = ui.date(
                    value=display_date, mask="YYYY-MM-DD"
                ).props("label='Event date'").classes("w-full md:w-[14rem]")
                dur_input = ui.number(
                    "Duration (min)", value=raw.get("approx_duration"), min=0, step=1
                ).classes("w-full md:w-[10rem]")

            # ── Interactions (objective data) ─────────────────────────────
            ui.label("Interactions (objective)").classes("font-medium text-sm mt-3")
            ui.label(
                "Each interaction captures the participants, positions, places, "
                "sex types, and orgasms for one pairing within this event."
            ).classes("text-xs text-gray-500")

            # We render one collapsible section per interaction
            intr_editors: list[dict] = []  # parallel list of live widget refs

            for intr in interactions:
                iid = int(intr["interaction_id"])
                participants = intr.get("participant_ids") or []
                orgasms_raw  = intr.get("orgasms") or {}       # {person_id: count}
                pos_ids      = intr.get("position_ids") or []
                place_ids    = intr.get("place_ids") or []
                st_ids       = intr.get("sex_type_ids") or []

                participant_names = ", ".join(
                    person_map.get(pid, f"#{pid}") for pid in sorted(participants)
                ) or "(no participants)"

                with ui.expansion(
                    f"Interaction #{iid}  ·  {participant_names}",
                    icon="people",
                ).classes("w-full border rounded"):
                    with ui.column().classes("w-full gap-2 p-2"):
                        ui.label("Participants").classes("text-xs text-gray-500")
                        intr_people_sel = ui.select(
                            person_map, label="Participants",
                            value=participants, multiple=True,
                        ).props("use-chips clearable").classes("w-full")

                        # Orgasm inputs per participant
                        ui.label("Orgasms per person").classes("text-xs text-gray-500 mt-1")
                        orgasm_inputs_intr: dict[int, object] = {}
                        orgasm_container_intr = ui.column().classes("w-full gap-1")

                        def render_orgasm_inputs_intr(
                            container=orgasm_container_intr,
                            people_sel=intr_people_sel,
                            existing_orgasms=orgasms_raw,
                            inp_dict=orgasm_inputs_intr,
                        ) -> None:
                            container.clear()
                            inp_dict.clear()
                            with container:
                                pids = [int(v) for v in (people_sel.value or [])]
                                if not pids:
                                    ui.label("(no participants)").classes("text-xs text-gray-400")
                                    return
                                with ui.row().classes("w-full gap-4 flex-wrap"):
                                    for pid in pids:
                                        name = person_map.get(pid, str(pid))
                                        val  = existing_orgasms.get(pid) or existing_orgasms.get(str(pid))
                                        inp_dict[pid] = ui.number(
                                            label=name, value=val, min=0, step=1
                                        ).classes("w-[9rem]")

                        intr_people_sel.on_value_change(
                            lambda _, c=orgasm_container_intr, s=intr_people_sel,
                            o=orgasms_raw, d=orgasm_inputs_intr:
                                render_orgasm_inputs_intr(c, s, o, d)
                        )
                        render_orgasm_inputs_intr()

                        ui.label("Positions").classes("text-xs text-gray-500 mt-1")
                        intr_positions_sel = ui.select(
                            position_map, label="Positions",
                            value=pos_ids, multiple=True,
                        ).props("use-chips clearable").classes("w-full")

                        ui.label("Places").classes("text-xs text-gray-500 mt-1")
                        intr_places_sel = ui.select(
                            place_map, label="Places",
                            value=place_ids, multiple=True,
                        ).props("use-chips clearable").classes("w-full")

                        ui.label("Sex types").classes("text-xs text-gray-500 mt-1")
                        intr_st_sel = ui.select(
                            SEX_TYPE_MAPPING, label="Sex types",
                            value=st_ids, multiple=True,
                        ).props("use-chips clearable").classes("w-full")

                intr_editors.append({
                    "interaction_id": iid,
                    "people_sel":     intr_people_sel,
                    "orgasm_inputs":  orgasm_inputs_intr,
                    "positions_sel":  intr_positions_sel,
                    "places_sel":     intr_places_sel,
                    "st_sel":         intr_st_sel,
                })

            # ── Reports (subjective data) ─────────────────────────────────
            ui.label("Reports (subjective — one per source)").classes("font-medium text-sm mt-3")
            report_editors: list[dict] = []

            for rpt in reports:
                rid           = int(rpt["report_id"])
                reporter_pid  = int(rpt.get("reporter_person_id") or 0)
                reporter_name = person_map.get(reporter_pid, f"Reporter #{reporter_pid}")
                init_val      = rpt.get("initiator")
                sstat_val     = rpt.get("safety_status")

                # Build initiator choices from this report's linked interaction
                # participants — much more meaningful than generic "Me / My Partner".
                linked_iid = rpt.get("interaction_id")
                linked_intr = next(
                    (i for i in interactions if i["interaction_id"] == linked_iid),
                    None,
                )
                intr_pids = sorted(linked_intr["participant_ids"]) if linked_intr else []
                intr_names = [person_map.get(pid, f"Person #{pid}") for pid in intr_pids]

                # 0 = Spontaneously, 1 = first person, 2 = second person, 3 = Both
                initiator_choices: dict[int, str] = {0: "Spontaneously"}
                if len(intr_names) >= 1:
                    initiator_choices[1] = intr_names[0]
                if len(intr_names) >= 2:
                    initiator_choices[2] = intr_names[1]
                initiator_choices[3] = "Both people"

                with ui.expansion(
                    f"Report #{rid}  ·  by {reporter_name}",
                    icon="edit_note",
                ).classes("w-full border rounded"):
                    with ui.column().classes("w-full gap-2 p-2"):
                        with ui.row().classes("w-full gap-4 flex-wrap items-start"):
                            rpt_rat  = ui.number(
                                "Rating (1–5)", value=rpt.get("rating"), min=1, max=5, step=1
                            ).classes("w-full md:w-[9rem]")
                            rpt_init = ui.select(
                                initiator_choices, label="Initiator",
                                value=int(init_val) if init_val is not None else None,
                            ).classes("w-full md:w-[16rem]")
                        with ui.row().classes("w-full gap-4 flex-wrap items-center"):
                            rpt_protection = ui.checkbox(
                                "Used Protection",
                                value=bool(sstat_val),
                            )
                        rpt_note = ui.textarea(
                            f"Note ({reporter_name})",
                            value=str(rpt.get("note") or ""),
                        ).classes("w-full").props("rows=3")

                report_editors.append({
                    "report_id":  rid,
                    "rat":        rpt_rat,
                    "init":       rpt_init,
                    "protection": rpt_protection,
                    "note":       rpt_note,
                })

            # ── Save / cancel ─────────────────────────────────────────────
            save_status = ui.label("").classes("text-sm text-red-500 mt-1")

            async def save_event() -> None:
                try:
                    raw_d = str(date_input.value or "").strip()
                    try:
                        db_date = datetime.strptime(raw_d[:10], "%Y-%m-%d").strftime("%Y.%m.%d")
                    except ValueError:
                        save_status.set_text("Invalid date — use YYYY-MM-DD.")
                        return

                    event_dur = int(dur_input.value) if dur_input.value is not None else None

                    # Build reports payload (no duration — that's event-level now)
                    rpts_payload = []
                    for re in report_editors:
                        rpts_payload.append({
                            "report_id":     re["report_id"],
                            "rating":        int(re["rat"].value)  if re["rat"].value  is not None else None,
                            "note":          re["note"].value or None,
                            "initiator":     int(re["init"].value) if re["init"].value is not None else None,
                            "safety_status": 1 if re["protection"].value else 0,
                        })

                    # Build interactions payload
                    intrs_payload = []
                    for ie in intr_editors:
                        participant_ids = [int(v) for v in (ie["people_sel"].value or [])]
                        orgasms_out: dict[int, int] = {}
                        for pid, inp in ie["orgasm_inputs"].items():
                            if inp.value is not None:
                                orgasms_out[int(pid)] = int(inp.value)
                        intrs_payload.append({
                            "interaction_id":  ie["interaction_id"],
                            "participant_ids": participant_ids,
                            "orgasms":         orgasms_out,
                            "position_ids":    [int(v) for v in (ie["positions_sel"].value or [])],
                            "place_ids":       [int(v) for v in (ie["places_sel"].value or [])],
                            "sex_type_ids":    [int(v) for v in (ie["st_sel"].value or [])],
                        })

                    try:
                        backup_path = self.service.backup_db()
                        self._set_status(f"Backup saved: {backup_path.name}")
                    except Exception:
                        pass

                    self.service.update_event(
                        event_id,
                        date=db_date,
                        duration=event_dur,
                        reports=rpts_payload,
                        interactions=intrs_payload,
                    )
                    dialog.close()
                    self._set_status(f"Event #{event_id} saved.")
                    global _LAST_FILTERS
                    self.refresh_entries(_LAST_FILTERS or SearchFilters())
                except DataSourceError as exc:
                    save_status.set_text(str(exc))
                except Exception as exc:
                    import traceback, sys
                    print(traceback.format_exc(), file=sys.stderr)
                    save_status.set_text(f"Unexpected error: {exc}")

            with ui.row().classes("justify-end w-full gap-2 mt-2"):
                ui.button("Cancel",       on_click=dialog.close).props("flat")
                ui.button("Save Changes", on_click=save_event).props("color=primary")

        dialog.open()

    # ── Utility helpers ───────────────────────────────────────────────────────

    def _normalize_ui_date(self, value) -> str | None:
        if not value:
            return None
        if isinstance(value, str):
            try:
                return datetime.strptime(value[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
            except ValueError:
                return None
        if isinstance(value, dict):
            y, m, d = value.get("year"), value.get("month"), value.get("day")
            if y and m and d:
                try:
                    return datetime(int(y), int(m), int(d)).strftime("%Y-%m-%d")
                except ValueError:
                    return None
        return None

    def _render_milestone_list(self) -> None:
        if not (self.milestone_list_label and self.milestone_list_container):
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
        metrics   = self.service.summary_metrics(filters)
        by_person = self.service.summary_metrics_by_person(filters)
        self._metric_value_label(self.entries_metric).set_text(str(metrics["entries"]))
        for person, card in self.person_metric_cards.items():
            self._metric_value_label(card).set_text(str(by_person.get(person, 0)))

    def _set_status(self, message: str) -> None:
        if self.status_label:
            self.status_label.text = message
            self.status_label.update()

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

    # ── Chart rendering ───────────────────────────────────────────────────────

    def render_chart_specs(
        self, filters: SearchFilters, person_choices: dict[str, str]
    ) -> None:
        if self.chart_output_container is None:
            return
        self.chart_output_container.clear()
        with self.chart_output_container:
            for index, spec in enumerate(self.chart_specs, start=1):
                with ui.card().classes("w-full"):
                    title = {
                        "orgasms":            "Orgasms Over Time",
                        "ratings":            "Rating Distribution",
                        "duration":           "Duration Distribution by Person",
                        "anomaly":            "Orgasm Anomaly Detection",
                        "streaks":            "Sex Streaks",
                        "position_frequency": "Position Frequency",
                        "position_combos":    "Position Combinations",
                        "position_association": "Position Association Rules",
                        "position_upset":     "Position UpSet",
                        "location_room":      "Location/Room Links",
                        "year_in_review":     "Rendezvous Report",
                    }.get(str(spec.get("type")), str(spec.get("type")))
                    ui.label(f"Chart {index}: {title}").classes("text-lg")
                    people_ids  = spec.get("people", []) or []
                    people_text = ", ".join(
                        person_choices.get(str(pid), str(pid)) for pid in people_ids
                    ) or "All people"
                    datasets_text = (
                        "; datasets=" + ", ".join(d["label"] for d in spec["datasets"])
                        if spec.get("datasets") else ""
                    )
                    ui.label(
                        f"Parameters: people={people_text}; "
                        f"trend={spec.get('include_trend')}; "
                        f"trend_kind={spec.get('trend_kind')}{datasets_text}"
                    ).classes("text-sm text-gray-600")
                    fig = self._build_chart(filters, spec, person_choices)
                    ui.plotly(fig).classes("w-full").style("min-height: 26rem")

    def _build_chart(
        self,
        filters: SearchFilters,
        spec: dict[str, object],
        person_choices: dict[str, str],
    ) -> Figure:
        chart_type_val = spec.get("type")
        people_ids     = [int(v) for v in (spec.get("people") or [])]
        aliases        = spec.get("person_aliases") if isinstance(spec.get("person_aliases"), dict) else {}
        trend_kind_val = str(spec.get("trend_kind") or "rolling_30")
        rename_map     = {
            person_choices.get(str(pid), str(pid)): alias
            for pid, alias in aliases.items()
            if str(alias).strip()
        }

        def apply_aliases(df, col_name="person"):
            if df.empty or not rename_map:
                return df
            df = df.copy()
            if col_name in df.columns:
                df[col_name] = df[col_name].apply(lambda x: rename_map.get(str(x), str(x)))
            return df

        try:
            if chart_type_val == "orgasms":
                import pandas as pd
                spec_datasets  = spec.get("datasets") or []
                normalize_year = len(spec_datasets) >= 2

                if spec_datasets:
                    frames = []
                    for ds in spec_datasets:
                        ds_filters = SearchFilters(
                            start_date=datetime.strptime(ds["start_date"], "%Y-%m-%d").strftime("%Y.%m.%d"),
                            end_date=datetime.strptime(ds["end_date"], "%Y-%m-%d").strftime("%Y.%m.%d"),
                            note_keyword=filters.note_keyword,
                            person_ids=people_ids or None,
                            position_ids=filters.position_ids,
                            place_id=filters.place_id,
                        )
                        df = self.service.orgasms_by_person_timeseries(
                            ds_filters, people_ids, trend_kind=trend_kind_val
                        )
                        df = apply_aliases(df)
                        if df.empty:
                            continue
                        if normalize_year:
                            df = df.copy()
                            df["date"] = df["date"].apply(lambda d: d.replace(year=1904))
                            df = df.sort_values(["person", "date"])
                            if trend_kind_val == "loess":
                                from webapp.services import _loess_smooth
                                df["trend"] = df.groupby("person", group_keys=False).apply(
                                    lambda g: _loess_smooth(
                                        pd.Series(range(len(g)), index=g.index),
                                        g["orgasms"], frac=0.3,
                                    )
                                )
                            else:
                                df["trend"] = df.groupby("person")["orgasms"].transform(
                                    lambda s: s.rolling(window=30, min_periods=1).mean()
                                )
                        df["person"] = ds["label"] + " — " + df["person"]
                        frames.append(df)
                    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
                    subtitle = "Orgasms per session — " + ", ".join(d["label"] for d in spec_datasets)
                    if people_ids:
                        names = [
                            (aliases or {}).get(pid) or person_choices.get(str(pid), str(pid))
                            for pid in people_ids
                        ]
                        subtitle += " for: " + ", ".join(names)
                else:
                    combined = self.service.orgasms_by_person_timeseries(
                        filters, people_ids, trend_kind=trend_kind_val
                    )
                    combined = apply_aliases(combined)
                    subtitle = self._chart_subtitle(
                        "Orgasms per session", filters, people_ids, person_choices, aliases
                    )

                return partner_orgasms_chart(
                    combined,
                    milestones=self.milestones,
                    include_trend=bool(spec.get("include_trend", True)),
                    trend_kind=trend_kind_val,
                    subtitle=subtitle,
                    normalize_year=normalize_year,
                )

            if chart_type_val == "ratings":
                return rating_histogram_chart(
                    self.service.ratings_dataframe(filters),
                    subtitle=self._chart_subtitle("Rating distribution", filters, people_ids, person_choices, aliases),
                )

            if chart_type_val == "duration":
                df = apply_aliases(self.service.duration_by_partner_dataframe(filters), col_name="partners")
                return duration_violin_chart(
                    df,
                    subtitle=self._chart_subtitle("Session duration", filters, people_ids, person_choices, aliases),
                )

            if chart_type_val == "anomaly":
                df = apply_aliases(self.service.partner_orgasms_anomaly_dataframe(filters))
                return rolling_anomaly_chart(
                    df,
                    milestones=self.milestones,
                    subtitle=self._chart_subtitle("Orgasm anomaly detection", filters, people_ids, person_choices, aliases),
                )

            if chart_type_val == "streaks":
                return sex_streaks_chart(
                    self.service.sex_streaks_dataframe(filters),
                    milestones=self.milestones,
                    subtitle=self._chart_subtitle("Sex streaks", filters, people_ids, person_choices, aliases),
                )

            if chart_type_val == "position_frequency":
                return position_frequency_chart(
                    self.service.position_frequency_dataframe(filters, require_people=people_ids),
                    subtitle=self._chart_subtitle("Position frequency", filters, people_ids, person_choices, aliases),
                )

            if chart_type_val == "position_combos":
                return position_combinations_chart(
                    self.service.position_combinations_dataframe(filters, require_people=people_ids),
                    subtitle=self._chart_subtitle("Position combinations", filters, people_ids, person_choices, aliases),
                )

            if chart_type_val == "position_association":
                return position_association_chart(
                    self.service.position_association_rules_dataframe(filters, require_people=people_ids),
                    subtitle=self._chart_subtitle("Position association rules", filters, people_ids, person_choices, aliases),
                )

            if chart_type_val == "position_upset":
                return position_upset_chart(
                    self.service.position_upset_dataframe(filters, require_people=people_ids),
                    filters.start_date, filters.end_date,
                    subtitle=self._chart_subtitle("Position UpSet", filters, people_ids, person_choices, aliases),
                )

            if chart_type_val == "location_room":
                return location_room_sankey_chart(
                    self.service.location_room_sankey_dataframe(filters),
                    subtitle=self._chart_subtitle("Location/Room links", filters, people_ids, person_choices, aliases),
                )

            if chart_type_val == "year_in_review":
                stats = self.service.year_in_review(filters, person_ids=people_ids or None)
                return year_in_review_chart(
                    stats,
                    subtitle=self._chart_subtitle("Rendezvous Report", filters, people_ids, person_choices, aliases),
                    custom_title=str(spec.get("custom_title") or "").strip() or None,
                    rename_map=rename_map,
                )

        except Exception as exc:
            import traceback, sys
            print(traceback.format_exc(), file=sys.stderr, flush=True)
            self._set_status(f"Chart build failed: {exc}")

        fig = Figure()
        fig.update_layout(title="Chart unavailable")
        return fig

    # ── Refresh / data loading ────────────────────────────────────────────────

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
            self._set_status(f"Loaded {len(rows)} event(s).")
        except DataSourceError as exc:
            self.table.rows = []
            self.table.update()
            self._set_status(str(exc))

    def refresh_charts(self, filters: SearchFilters) -> None:
        options = {str(pid): name for pid, name in self.service.people_options()}
        self.render_chart_specs(filters, options)

    async def refresh_charts_async(self, filters: SearchFilters) -> None:
        self.refresh_charts(filters)
        await asyncio.sleep(0)

    # ── Export ────────────────────────────────────────────────────────────────

    def export_csv(self, filters: SearchFilters) -> None:
        try:
            path = self.service.export_entries_csv(filters)
            ui.download(str(path), filename="entries_export.csv")
            self._set_status(f"CSV export ready: {path}")
        except DataSourceError as exc:
            self._set_status(str(exc))

    def export_report_json(self, filters: SearchFilters) -> None:
        try:
            path = self.service.export_report_json(filters)
            ui.download(str(path), filename="report_export.json")
            self._set_status(f"Report JSON export ready: {path}")
        except DataSourceError as exc:
            self._set_status(str(exc))

    def export_chart_png(self, filters: SearchFilters) -> None:
        try:
            df  = self.service.partner_orgasms_timeseries(filters)
            fig = partner_orgasms_chart(df, milestones=self.milestones)
            png_path = self.service.temp_export_path("partner_orgasms_chart_", ".png")
            fig.write_image(str(png_path), format="png", engine="kaleido")
            ui.download(str(png_path), filename="partner_orgasms_chart.png")
            self._set_status(f"Chart PNG export ready: {png_path}")
        except DataSourceError as exc:
            self._set_status(str(exc))
        except Exception as exc:
            self._set_status(f"Chart export failed: {exc}")

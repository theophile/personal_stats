from __future__ import annotations

from dataclasses import dataclass
import pandas as pd
from pathlib import Path
import sqlite3
import tempfile
import csv
import json
from collections import Counter

from webapp.db import ReadOnlyDatabase


PLACE_MAPPING = {
    0: "Bedroom",
    1: "Kitchen",
    2: "Shower",
    3: "Restroom",
    4: "Living Room",
    5: "Garage",
    6: "Backyard",
    7: "Roof",
    8: "Jacuzzi",
    9: "Pool",
    10: "Beach",
    11: "Home",
    12: "Hotel",
    13: "Lifestyle Club",
    14: "Cinema",
    15: "Theatre",
    16: "School",
    17: "Museum",
    18: "Car",
    19: "Plane",
    20: "Train",
    21: "Ship",
    22: "Public",
}
ROOM_IDS = set(range(0, 10))
LOCATION_IDS = set(range(10, 23))


class DataSourceError(RuntimeError):
    """Raised when the configured SQLite file is missing expected tables."""


@dataclass
class SearchFilters:
    start_date: str | None = None
    end_date: str | None = None
    note_keyword: str | None = None
    partner_id: int | None = None
    position_id: int | None = None
    place_id: int | None = None


class StatsService:
    def __init__(self, db: ReadOnlyDatabase):
        self.db = db

    def list_tables(self) -> list[str]:
        try:
            with self.db.cursor() as cur:
                rows = cur.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
                ).fetchall()
            return [str(row[0]) for row in rows]
        except FileNotFoundError as exc:
            raise DataSourceError(str(exc)) from exc
        except sqlite3.Error as exc:
            raise DataSourceError(f"Could not inspect database tables: {exc}") from exc

    def ensure_expected_schema(self) -> None:
        expected = {
            "entries",
            "entry_partner",
            "entry_position",
            "entry_place",
            "partners",
            "positions",
        }
        available = set(self.list_tables())
        missing = sorted(expected - available)
        if missing:
            raise DataSourceError(
                "The configured database does not include required table(s): "
                f"{', '.join(missing)}. Available tables: {', '.join(sorted(available)) or '(none)'}"
            )

    def _fetch_id_name_map(self, table: str) -> dict[int, str]:
        with self.db.cursor() as cur:
            rows = cur.execute(f"SELECT * FROM {table}").fetchall()
        return {int(row[0]): str(row[2]) for row in rows}

    def partner_options(self) -> list[tuple[int, str]]:
        self.ensure_expected_schema()
        mapping = self._fetch_id_name_map("partners")
        return sorted(mapping.items(), key=lambda x: x[1].lower())

    def position_options(self) -> list[tuple[int, str]]:
        self.ensure_expected_schema()
        mapping = self._fetch_id_name_map("positions")
        return sorted(mapping.items(), key=lambda x: x[1].lower())

    def place_options(self) -> list[tuple[int, str]]:
        self.ensure_expected_schema()
        return sorted(PLACE_MAPPING.items(), key=lambda x: x[1].lower())

    def _build_where_clause(self, filters: SearchFilters) -> tuple[str, list[object]]:
        clauses = []
        params: list[object] = []

        if filters.start_date:
            clauses.append("e.date >= ?")
            params.append(filters.start_date)
        if filters.end_date:
            clauses.append("e.date <= ?")
            params.append(filters.end_date)
        if filters.note_keyword:
            clauses.append("LOWER(COALESCE(e.note, '')) LIKE ?")
            params.append(f"%{filters.note_keyword.lower()}%")
        if filters.partner_id is not None:
            clauses.append(
                "EXISTS (SELECT 1 FROM entry_partner ep WHERE ep.entry_id = e.entry_id AND ep.partner_id = ?)"
            )
            params.append(filters.partner_id)
        if filters.position_id is not None:
            clauses.append(
                "EXISTS (SELECT 1 FROM entry_position epo WHERE epo.entry_id = e.entry_id AND epo.position_id = ?)"
            )
            params.append(filters.position_id)
        if filters.place_id is not None:
            clauses.append(
                "EXISTS (SELECT 1 FROM entry_place epl WHERE epl.entry_id = e.entry_id AND epl.place_id = ?)"
            )
            params.append(filters.place_id)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return where, params

    def search_entries(self, filters: SearchFilters, limit: int = 300) -> list[dict]:
        self.ensure_expected_schema()
        where, params = self._build_where_clause(filters)

        query = f"""
            SELECT e.entry_id, e.date, e.duration, e.note, e.rating, e.initiator,
                   e.safety_status, e.total_org, e.total_org_partner
            FROM entries e
            {where}
            ORDER BY e.date DESC
            LIMIT ?
        """
        params.append(limit)

        try:
            with self.db.cursor() as cur:
                entries = [dict(row) for row in cur.execute(query, params).fetchall()]

                partner_map = self._fetch_id_name_map("partners")
                position_map = self._fetch_id_name_map("positions")

                for entry in entries:
                    entry_id = entry["entry_id"]
                    partner_ids = [
                        int(r[0])
                        for r in cur.execute(
                            "SELECT partner_id FROM entry_partner WHERE entry_id = ?", (entry_id,)
                        ).fetchall()
                    ]
                    position_ids = [
                        int(r[0])
                        for r in cur.execute(
                            "SELECT position_id FROM entry_position WHERE entry_id = ?", (entry_id,)
                        ).fetchall()
                    ]
                    place_ids = [
                        int(r[0])
                        for r in cur.execute(
                            "SELECT place_id FROM entry_place WHERE entry_id = ?", (entry_id,)
                        ).fetchall()
                    ]

                    entry["partners"] = ", ".join(partner_map.get(i, f"Unknown({i})") for i in partner_ids)
                    entry["positions"] = ", ".join(position_map.get(i, f"Unknown({i})") for i in position_ids)
                    entry["places"] = ", ".join(PLACE_MAPPING.get(i, f"Unknown({i})") for i in place_ids)

            return entries
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to query entries: {exc}") from exc

    def _entry_ids_for_filters(self, filters: SearchFilters) -> list[int]:
        self.ensure_expected_schema()
        where, params = self._build_where_clause(filters)
        query = f"SELECT e.entry_id FROM entries e {where} ORDER BY e.date"
        try:
            with self.db.cursor() as cur:
                rows = cur.execute(query, params).fetchall()
            return [int(r[0]) for r in rows]
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to query filtered entry IDs: {exc}") from exc

    def partner_orgasms_timeseries(self, filters: SearchFilters):
        self.ensure_expected_schema()
        where, params = self._build_where_clause(filters)
        query = f"SELECT e.date, e.total_org_partner FROM entries e {where} ORDER BY e.date"

        try:
            with self.db.cursor() as cur:
                rows = cur.execute(query, params).fetchall()
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to query time series data: {exc}") from exc

        df = pd.DataFrame(rows, columns=["date", "total_org_partner"])
        if df.empty:
            return df

        df["date"] = pd.to_datetime(df["date"], format="%Y.%m.%d")
        daily = df.groupby("date", as_index=False)["total_org_partner"].sum()
        daily["trend"] = daily["total_org_partner"].rolling(window=30, min_periods=1).mean()
        return daily

    def ratings_dataframe(self, filters: SearchFilters):
        rows = self.search_entries(filters, limit=100000)
        return pd.DataFrame(rows)

    def sex_streaks_dataframe(self, filters: SearchFilters):
        self.ensure_expected_schema()
        where, params = self._build_where_clause(filters)
        query = f"SELECT DISTINCT e.date FROM entries e {where} ORDER BY e.date"
        try:
            with self.db.cursor() as cur:
                rows = cur.execute(query, params).fetchall()
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to query streak data: {exc}") from exc

        df = pd.DataFrame(rows, columns=["date"])
        if df.empty:
            return pd.DataFrame(columns=["start_date", "length", "signed_length", "type"])

        df["date"] = pd.to_datetime(df["date"], format="%Y.%m.%d")
        full_range = pd.date_range(start=df["date"].min(), end=df["date"].max(), freq="D")
        points = pd.DataFrame({"date": full_range})
        points["sex_occurred"] = points["date"].isin(df["date"]).astype(int)

        streaks = []
        current_type = None
        current_start = None
        current_length = 0

        for _, row in points.iterrows():
            value = int(row["sex_occurred"])
            date = row["date"]
            if current_type is None:
                current_type, current_start, current_length = value, date, 1
            elif current_type == value:
                current_length += 1
            else:
                streaks.append((current_start, current_length, current_type))
                current_type, current_start, current_length = value, date, 1

        if current_type is not None:
            streaks.append((current_start, current_length, current_type))

        out = pd.DataFrame(streaks, columns=["start_date", "length", "type_flag"])
        out["start_date"] = out["start_date"].dt.strftime("%Y-%m-%d")
        out["type"] = out["type_flag"].map({1: "sex", 0: "no_sex"})
        out["signed_length"] = out.apply(
            lambda r: int(r["length"]) if r["type_flag"] == 1 else -int(r["length"]), axis=1
        )
        return out[["start_date", "length", "signed_length", "type"]]

    def position_frequency_dataframe(self, filters: SearchFilters):
        self.ensure_expected_schema()
        entry_ids = self._entry_ids_for_filters(filters)
        if not entry_ids:
            return pd.DataFrame(columns=["position", "count"])

        try:
            with self.db.cursor() as cur:
                position_map = self._fetch_id_name_map("positions")
                counts: Counter[str] = Counter()
                for entry_id in entry_ids:
                    position_ids = [
                        int(r[0])
                        for r in cur.execute(
                            "SELECT position_id FROM entry_position WHERE entry_id = ?", (entry_id,)
                        ).fetchall()
                    ]
                    for pid in position_ids:
                        counts[position_map.get(pid, f"Unknown({pid})")] += 1
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to query position frequency data: {exc}") from exc

        return pd.DataFrame(
            [{"position": name, "count": count} for name, count in counts.items()]
        )

    def position_combinations_dataframe(self, filters: SearchFilters):
        self.ensure_expected_schema()
        entry_ids = self._entry_ids_for_filters(filters)
        if not entry_ids:
            return pd.DataFrame(columns=["combination", "count"])

        try:
            with self.db.cursor() as cur:
                position_map = self._fetch_id_name_map("positions")
                combo_counter: Counter[str] = Counter()
                for entry_id in entry_ids:
                    position_ids = sorted(
                        {
                            int(r[0])
                            for r in cur.execute(
                                "SELECT position_id FROM entry_position WHERE entry_id = ?", (entry_id,)
                            ).fetchall()
                        }
                    )
                    if not position_ids:
                        continue
                    label = " + ".join(position_map.get(pid, f"Unknown({pid})") for pid in position_ids)
                    combo_counter[label] += 1
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to query position combination data: {exc}") from exc

        return pd.DataFrame(
            [{"combination": name, "count": count} for name, count in combo_counter.items()]
        )


    def position_upset_dataframe(self, filters: SearchFilters, max_positions: int = 6, min_instances: int = 1):
        self.ensure_expected_schema()
        entry_ids = self._entry_ids_for_filters(filters)
        if not entry_ids:
            return pd.DataFrame()

        if max_positions < 1:
            max_positions = 1
        if min_instances < 1:
            min_instances = 1

        try:
            with self.db.cursor() as cur:
                position_map = self._fetch_id_name_map("positions")

                position_counter: Counter[int] = Counter()
                combinations: list[list[int]] = []

                for entry_id in entry_ids:
                    position_ids = sorted(
                        {
                            int(r[0])
                            for r in cur.execute(
                                "SELECT position_id FROM entry_position WHERE entry_id = ?", (entry_id,)
                            ).fetchall()
                        }
                    )
                    if not position_ids:
                        continue
                    combinations.append(position_ids)
                    position_counter.update(position_ids)
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to query upset data: {exc}") from exc

        top_position_ids = [pid for pid, _ in position_counter.most_common(max_positions)]
        if not top_position_ids:
            return pd.DataFrame()

        combo_counter: Counter[tuple[int, ...]] = Counter()
        for position_ids in combinations:
            kept = tuple(pid for pid in top_position_ids if pid in position_ids)
            if not kept:
                continue
            combo_counter[kept] += 1

        filtered_combos = [combo for combo, count in combo_counter.items() if count >= min_instances]
        if not filtered_combos:
            return pd.DataFrame()

        top_position_names = [position_map.get(pid, f"Unknown({pid})") for pid in top_position_ids]
        rows: list[dict[str, int]] = []
        for combo in filtered_combos:
            combo_set = set(combo)
            row = {
                position_map.get(pid, f"Unknown({pid})"): (1 if pid in combo_set else 0)
                for pid in top_position_ids
            }
            rows.append(row)

        return pd.DataFrame(rows, columns=top_position_names)

    def location_room_sankey_dataframe(self, filters: SearchFilters):
        self.ensure_expected_schema()
        entry_ids = self._entry_ids_for_filters(filters)
        if not entry_ids:
            return pd.DataFrame(columns=["location", "room", "count"])

        counter: Counter[tuple[str, str]] = Counter()
        try:
            with self.db.cursor() as cur:
                for entry_id in entry_ids:
                    place_ids = [
                        int(r[0])
                        for r in cur.execute(
                            "SELECT place_id FROM entry_place WHERE entry_id = ?", (entry_id,)
                        ).fetchall()
                    ]
                    locations = [PLACE_MAPPING[p] for p in place_ids if p in LOCATION_IDS and p in PLACE_MAPPING]
                    rooms = [PLACE_MAPPING[p] for p in place_ids if p in ROOM_IDS and p in PLACE_MAPPING]
                    for location in locations:
                        for room in rooms:
                            counter[(location, room)] += 1
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to query location-room data: {exc}") from exc

        return pd.DataFrame(
            [
                {"location": location, "room": room, "count": count}
                for (location, room), count in counter.items()
            ]
        )

    def summary_metrics(self, filters: SearchFilters) -> dict[str, int]:
        rows = self.search_entries(filters, limit=100000)
        return {
            "entries": len(rows),
            "total_partner_orgasms": sum(int(r.get("total_org_partner") or 0) for r in rows),
            "total_my_orgasms": sum(int(r.get("total_org") or 0) for r in rows),
        }

    def build_report(self, filters: SearchFilters, top_n: int = 5) -> dict:
        rows = self.search_entries(filters, limit=100000)
        metrics = self.summary_metrics(filters)

        partner_counter: Counter[str] = Counter()
        position_counter: Counter[str] = Counter()
        place_counter: Counter[str] = Counter()

        for row in rows:
            for key, counter in (("partners", partner_counter), ("positions", position_counter), ("places", place_counter)):
                raw = row.get(key) or ""
                for item in [s.strip() for s in str(raw).split(",") if s.strip()]:
                    counter[item] += 1

        dates = [r.get("date") for r in rows if r.get("date")]

        return {
            "filters": filters.__dict__,
            "metrics": metrics,
            "date_range": {
                "min": min(dates) if dates else None,
                "max": max(dates) if dates else None,
            },
            "top_partners": [{"name": n, "count": c} for n, c in partner_counter.most_common(top_n)],
            "top_positions": [{"name": n, "count": c} for n, c in position_counter.most_common(top_n)],
            "top_places": [{"name": n, "count": c} for n, c in place_counter.most_common(top_n)],
            "chart_summaries": {
                "sex_streak_segments": int(len(self.sex_streaks_dataframe(filters))),
                "distinct_positions": int(len(self.position_frequency_dataframe(filters))),
                "distinct_position_combinations": int(len(self.position_combinations_dataframe(filters))),
                "upset_combinations": int(len(self.position_upset_dataframe(filters))),
                "location_room_links": int(len(self.location_room_sankey_dataframe(filters))),
            },
        }

    def export_report_json(self, filters: SearchFilters) -> Path:
        report = self.build_report(filters)
        path = self.temp_export_path("report_export_", ".json")
        with path.open("w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        return path

    def export_entries_csv(self, filters: SearchFilters) -> Path:
        rows = self.search_entries(filters, limit=100000)
        tmp_path = self.temp_export_path("entries_export_", ".csv")

        if not rows:
            with tmp_path.open("w", newline="", encoding="utf-8") as f:
                f.write("\n")
            return tmp_path

        fieldnames = list(rows[0].keys())
        with tmp_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return tmp_path

    def temp_export_path(self, prefix: str, suffix: str) -> Path:
        tmp = tempfile.NamedTemporaryFile(prefix=prefix, suffix=suffix, delete=False)
        tmp_path = Path(tmp.name)
        tmp.close()
        return tmp_path

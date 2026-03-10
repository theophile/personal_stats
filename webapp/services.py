from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3
import tempfile
import csv
import json
from collections import Counter

try:
    import pandas as pd
except ModuleNotFoundError:
    pd = None

from webapp.db import ReadOnlyDatabase, WritableDatabase


# ── LOESS smoother (unchanged) ────────────────────────────────────────────────

def _loess_smooth(x: "pd.Series", y: "pd.Series", frac: float = 0.3) -> "pd.Series":
    import numpy as np
    x_arr = x.to_numpy(dtype=float)
    y_arr = y.to_numpy(dtype=float)
    n = len(x_arr)
    smoothed = np.empty(n)
    half_window = max(int(np.ceil(frac * n)), 2)
    for i in range(n):
        dists = np.abs(x_arr - x_arr[i])
        idx = np.argsort(dists)[:half_window]
        h = dists[idx[-1]] or 1.0
        u = dists[idx] / h
        w = (1.0 - u ** 3) ** 3
        xi = x_arr[idx]
        yi = y_arr[idx]
        W = np.diag(w)
        X = np.column_stack([np.ones(len(xi)), xi])
        try:
            XtW = X.T @ W
            coef = np.linalg.solve(XtW @ X, XtW @ yi)
            smoothed[i] = coef[0] + coef[1] * x_arr[i]
        except np.linalg.LinAlgError:
            smoothed[i] = np.average(yi, weights=w)
    return pd.Series(smoothed, index=y.index)


# ── Lookup tables ─────────────────────────────────────────────────────────────

PLACE_MAPPING = {
    0: "Bedroom", 1: "Kitchen", 2: "Shower", 3: "Restroom", 4: "Living Room",
    5: "Garage", 6: "Backyard", 7: "Roof", 8: "Jacuzzi", 9: "Pool",
    10: "Beach", 11: "Home", 12: "Hotel", 13: "Lifestyle Club", 14: "Cinema",
    15: "Theatre", 16: "School", 17: "Museum", 18: "Car", 19: "Plane",
    20: "Train", 21: "Ship", 22: "Public",
}
ROOM_IDS = set(range(0, 10))
LOCATION_IDS = set(range(10, 23))

SEX_TYPE_MAPPING = {
    0: "Vaginal", 1: "Oral", 2: "Handjob", 3: "Masturbation", 4: "Finger",
    5: "Toy", 6: "Anal", 7: "Group", 8: "Active", 9: "Passive", 10: "BDSM",
}

INITIATOR_MAPPING = {
    0: "Spontaneously", 1: "Me", 2: "My Partner", 3: "Both of Us",
}


class DataSourceError(RuntimeError):
    """Raised when the configured SQLite file is missing expected tables."""


@dataclass
class SearchFilters:
    start_date: str | None = None
    end_date: str | None = None
    note_keyword: str | None = None
    person_ids: list[int] | None = None
    position_ids: list[int] | None = None
    place_id: int | None = None


# ═════════════════════════════════════════════════════════════════════════════
# StatsService
# ═════════════════════════════════════════════════════════════════════════════

class StatsService:
    def __init__(self, db: ReadOnlyDatabase):
        self.db = db

    # ── Schema helpers ────────────────────────────────────────────────────────

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
            "events",
            "event_reports",
            "interactions",
            "interaction_participants",
            "interaction_orgasms",
            "interaction_positions",
            "interaction_places",
            "interaction_sex_types",
            "people",
            "canonical_positions",
        }
        available = set(self.list_tables())
        missing = sorted(expected - available)
        if missing:
            raise DataSourceError(
                "The configured database does not include required table(s): "
                f"{', '.join(missing)}. "
                f"Available tables: {', '.join(sorted(available)) or '(none)'}"
            )

    def _fetch_id_name_map(self, table: str) -> dict[int, str]:
        table_sql = {
            "partners": "people",
            "positions": "canonical_positions",
        }.get(table, table)
        with self.db.cursor() as cur:
            rows = cur.execute(f"SELECT * FROM {table_sql}").fetchall()
        return {int(row[0]): str(row[1]) for row in rows}

    def _require_pandas(self) -> None:
        if pd is None:
            raise DataSourceError("pandas is required for dataframe/chart operations")

    # ── Lookup helpers ────────────────────────────────────────────────────────

    def people_options(self) -> list[tuple[int, str]]:
        self.ensure_expected_schema()
        return sorted(self._fetch_id_name_map("partners").items(), key=lambda x: x[1].lower())

    def partner_options(self) -> list[tuple[int, str]]:
        return self.people_options()

    def person_name_map(self) -> dict[int, str]:
        self.ensure_expected_schema()
        return self._fetch_id_name_map("partners")

    def position_options(self) -> list[tuple[int, str]]:
        self.ensure_expected_schema()
        return sorted(self._fetch_id_name_map("positions").items(), key=lambda x: x[1].lower())

    def place_options(self) -> list[tuple[int, str]]:
        self.ensure_expected_schema()
        return sorted(PLACE_MAPPING.items(), key=lambda x: x[1].lower())

    # ── Per-interaction data helpers ──────────────────────────────────────────

    def _participants_for_interaction(
        self, cur: sqlite3.Cursor, interaction_id: int
    ) -> list[int]:
        return [
            int(r[0])
            for r in cur.execute(
                "SELECT person_id FROM interaction_participants WHERE interaction_id = ?",
                (interaction_id,),
            ).fetchall()
        ]

    def _orgasms_for_interaction(
        self, cur: sqlite3.Cursor, interaction_id: int
    ) -> dict[int, int]:
        """Returns {person_id: count}."""
        return {
            int(r[0]): int(r[1])
            for r in cur.execute(
                "SELECT person_id, count FROM interaction_orgasms WHERE interaction_id = ?",
                (interaction_id,),
            ).fetchall()
        }

    def _positions_for_interaction(
        self, cur: sqlite3.Cursor, interaction_id: int
    ) -> list[int]:
        return [
            int(r[0])
            for r in cur.execute(
                "SELECT canonical_position_id FROM interaction_positions "
                "WHERE interaction_id = ?",
                (interaction_id,),
            ).fetchall()
        ]

    def _places_for_interaction(
        self, cur: sqlite3.Cursor, interaction_id: int
    ) -> list[int]:
        return [
            int(r[0])
            for r in cur.execute(
                "SELECT place_id FROM interaction_places WHERE interaction_id = ?",
                (interaction_id,),
            ).fetchall()
        ]

    def _sex_types_for_interaction(
        self, cur: sqlite3.Cursor, interaction_id: int
    ) -> list[int]:
        return [
            int(r[0])
            for r in cur.execute(
                "SELECT sex_type_id FROM interaction_sex_types WHERE interaction_id = ?",
                (interaction_id,),
            ).fetchall()
        ]

    def _interactions_for_event(
        self, cur: sqlite3.Cursor, event_id: int
    ) -> list[int]:
        return [
            int(r[0])
            for r in cur.execute(
                "SELECT interaction_id FROM interactions WHERE event_id = ?",
                (event_id,),
            ).fetchall()
        ]

    # ── Aggregate helpers across all interactions for an event ────────────────

    def _event_person_orgasms(
        self, cur: sqlite3.Cursor, event_id: int, person_map: dict[int, str]
    ) -> dict[str, int]:
        """Sum orgasms across all interactions of this event, keyed by person name."""
        totals: dict[str, int] = {name: 0 for name in person_map.values()}
        for iid in self._interactions_for_event(cur, event_id):
            for pid, count in self._orgasms_for_interaction(cur, iid).items():
                name = person_map.get(pid)
                if name:
                    totals[name] = totals.get(name, 0) + count
        return totals

    def _event_participants(
        self, cur: sqlite3.Cursor, event_id: int
    ) -> set[int]:
        """All people present at an event (union over all interactions)."""
        result: set[int] = set()
        for iid in self._interactions_for_event(cur, event_id):
            result.update(self._participants_for_interaction(cur, iid))
        return result

    def _event_positions(
        self, cur: sqlite3.Cursor, event_id: int
    ) -> list[int]:
        seen: set[int] = set()
        result: list[int] = []
        for iid in self._interactions_for_event(cur, event_id):
            for pid in self._positions_for_interaction(cur, iid):
                if pid not in seen:
                    seen.add(pid)
                    result.append(pid)
        return result

    def _event_places(self, cur: sqlite3.Cursor, event_id: int) -> list[int]:
        seen: set[int] = set()
        result: list[int] = []
        for iid in self._interactions_for_event(cur, event_id):
            for pid in self._places_for_interaction(cur, iid):
                if pid not in seen:
                    seen.add(pid)
                    result.append(pid)
        return result

    def _event_sex_types(self, cur: sqlite3.Cursor, event_id: int) -> list[int]:
        seen: set[int] = set()
        result: list[int] = []
        for iid in self._interactions_for_event(cur, event_id):
            for sid in self._sex_types_for_interaction(cur, iid):
                if sid not in seen:
                    seen.add(sid)
                    result.append(sid)
        return result

    # ── WHERE clause builder ──────────────────────────────────────────────────

    def _build_where_clause(self, filters: SearchFilters) -> tuple[str, list[object]]:
        clauses: list[str] = []
        params: list[object] = []

        if filters.start_date:
            clauses.append("ev.event_date >= ?")
            params.append(filters.start_date)
        if filters.end_date:
            clauses.append("ev.event_date <= ?")
            params.append(filters.end_date)
        if filters.note_keyword:
            # Match against any report's note for this event
            clauses.append(
                "EXISTS (SELECT 1 FROM event_reports er2 "
                "WHERE er2.event_id = ev.event_id "
                "AND LOWER(COALESCE(er2.note, '')) LIKE ?)"
            )
            params.append(f"%{filters.note_keyword.lower()}%")
        if filters.person_ids:
            placeholders = ", ".join("?" for _ in filters.person_ids)
            clauses.append(
                "EXISTS (SELECT 1 FROM interaction_participants ip "
                "JOIN interactions i ON i.interaction_id = ip.interaction_id "
                f"WHERE i.event_id = ev.event_id AND ip.person_id IN ({placeholders}))"
            )
            params.extend(filters.person_ids)
        if filters.position_ids:
            placeholders = ", ".join("?" for _ in filters.position_ids)
            clauses.append(
                "EXISTS (SELECT 1 FROM interaction_positions ipos "
                "JOIN interactions i ON i.interaction_id = ipos.interaction_id "
                f"WHERE i.event_id = ev.event_id "
                f"AND ipos.canonical_position_id IN ({placeholders}))"
            )
            params.extend(filters.position_ids)
        if filters.place_id is not None:
            clauses.append(
                "EXISTS (SELECT 1 FROM interaction_places ipl "
                "JOIN interactions i ON i.interaction_id = ipl.interaction_id "
                "WHERE i.event_id = ev.event_id AND ipl.place_id = ?)"
            )
            params.append(filters.place_id)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return where, params

    # ── Core search — events, not reports ─────────────────────────────────────

    def search_entries(self, filters: SearchFilters, limit: int = 300) -> list[dict]:
        """Return one row per *event* (not per report) matching the filters.

        Aggregates across all reports and interactions for each event so
        that multi-source merged events appear as a single table row.
        """
        self.ensure_expected_schema()
        where, params = self._build_where_clause(filters)

        # Use the earliest report's duration and the primary reporter as
        # representative fields for display.
        query = f"""
            SELECT
                ev.event_id,
                ev.event_date       AS date,
                ev.approx_duration  AS duration,
                ev.merge_confidence,
                (
                    SELECT er2.reporter_person_id
                    FROM event_reports er2
                    WHERE er2.event_id = ev.event_id
                    ORDER BY er2.report_id
                    LIMIT 1
                ) AS primary_reporter_id,
                (
                    SELECT er2.rating
                    FROM event_reports er2
                    WHERE er2.event_id = ev.event_id
                    ORDER BY er2.report_id
                    LIMIT 1
                ) AS rating,
                (
                    SELECT er2.note
                    FROM event_reports er2
                    WHERE er2.event_id = ev.event_id
                    ORDER BY er2.report_id
                    LIMIT 1
                ) AS note,
                (
                    SELECT er2.initiator
                    FROM event_reports er2
                    WHERE er2.event_id = ev.event_id
                    ORDER BY er2.report_id
                    LIMIT 1
                ) AS initiator,
                (
                    SELECT er2.safety_status
                    FROM event_reports er2
                    WHERE er2.event_id = ev.event_id
                    ORDER BY er2.report_id
                    LIMIT 1
                ) AS safety_status,
                ev.report_count
            FROM events ev
            {where}
            ORDER BY ev.event_date DESC
            LIMIT ?
        """
        params.append(limit)

        try:
            with self.db.cursor() as cur:
                raw_rows = [dict(row) for row in cur.execute(query, params).fetchall()]

                person_map   = self._fetch_id_name_map("partners")
                position_map = self._fetch_id_name_map("positions")

                result: list[dict] = []
                for row in raw_rows:
                    event_id = int(row["event_id"])

                    # Collect all participants across all interactions
                    participant_ids = self._event_participants(cur, event_id)
                    participants = sorted(
                        {person_map.get(pid, f"Unknown({pid})") for pid in participant_ids}
                    )

                    # Positions, places, sex types (union across interactions)
                    pos_ids  = self._event_positions(cur, event_id)
                    place_ids = self._event_places(cur, event_id)
                    st_ids   = self._event_sex_types(cur, event_id)

                    # Orgasms per person (sum across interactions)
                    person_orgasms = self._event_person_orgasms(cur, event_id, person_map)

                    # All reports for tooltip / edit access
                    report_ids = [
                        int(r[0])
                        for r in cur.execute(
                            "SELECT report_id FROM event_reports WHERE event_id = ? "
                            "ORDER BY report_id",
                            (event_id,),
                        ).fetchall()
                    ]

                    entry = dict(row)
                    entry["entry_id"]      = event_id   # alias for UI compatibility
                    entry["partners"]      = ", ".join(participants)
                    entry["positions"]     = ", ".join(
                        position_map.get(i, f"Unknown({i})") for i in pos_ids
                    )
                    entry["places"]        = ", ".join(
                        PLACE_MAPPING.get(i, f"Unknown({i})") for i in place_ids
                    )
                    entry["sex_types"]     = ", ".join(
                        SEX_TYPE_MAPPING.get(i, f"Unknown({i})") for i in st_ids
                    )
                    entry["sex_type_ids"]  = st_ids
                    entry["person_orgasms"] = person_orgasms
                    entry["report_ids"]    = report_ids
                    result.append(entry)

            return result
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to query entries: {exc}") from exc

    # ── Event ID lists for chart queries ──────────────────────────────────────

    def _event_ids_for_filters(self, filters: SearchFilters) -> list[int]:
        self.ensure_expected_schema()
        where, params = self._build_where_clause(filters)
        query = f"SELECT ev.event_id FROM events ev {where} ORDER BY ev.event_date"
        try:
            with self.db.cursor() as cur:
                rows = cur.execute(query, params).fetchall()
            return [int(r[0]) for r in rows]
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to query filtered event IDs: {exc}") from exc

    # Alias kept for callers that use the old name
    def _entry_ids_for_filters(self, filters: SearchFilters) -> list[int]:
        return self._event_ids_for_filters(filters)

    def _event_ids_for_all_people(
        self, filters: SearchFilters, person_ids: list[int]
    ) -> list[int]:
        """Return event IDs where ALL of person_ids are participants."""
        all_ids = self._event_ids_for_filters(filters)
        if not person_ids:
            return all_ids
        required = set(person_ids)
        out: list[int] = []
        try:
            with self.db.cursor() as cur:
                for eid in all_ids:
                    if required.issubset(self._event_participants(cur, eid)):
                        out.append(eid)
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to filter events by people: {exc}") from exc
        return out

    # Alias kept for callers that use the old name
    def _entry_ids_for_all_people(
        self, filters: SearchFilters, person_ids: list[int]
    ) -> list[int]:
        return self._event_ids_for_all_people(filters, person_ids)

    # ── Time-series data ──────────────────────────────────────────────────────

    def orgasms_by_person_timeseries(
        self,
        filters: SearchFilters,
        person_ids: list[int] | None = None,
        trend_kind: str = "rolling_30",
    ):
        self._require_pandas()
        rows = self.search_entries(filters, limit=100000)
        people_map = self.person_name_map()
        selected = set(person_ids or people_map.keys())
        inverse_map = {name: pid for pid, name in people_map.items()}

        records: list[dict] = []
        for row in rows:
            try:
                event_date = pd.to_datetime(str(row.get("date")), format="%Y.%m.%d")
            except Exception:
                continue
            orgasms = row.get("person_orgasms") or {}
            for person_name, count in orgasms.items():
                person_id = inverse_map.get(str(person_name))
                if person_id is None or person_id not in selected:
                    continue
                records.append({
                    "date": event_date, "person": person_name,
                    "orgasms": int(count or 0),
                })

        df = pd.DataFrame(records, columns=["date", "person", "orgasms"])
        if df.empty:
            return df

        daily = df.groupby(["date", "person"], as_index=False)["orgasms"].sum()
        daily = daily.sort_values(["person", "date"])
        if trend_kind == "loess":
            trend_parts = {}
            for person, group in daily.groupby("person"):
                x = pd.Series(range(len(group)), index=group.index)
                trend_parts[person] = _loess_smooth(x, group["orgasms"], frac=0.3)
            daily["trend"] = pd.concat(trend_parts.values())
        else:
            daily["trend"] = daily.groupby("person")["orgasms"].transform(
                lambda s: s.rolling(window=30, min_periods=1).mean()
            )
        return daily

    def partner_orgasms_timeseries(self, filters: SearchFilters):
        """Total orgasms across all partners per event date (for anomaly chart)."""
        self._require_pandas()
        rows = self.search_entries(filters, limit=100000)
        person_map = self.person_name_map()

        records: list[dict] = []
        for row in rows:
            try:
                event_date = pd.to_datetime(str(row.get("date")), format="%Y.%m.%d")
            except Exception:
                continue
            orgasms = row.get("person_orgasms") or {}
            # "partner" orgasms = anyone who is NOT the primary reporter
            primary_reporter_id = row.get("primary_reporter_id")
            primary_reporter_name = person_map.get(int(primary_reporter_id)) if primary_reporter_id else None
            total_partner = sum(
                int(count or 0)
                for name, count in orgasms.items()
                if name != primary_reporter_name
            )
            records.append({"date": event_date, "total_org_partner": total_partner})

        df = pd.DataFrame(records, columns=["date", "total_org_partner"])
        if df.empty:
            return df
        daily = df.groupby("date", as_index=False)["total_org_partner"].sum()
        daily["trend"] = daily["total_org_partner"].rolling(window=30, min_periods=1).mean()
        return daily

    # ── Chart dataframes ──────────────────────────────────────────────────────

    def ratings_dataframe(self, filters: SearchFilters):
        self._require_pandas()
        rows = self.search_entries(filters, limit=100000)
        return pd.DataFrame(rows)

    def sex_streaks_dataframe(self, filters: SearchFilters):
        self._require_pandas()
        self.ensure_expected_schema()
        where, params = self._build_where_clause(filters)
        query = (
            f"SELECT DISTINCT ev.event_date AS date FROM events ev {where} "
            f"ORDER BY ev.event_date"
        )
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
        current_type = current_start = None
        current_length = 0
        for _, row in points.iterrows():
            value, date = int(row["sex_occurred"]), row["date"]
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

    def position_frequency_dataframe(
        self, filters: SearchFilters, require_people: list[int] | None = None
    ):
        self._require_pandas()
        self.ensure_expected_schema()
        event_ids = self._event_ids_for_all_people(filters, require_people or [])
        if not event_ids:
            return pd.DataFrame(columns=["position", "count"])
        position_map = self._fetch_id_name_map("positions")
        counts: Counter[str] = Counter()
        try:
            with self.db.cursor() as cur:
                for eid in event_ids:
                    for pid in self._event_positions(cur, eid):
                        counts[position_map.get(pid, f"Unknown({pid})")] += 1
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to query position frequency: {exc}") from exc
        return pd.DataFrame([{"position": n, "count": c} for n, c in counts.items()])

    def position_combinations_dataframe(
        self, filters: SearchFilters, require_people: list[int] | None = None
    ):
        self._require_pandas()
        self.ensure_expected_schema()
        event_ids = self._event_ids_for_all_people(filters, require_people or [])
        if not event_ids:
            return pd.DataFrame(columns=["combination", "count"])
        position_map = self._fetch_id_name_map("positions")
        combo_counter: Counter[str] = Counter()
        try:
            with self.db.cursor() as cur:
                for eid in event_ids:
                    pos_ids = sorted(set(self._event_positions(cur, eid)))
                    if not pos_ids:
                        continue
                    label = " + ".join(position_map.get(p, f"Unknown({p})") for p in pos_ids)
                    combo_counter[label] += 1
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to query position combinations: {exc}") from exc
        return pd.DataFrame([{"combination": n, "count": c} for n, c in combo_counter.items()])

    def position_upset_dataframe(
        self,
        filters: SearchFilters,
        max_positions: int = 6,
        min_instances: int = 1,
        require_people: list[int] | None = None,
    ):
        self._require_pandas()
        self.ensure_expected_schema()
        event_ids = self._event_ids_for_all_people(filters, require_people or [])
        if not event_ids:
            return pd.DataFrame()
        max_positions = max(max_positions, 1)
        min_instances = max(min_instances, 1)
        position_map = self._fetch_id_name_map("positions")
        position_counter: Counter[int] = Counter()
        event_positions: list[list[int]] = []
        try:
            with self.db.cursor() as cur:
                for eid in event_ids:
                    pos_ids = sorted(set(self._event_positions(cur, eid)))
                    if not pos_ids:
                        continue
                    event_positions.append(pos_ids)
                    position_counter.update(pos_ids)
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to query upset data: {exc}") from exc

        top = [pid for pid, _ in position_counter.most_common(max_positions)]
        if not top:
            return pd.DataFrame()
        matrix = [tuple(1 if pid in ep else 0 for pid in top) for ep in event_positions]
        if min_instances > 1:
            ct = Counter(matrix)
            matrix = [row for row in matrix if ct[row] >= min_instances]
        names = [position_map.get(pid, f"Unknown({pid})") for pid in top]
        df = pd.DataFrame(matrix, columns=names)
        if df.empty:
            return df
        return df[df.sum().sort_values(ascending=True).keys()]

    def location_room_sankey_dataframe(self, filters: SearchFilters):
        self._require_pandas()
        self.ensure_expected_schema()
        event_ids = self._event_ids_for_filters(filters)
        if not event_ids:
            return pd.DataFrame(columns=["location", "room", "count"])
        counter: Counter[tuple[str, str]] = Counter()
        try:
            with self.db.cursor() as cur:
                for eid in event_ids:
                    place_ids = self._event_places(cur, eid)
                    locations = [PLACE_MAPPING[p] for p in place_ids if p in LOCATION_IDS]
                    rooms     = [PLACE_MAPPING[p] for p in place_ids if p in ROOM_IDS]
                    for loc in locations:
                        for room in rooms:
                            counter[(loc, room)] += 1
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to query location-room data: {exc}") from exc
        return pd.DataFrame(
            [{"location": loc, "room": room, "count": ct}
             for (loc, room), ct in counter.items()]
        )

    def duration_by_partner_dataframe(self, filters: SearchFilters):
        self._require_pandas()
        rows = self.search_entries(filters, limit=100000)
        records: list[dict] = []
        for row in rows:
            duration = row.get("duration")
            if duration is None:
                continue
            try:
                duration_value = int(duration)
            except (TypeError, ValueError):
                continue
            raw = str(row.get("partners") or "").strip()
            partners = [p.strip() for p in raw.split(",") if p.strip()] or ["Unknown"]
            for partner in partners:
                records.append({"partner": partner, "duration": duration_value})
        return pd.DataFrame(records, columns=["partner", "duration"])

    def partner_orgasms_anomaly_dataframe(
        self,
        filters: SearchFilters,
        window_days: int = 30,
        z_threshold: float = 2.0,
    ):
        self._require_pandas()
        daily = self.partner_orgasms_timeseries(filters)
        if daily.empty:
            return pd.DataFrame(
                columns=["date", "value", "baseline", "rolling_std", "zscore", "is_anomaly"]
            )
        window_days = max(window_days, 2)
        out = daily.rename(columns={"total_org_partner": "value"}).copy()
        out["baseline"]    = out["value"].rolling(window=window_days, min_periods=2).mean()
        out["rolling_std"] = out["value"].rolling(window=window_days, min_periods=2).std(ddof=0)
        out["zscore"]      = 0.0
        valid_std = out["rolling_std"].fillna(0) > 0
        out.loc[valid_std, "zscore"] = (
            (out.loc[valid_std, "value"] - out.loc[valid_std, "baseline"])
            / out.loc[valid_std, "rolling_std"]
        )
        out["is_anomaly"] = (out["zscore"].abs() >= float(z_threshold)).astype(int)
        return out[["date", "value", "baseline", "rolling_std", "zscore", "is_anomaly"]]

    def position_association_rules_dataframe(
        self,
        filters: SearchFilters,
        min_support: float = 0.05,
        min_confidence: float = 0.3,
        require_people: list[int] | None = None,
    ):
        self._require_pandas()
        self.ensure_expected_schema()
        event_ids = self._event_ids_for_all_people(filters, require_people or [])
        if not event_ids:
            return pd.DataFrame(
                columns=["antecedent","consequent","support","confidence","lift","count"]
            )
        position_map = self._fetch_id_name_map("positions")
        transactions: list[set[int]] = []
        try:
            with self.db.cursor() as cur:
                for eid in event_ids:
                    ids = set(self._event_positions(cur, eid))
                    if ids:
                        transactions.append(ids)
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to query association data: {exc}") from exc

        n = len(transactions)
        if n == 0:
            return pd.DataFrame(
                columns=["antecedent","consequent","support","confidence","lift","count"]
            )
        min_support    = max(min_support, 0.01)
        min_confidence = max(min_confidence, 0.01)
        single: Counter[int] = Counter()
        pairs:  Counter[tuple[int,int]] = Counter()
        for t in transactions:
            single.update(t)
            ordered = sorted(t)
            for i, left in enumerate(ordered):
                for right in ordered[i + 1:]:
                    pairs[(left, right)] += 1

        rules: list[dict] = []
        for (left, right), both in pairs.items():
            support = both / n
            if support < min_support:
                continue
            lc, rc = single[left], single[right]
            if lc <= 0 or rc <= 0:
                continue
            conf_lr = both / lc
            conf_rl = both / rc
            sup_r   = rc / n
            sup_l   = lc / n
            if conf_lr >= min_confidence and sup_r > 0:
                rules.append({
                    "antecedent": position_map.get(left,  f"Unknown({left})"),
                    "consequent": position_map.get(right, f"Unknown({right})"),
                    "support": support, "confidence": conf_lr,
                    "lift": conf_lr / sup_r, "count": both,
                })
            if conf_rl >= min_confidence and sup_l > 0:
                rules.append({
                    "antecedent": position_map.get(right, f"Unknown({right})"),
                    "consequent": position_map.get(left,  f"Unknown({left})"),
                    "support": support, "confidence": conf_rl,
                    "lift": conf_rl / sup_l, "count": both,
                })
        if not rules:
            return pd.DataFrame(
                columns=["antecedent","consequent","support","confidence","lift","count"]
            )
        df = pd.DataFrame(rules)
        return df.sort_values(
            ["lift", "confidence", "support"], ascending=False
        ).reset_index(drop=True)

    # ── Summary + report ─────────────────────────────────────────────────────

    def summary_metrics(self, filters: SearchFilters) -> dict[str, int]:
        rows = self.search_entries(filters, limit=100000)
        person_map = self.person_name_map()
        # "total_my_orgasms" = orgasms of the primary reporter across events
        total_all = 0
        for row in rows:
            orgasms = row.get("person_orgasms") or {}
            total_all += sum(int(c or 0) for c in orgasms.values())
        return {
            "entries": len(rows),
            "total_orgasms": total_all,
        }

    def summary_metrics_by_person(self, filters: SearchFilters) -> dict[str, int]:
        rows = self.search_entries(filters, limit=100000)
        totals: Counter[str] = Counter()
        for row in rows:
            orgasms = row.get("person_orgasms") or {}
            for person, count in orgasms.items():
                totals[str(person)] += int(count or 0)
        return dict(totals)

    def build_report(self, filters: SearchFilters, top_n: int = 5) -> dict:
        rows = self.search_entries(filters, limit=100000)
        metrics = self.summary_metrics(filters)
        partner_counter: Counter[str] = Counter()
        position_counter: Counter[str] = Counter()
        place_counter: Counter[str] = Counter()
        for row in rows:
            for key, ctr in (
                ("partners", partner_counter),
                ("positions", position_counter),
                ("places", place_counter),
            ):
                for item in [s.strip() for s in str(row.get(key) or "").split(",") if s.strip()]:
                    ctr[item] += 1
        dates = [r.get("date") for r in rows if r.get("date")]
        return {
            "filters": filters.__dict__,
            "metrics": metrics,
            "date_range": {
                "min": min(dates) if dates else None,
                "max": max(dates) if dates else None,
            },
            "top_partners":  [{"name": n, "count": c} for n, c in partner_counter.most_common(top_n)],
            "top_positions": [{"name": n, "count": c} for n, c in position_counter.most_common(top_n)],
            "top_places":    [{"name": n, "count": c} for n, c in place_counter.most_common(top_n)],
            "chart_summaries": {
                "sex_streak_segments":           int(len(self.sex_streaks_dataframe(filters))),
                "distinct_positions":            int(len(self.position_frequency_dataframe(filters))),
                "distinct_position_combinations": int(len(self.position_combinations_dataframe(filters))),
                "upset_combinations":            int(len(self.position_upset_dataframe(filters))),
                "location_room_links":           int(len(self.location_room_sankey_dataframe(filters))),
            },
        }

    def year_in_review(
        self, filters: SearchFilters, person_ids: list[int] | None = None
    ) -> dict:
        """Compute stats for the Rendezvous Report card."""
        if person_ids:
            event_ids = set(self._event_ids_for_all_people(filters, person_ids))
            rows = [
                r for r in self.search_entries(filters, limit=100000)
                if int(r.get("entry_id") or 0) in event_ids
            ]
        else:
            rows = self.search_entries(filters, limit=100000)
        if not rows:
            return {}

        dates = []
        for r in rows:
            try:
                dates.append(pd.to_datetime(str(r.get("date")), format="%Y.%m.%d"))
            except Exception:
                pass
        dates.sort()
        date_min = dates[0].strftime("%b %-d, %Y") if dates else None
        date_max = dates[-1].strftime("%b %-d, %Y") if dates else None
        n_sessions = len(rows)
        span_days = (dates[-1] - dates[0]).days + 1 if len(dates) >= 2 else 0
        sessions_per_week = round(n_sessions / (span_days / 7), 1) if span_days > 0 else 0.0

        durations = [int(r.get("duration") or 0) for r in rows if r.get("duration")]
        total_minutes = sum(durations)
        avg_minutes   = round(total_minutes / len(durations)) if durations else 0

        person_map = self.person_name_map()
        selected_names = (
            {person_map[pid] for pid in person_ids if pid in person_map}
            if person_ids else set(person_map.values())
        )

        by_person_total: Counter[str] = Counter()
        by_person_max:   Counter[str] = Counter()
        for row in rows:
            orgasms = row.get("person_orgasms") or {}
            for person, count in orgasms.items():
                if str(person) not in selected_names:
                    continue
                c = int(count or 0)
                by_person_total[str(person)] += c
                if c > by_person_max[str(person)]:
                    by_person_max[str(person)] = c
        by_person_avg = {
            name: round(by_person_total[name] / n_sessions, 2)
            for name in by_person_total
        }

        position_counter: Counter[str] = Counter()
        combo_counter:    Counter[str] = Counter()
        for row in rows:
            positions = sorted([
                p.strip() for p in str(row.get("positions") or "").split(",") if p.strip()
            ])
            for pos in positions:
                position_counter[pos] += 1
            if positions:
                combo_counter[" + ".join(positions)] += 1
        top_position, top_position_count = (
            position_counter.most_common(1)[0] if position_counter else (None, 0)
        )

        place_counter: Counter[str] = Counter()
        for row in rows:
            for pl in [p.strip() for p in str(row.get("places") or "").split(",") if p.strip()]:
                place_counter[pl] += 1
        top_place, top_place_count = (
            place_counter.most_common(1)[0] if place_counter else (None, 0)
        )

        streaks_df = self.sex_streaks_dataframe(filters)
        longest_sex_streak = longest_no_sex_streak = 0
        if not streaks_df.empty:
            sex_df    = streaks_df[streaks_df["type"] == "sex"]
            no_sex_df = streaks_df[streaks_df["type"] == "no_sex"]
            longest_sex_streak    = int(sex_df["length"].max())    if not sex_df.empty    else 0
            longest_no_sex_streak = int(no_sex_df["length"].max()) if not no_sex_df.empty else 0

        dow_counter:   Counter[str] = Counter()
        month_counter: Counter[str] = Counter()
        for d in dates:
            dow_counter[d.strftime("%A")]  += 1
            month_counter[d.strftime("%B")] += 1
        top_dow,   top_dow_count   = (dow_counter.most_common(1)[0]   if dow_counter   else (None, 0))
        top_month, top_month_count = (month_counter.most_common(1)[0] if month_counter else (None, 0))
        least_month, least_month_count = (
            month_counter.most_common()[-1] if len(month_counter) > 1 else (None, 0)
        )

        return {
            "date_min": date_min, "date_max": date_max,
            "n_sessions": n_sessions, "sessions_per_week": sessions_per_week,
            "total_minutes": total_minutes, "avg_minutes": avg_minutes,
            "orgasms_by_person_total": dict(by_person_total),
            "orgasms_by_person_avg":   by_person_avg,
            "orgasms_by_person_max":   dict(by_person_max),
            "n_distinct_positions": len(position_counter),
            "n_distinct_combos":    len(combo_counter),
            "top_position": top_position, "top_position_count": top_position_count,
            "top_place":    top_place,    "top_place_count":    top_place_count,
            "longest_sex_streak":    longest_sex_streak,
            "longest_no_sex_streak": longest_no_sex_streak,
            "top_day_of_week":          top_dow,        "top_day_of_week_count":  top_dow_count,
            "top_month":                top_month,      "top_month_count":        top_month_count,
            "least_month":              least_month,    "least_month_count":      least_month_count,
        }

    # ═════════════════════════════════════════════════════════════════════════
    # Write operations
    # ═════════════════════════════════════════════════════════════════════════

    def _writable_db(self) -> WritableDatabase:
        return WritableDatabase(self.db.db_path)

    def backup_db(self) -> Path:
        from datetime import datetime
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = (
            self.db.db_path.parent
            / f"{self.db.db_path.stem}_backup_{ts}{self.db.db_path.suffix}"
        )
        self._writable_db().backup(backup_path)
        return backup_path

    # ── Fetch event for edit dialog ───────────────────────────────────────────

    def fetch_event_for_edit(self, event_id: int) -> dict:
        """Return full event data including all interactions and reports."""
        self.ensure_expected_schema()
        try:
            with self.db.cursor() as cur:
                ev = cur.execute(
                    "SELECT event_id, event_date AS date, approx_duration, "
                    "merge_confidence FROM events WHERE event_id = ?",
                    (event_id,),
                ).fetchone()
                if ev is None:
                    raise DataSourceError(f"Event {event_id} not found")
                data = dict(ev)

                person_map   = self._fetch_id_name_map("partners")
                position_map = self._fetch_id_name_map("positions")

                # ── Reports (subjective) ──────────────────────────────────────
                report_rows = cur.execute(
                    "SELECT report_id, reporter_person_id, duration, note, "
                    "rating, initiator, safety_status, interaction_id "
                    "FROM event_reports WHERE event_id = ? ORDER BY report_id",
                    (event_id,),
                ).fetchall()
                reports = [dict(r) for r in report_rows]
                data["reports"] = reports

                # ── Interactions (objective) ──────────────────────────────────
                iid_rows = cur.execute(
                    "SELECT interaction_id FROM interactions WHERE event_id = ? "
                    "ORDER BY interaction_id",
                    (event_id,),
                ).fetchall()

                interactions: list[dict] = []
                for (iid,) in iid_rows:
                    participants = self._participants_for_interaction(cur, iid)
                    orgasms_raw  = self._orgasms_for_interaction(cur, iid)
                    interactions.append({
                        "interaction_id":  iid,
                        "participant_ids": participants,
                        "orgasms":         orgasms_raw,  # {person_id: count}
                        "position_ids":    self._positions_for_interaction(cur, iid),
                        "place_ids":       self._places_for_interaction(cur, iid),
                        "sex_type_ids":    self._sex_types_for_interaction(cur, iid),
                    })
                data["interactions"] = interactions

            return data
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to fetch event for edit: {exc}") from exc

    # Backwards-compat alias used by older UI code
    def fetch_entry_for_edit(self, entry_id: int) -> dict:
        return self.fetch_event_for_edit(entry_id)

    # ── Update event ──────────────────────────────────────────────────────────

    def update_event(
        self,
        event_id: int,
        *,
        date: str,
        duration: int | None = None,
        # Per-report subjective fields keyed by report_id
        reports: list[dict],
        # Per-interaction objective fields (list of dicts with interaction_id)
        interactions: list[dict],
    ) -> None:
        """Persist all editable fields for an event atomically.

        reports:  [{ report_id, rating, note, initiator, safety_status }]
        interactions: [{
            interaction_id,
            participant_ids: [int],
            orgasms: {person_id: int},
            position_ids: [int],
            place_ids: [int],
            sex_type_ids: [int],
        }]
        """
        wdb = self._writable_db()
        try:
            with wdb.transaction() as conn:
                cur = conn.cursor()

                # Update event date and duration
                cur.execute(
                    "UPDATE events SET event_date = ?, approx_duration = ? WHERE event_id = ?",
                    (date, duration, event_id),
                )

                # Update each report (subjective fields only — duration lives on the event)
                for rpt in reports:
                    cur.execute(
                        """UPDATE event_reports
                           SET rating = ?, note = ?,
                               initiator = ?, safety_status = ?
                           WHERE report_id = ?""",
                        (
                            rpt.get("rating"),
                            rpt.get("note") or None,
                            rpt.get("initiator"),
                            rpt.get("safety_status"),
                            int(rpt["report_id"]),
                        ),
                    )

                # Update each interaction (objective fields)
                for intr in interactions:
                    iid = int(intr["interaction_id"])

                    # Participants
                    cur.execute(
                        "DELETE FROM interaction_participants WHERE interaction_id = ?",
                        (iid,),
                    )
                    for pid in intr.get("participant_ids") or []:
                        cur.execute(
                            "INSERT OR IGNORE INTO interaction_participants "
                            "(interaction_id, person_id) VALUES (?, ?)",
                            (iid, int(pid)),
                        )

                    # Orgasms
                    cur.execute(
                        "DELETE FROM interaction_orgasms WHERE interaction_id = ?",
                        (iid,),
                    )
                    for pid, count in (intr.get("orgasms") or {}).items():
                        if count is not None and int(count) >= 0:
                            cur.execute(
                                "INSERT INTO interaction_orgasms "
                                "(interaction_id, person_id, count) VALUES (?, ?, ?)",
                                (iid, int(pid), int(count)),
                            )

                    # Positions
                    cur.execute(
                        "DELETE FROM interaction_positions WHERE interaction_id = ?",
                        (iid,),
                    )
                    for pos_id in intr.get("position_ids") or []:
                        cur.execute(
                            "INSERT OR IGNORE INTO interaction_positions "
                            "(interaction_id, canonical_position_id) VALUES (?, ?)",
                            (iid, int(pos_id)),
                        )

                    # Places
                    cur.execute(
                        "DELETE FROM interaction_places WHERE interaction_id = ?",
                        (iid,),
                    )
                    for place_id in intr.get("place_ids") or []:
                        cur.execute(
                            "INSERT OR IGNORE INTO interaction_places "
                            "(interaction_id, place_id) VALUES (?, ?)",
                            (iid, int(place_id)),
                        )

                    # Sex types
                    cur.execute(
                        "DELETE FROM interaction_sex_types WHERE interaction_id = ?",
                        (iid,),
                    )
                    for st_id in intr.get("sex_type_ids") or []:
                        cur.execute(
                            "INSERT OR IGNORE INTO interaction_sex_types "
                            "(interaction_id, sex_type_id) VALUES (?, ?)",
                            (iid, int(st_id)),
                        )

        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to update event {event_id}: {exc}") from exc
        finally:
            wdb.close()

    # Backwards-compat shim — UI calls update_entry with old signature;
    # translate to a single-interaction update_event call.
    def update_entry(
        self,
        entry_id: int,
        *,
        date: str,
        duration: int | None,
        rating: int | None,
        note: str | None,
        initiator: int | None,
        sex_type_ids: list[int],
        total_org: int,
        total_org_partner: int,
        reporter_person_id: int,
        partner_ids: list[int],
        partner_orgasms: dict[int, int | None],
        position_ids: list[int],
        place_ids: list[int],
    ) -> None:
        raw = self.fetch_event_for_edit(entry_id)
        reports      = raw.get("reports") or []
        interactions = raw.get("interactions") or []

        # Build orgasms dict: reporter gets total_org; partners get their attributed count
        orgasms: dict[int, int] = {}
        if reporter_person_id and total_org:
            orgasms[reporter_person_id] = total_org
        for pid in partner_ids:
            v = partner_orgasms.get(pid)
            if v is not None:
                orgasms[pid] = v

        # All people in the single interaction
        all_participants = list({reporter_person_id} | set(partner_ids))

        # Update the first interaction (or create one if somehow missing)
        if interactions:
            interactions[0].update({
                "participant_ids": all_participants,
                "orgasms":         orgasms,
                "position_ids":    position_ids,
                "place_ids":       place_ids,
                "sex_type_ids":    sex_type_ids,
            })
        else:
            # Shouldn't happen for valid data, but be defensive
            interactions = [{
                "interaction_id":  None,
                "participant_ids": all_participants,
                "orgasms":         orgasms,
                "position_ids":    position_ids,
                "place_ids":       place_ids,
                "sex_type_ids":    sex_type_ids,
            }]

        # Update the first report's subjective fields (no duration — event-level now)
        if reports:
            reports[0].update({
                "rating":        rating,
                "note":          note,
                "initiator":     initiator,
                "safety_status": reports[0].get("safety_status"),
            })

        self.update_event(
            entry_id,
            date=date,
            duration=duration,
            reports=reports,
            interactions=interactions,
        )

    # ── Merge events ──────────────────────────────────────────────────────────

    def merge_events(
        self,
        event_ids: list[int],
        *,
        canonical_date: str | None = None,
        canonical_duration: int | None = None,
    ) -> int:
        """Merge two or more events into the one with the lowest event_id.

        All interactions and reports from the other events are reassigned to
        the survivor.  The merged-away events are deleted.  Returns the
        surviving event_id.
        """
        if len(event_ids) < 2:
            raise DataSourceError("merge_events requires at least two event IDs")
        survivor_id = min(event_ids)
        others      = [eid for eid in event_ids if eid != survivor_id]

        wdb = self._writable_db()
        try:
            with wdb.transaction() as conn:
                cur = conn.cursor()

                # Resolve canonical date / duration
                if canonical_date is None:
                    row = cur.execute(
                        "SELECT event_date FROM events WHERE event_id = ?", (survivor_id,)
                    ).fetchone()
                    canonical_date = str(row[0]) if row else ""

                if canonical_duration is None:
                    durs = [
                        int(r[0]) for r in cur.execute(
                            f"SELECT approx_duration FROM events "
                            f"WHERE event_id IN ({','.join('?' * len(event_ids))})"
                            " AND approx_duration IS NOT NULL",
                            event_ids,
                        ).fetchall()
                    ]
                    canonical_duration = int(sum(durs) / len(durs)) if durs else None

                # Move interactions and reports to survivor
                for eid in others:
                    cur.execute(
                        "UPDATE interactions SET event_id = ? WHERE event_id = ?",
                        (survivor_id, eid),
                    )
                    cur.execute(
                        "UPDATE event_reports SET event_id = ? WHERE event_id = ?",
                        (survivor_id, eid),
                    )
                    cur.execute("DELETE FROM events WHERE event_id = ?", (eid,))

                # Update survivor metadata
                report_count = cur.execute(
                    "SELECT COUNT(*) FROM event_reports WHERE event_id = ?",
                    (survivor_id,),
                ).fetchone()[0]
                cur.execute(
                    "UPDATE events SET event_date = ?, approx_duration = ?, "
                    "report_count = ?, merge_confidence = 'manual' "
                    "WHERE event_id = ?",
                    (canonical_date, canonical_duration, report_count, survivor_id),
                )
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to merge events: {exc}") from exc
        finally:
            wdb.close()

        return survivor_id

    # ── Delete events ─────────────────────────────────────────────────────────

    def delete_events(self, event_ids: list[int]) -> None:
        """Permanently delete events and all their child rows."""
        if not event_ids:
            raise DataSourceError("delete_events requires at least one event ID")
        wdb = self._writable_db()
        try:
            with wdb.transaction() as conn:
                cur = conn.cursor()
                for eid in event_ids:
                    # Fetch all interaction_ids for this event
                    iid_rows = cur.execute(
                        "SELECT interaction_id FROM interactions WHERE event_id = ?",
                        (eid,),
                    ).fetchall()
                    iids = [int(r[0]) for r in iid_rows]

                    # Delete interaction child rows
                    for iid in iids:
                        cur.execute("DELETE FROM interaction_participants WHERE interaction_id = ?", (iid,))
                        cur.execute("DELETE FROM interaction_orgasms     WHERE interaction_id = ?", (iid,))
                        cur.execute("DELETE FROM interaction_positions   WHERE interaction_id = ?", (iid,))
                        cur.execute("DELETE FROM interaction_places      WHERE interaction_id = ?", (iid,))
                        cur.execute("DELETE FROM interaction_sex_types   WHERE interaction_id = ?", (iid,))
                    cur.execute("DELETE FROM interactions   WHERE event_id = ?", (eid,))
                    cur.execute("DELETE FROM event_reports  WHERE event_id = ?", (eid,))
                    cur.execute("DELETE FROM events         WHERE event_id = ?", (eid,))
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to delete events: {exc}") from exc
        finally:
            wdb.close()

    # ── Export helpers ────────────────────────────────────────────────────────

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
            tmp_path.write_text("\n", encoding="utf-8")
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

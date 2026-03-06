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
except ModuleNotFoundError:  # pragma: no cover - exercised in dependency-limited environments
    pd = None

from webapp.db import ReadOnlyDatabase


def _loess_smooth(x: "pd.Series", y: "pd.Series", frac: float = 0.3) -> "pd.Series":
    """1-D LOESS smoother using tricubic weights and local linear regression.

    Parameters
    ----------
    x : numeric Series (e.g. ordinal index 0..n-1)
    y : value Series
    frac : fraction of data used for each local fit (bandwidth)
    """
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
    person_ids: list[int] | None = None
    position_ids: list[int] | None = None
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
            "events",
            "event_reports",
            "report_partners",
            "report_positions",
            "report_places",
            "people",
            "canonical_positions",
        }
        available = set(self.list_tables())
        missing = sorted(expected - available)
        if missing:
            raise DataSourceError(
                "The configured database does not include required table(s): "
                f"{', '.join(missing)}. Available tables: {', '.join(sorted(available)) or '(none)'}"
            )

    def _fetch_id_name_map(self, table: str) -> dict[int, str]:
        table_sql = {"partners": "people", "positions": "canonical_positions"}.get(table, table)
        with self.db.cursor() as cur:
            rows = cur.execute(f"SELECT * FROM {table_sql}").fetchall()
        return {int(row[0]): str(row[1]) for row in rows}

    def _entry_base_sql(self) -> str:
        return "FROM event_reports e JOIN events ev ON ev.event_id = e.event_id"

    def _entry_date_col(self) -> str:
        return "ev.event_date"

    def _entry_id_col(self) -> str:
        return "e.report_id"

    def _require_pandas(self) -> None:
        if pd is None:
            raise DataSourceError("pandas is required for dataframe/chart operations")

    def _partner_ids_for_entry(self, cur: sqlite3.Cursor, entry_id: int) -> list[int]:
        sql = "SELECT partner_person_id FROM report_partners WHERE report_id = ?"
        return [int(r[0]) for r in cur.execute(sql, (entry_id,)).fetchall()]

    def _position_ids_for_entry(self, cur: sqlite3.Cursor, entry_id: int) -> list[int]:
        sql = "SELECT canonical_position_id FROM report_positions WHERE report_id = ?"
        return [int(r[0]) for r in cur.execute(sql, (entry_id,)).fetchall()]

    def _place_ids_for_entry(self, cur: sqlite3.Cursor, entry_id: int) -> list[int]:
        sql = "SELECT place_id FROM report_places WHERE report_id = ?"
        return [int(r[0]) for r in cur.execute(sql, (entry_id,)).fetchall()]

    def people_options(self) -> list[tuple[int, str]]:
        self.ensure_expected_schema()
        mapping = self._fetch_id_name_map("partners")
        return sorted(mapping.items(), key=lambda x: x[1].lower())

    def partner_options(self) -> list[tuple[int, str]]:
        return self.people_options()

    def person_name_map(self) -> dict[int, str]:
        self.ensure_expected_schema()
        return self._fetch_id_name_map("partners")

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

        date_col = self._entry_date_col()

        if filters.start_date:
            clauses.append(f"{date_col} >= ?")
            params.append(filters.start_date)
        if filters.end_date:
            clauses.append(f"{date_col} <= ?")
            params.append(filters.end_date)
        if filters.note_keyword:
            clauses.append("LOWER(COALESCE(e.note, '')) LIKE ?")
            params.append(f"%{filters.note_keyword.lower()}%")
        if filters.person_ids:
            placeholders = ", ".join("?" for _ in filters.person_ids)
            clauses.append(
                "("
                "e.reporter_person_id IN (" + placeholders + ") "
                "OR EXISTS (SELECT 1 FROM report_partners ep WHERE ep.report_id = e.report_id "
                "AND ep.partner_person_id IN (" + placeholders + "))"
                ")"
            )
            params.extend(filters.person_ids)
            params.extend(filters.person_ids)
        if filters.position_ids:
            placeholders = ", ".join("?" for _ in filters.position_ids)
            clauses.append(
                "EXISTS (SELECT 1 FROM report_positions epo "
                "WHERE epo.report_id = e.report_id "
                f"AND epo.canonical_position_id IN ({placeholders}))"
            )
            params.extend(filters.position_ids)
        if filters.place_id is not None:
            clauses.append(
                "EXISTS (SELECT 1 FROM report_places epl WHERE epl.report_id = e.report_id AND epl.place_id = ?)"
            )
            params.append(filters.place_id)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return where, params

    def search_entries(self, filters: SearchFilters, limit: int = 300) -> list[dict]:
        self.ensure_expected_schema()
        where, params = self._build_where_clause(filters)

        query = f"""
            SELECT
                {self._entry_id_col()} AS entry_id,
                {self._entry_date_col()} AS date,
                e.reporter_person_id,
                e.duration, e.note, e.rating, e.initiator,
                e.safety_status, e.total_org, e.total_org_partner
            {self._entry_base_sql()}
            {where}
            ORDER BY {self._entry_date_col()} DESC
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
                    partner_ids = self._partner_ids_for_entry(cur, int(entry_id))
                    position_ids = self._position_ids_for_entry(cur, int(entry_id))
                    place_ids = self._place_ids_for_entry(cur, int(entry_id))

                    reporter_name = partner_map.get(int(entry.get("reporter_person_id") or 0))
                    people_involved: list[str] = []
                    if reporter_name:
                        people_involved.append(reporter_name)
                    for i in partner_ids:
                        partner_name = partner_map.get(i, f"Unknown({i})")
                        if partner_name not in people_involved:
                            people_involved.append(partner_name)
                    entry["partners"] = ", ".join(people_involved)
                    entry["positions"] = ", ".join(position_map.get(i, f"Unknown({i})") for i in position_ids)
                    entry["places"] = ", ".join(PLACE_MAPPING.get(i, f"Unknown({i})") for i in place_ids)
                    entry["person_orgasms"] = self._row_person_orgasms(cur, entry, partner_map)

            return entries
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to query entries: {exc}") from exc


    def _row_person_orgasms(self, cur: sqlite3.Cursor, row: dict[str, object], person_map: dict[int, str]) -> dict[str, int]:
        totals = {name: 0 for name in person_map.values()}
        reporter_id = int(row.get("reporter_person_id") or 0)
        if reporter_id in person_map:
            totals[person_map[reporter_id]] += int(row.get("total_org") or 0)

        report_id = int(row.get("entry_id") or 0)
        partner_rows = cur.execute(
            "SELECT partner_person_id, orgasms_attributed FROM report_partners WHERE report_id = ?",
            (report_id,),
        ).fetchall()

        unknown_partner_ids: list[int] = []
        known_partner_total = 0
        for partner_id, orgasms_attributed in partner_rows:
            pid = int(partner_id)
            if pid not in person_map:
                continue
            if orgasms_attributed is None:
                unknown_partner_ids.append(pid)
            else:
                amount = int(orgasms_attributed)
                known_partner_total += amount
                totals[person_map[pid]] += amount

        if unknown_partner_ids:
            remaining = max(int(row.get("total_org_partner") or 0) - known_partner_total, 0)
            base = remaining // len(unknown_partner_ids)
            extra = remaining % len(unknown_partner_ids)
            for index, pid in enumerate(unknown_partner_ids):
                totals[person_map[pid]] += base + (1 if index < extra else 0)

        return totals

    def _entry_ids_for_filters(self, filters: SearchFilters) -> list[int]:
        self.ensure_expected_schema()
        where, params = self._build_where_clause(filters)
        query = f"SELECT {self._entry_id_col()} AS entry_id {self._entry_base_sql()} {where} ORDER BY {self._entry_date_col()}"
        try:
            with self.db.cursor() as cur:
                rows = cur.execute(query, params).fetchall()
            return [int(r[0]) for r in rows]
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to query filtered entry IDs: {exc}") from exc


    def _entry_ids_for_all_people(self, filters: SearchFilters, person_ids: list[int]) -> list[int]:
        entry_ids = self._entry_ids_for_filters(filters)
        if not person_ids:
            return entry_ids

        required = set(person_ids)
        out: list[int] = []
        try:
            with self.db.cursor() as cur:
                for entry_id in entry_ids:
                    reporter_row = cur.execute(
                        "SELECT reporter_person_id FROM event_reports WHERE report_id = ?",
                        (int(entry_id),),
                    ).fetchone()
                    reporter = int(reporter_row[0]) if reporter_row else None
                    partners = set(self._partner_ids_for_entry(cur, int(entry_id)))
                    participants = partners | ({reporter} if reporter is not None else set())
                    if required.issubset(participants):
                        out.append(entry_id)
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to query filtered entry IDs by people: {exc}") from exc

        return out

    def orgasms_by_person_timeseries(self, filters: SearchFilters, person_ids: list[int] | None = None, trend_kind: str = "rolling_30"):
        self._require_pandas()
        rows = self.search_entries(filters, limit=100000)
        people_map = self.person_name_map()
        selected = set(person_ids or people_map.keys())
        inverse_map = {name: pid for pid, name in people_map.items()}

        records: list[dict[str, object]] = []
        for row in rows:
            try:
                event_date = pd.to_datetime(str(row.get("date")), format="%Y.%m.%d")
            except Exception:
                continue
            orgasms = row.get("person_orgasms") or {}
            if not isinstance(orgasms, dict):
                continue
            for person_name, count in orgasms.items():
                person_id = inverse_map.get(str(person_name))
                if person_id is None or person_id not in selected:
                    continue
                records.append({"date": event_date, "person": person_name, "orgasms": int(count or 0)})

        df = pd.DataFrame(records, columns=["date", "person", "orgasms"])
        if df.empty:
            return df

        daily = df.groupby(["date", "person"], as_index=False)["orgasms"].sum()
        daily = daily.sort_values(["person", "date"])
        if trend_kind == "loess":
            # Apply LOESS per person; use transform-style assignment via a dict
            # to guarantee a Series (not DataFrame) is assigned to the column.
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
        self._require_pandas()
        self.ensure_expected_schema()
        where, params = self._build_where_clause(filters)
        query = (
            f"SELECT {self._entry_date_col()} AS date, e.total_org_partner "
            f"{self._entry_base_sql()} {where} ORDER BY {self._entry_date_col()}"
        )

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
        self._require_pandas()
        rows = self.search_entries(filters, limit=100000)
        return pd.DataFrame(rows)

    def sex_streaks_dataframe(self, filters: SearchFilters):
        self._require_pandas()
        self.ensure_expected_schema()
        where, params = self._build_where_clause(filters)
        query = f"SELECT DISTINCT {self._entry_date_col()} AS date {self._entry_base_sql()} {where} ORDER BY {self._entry_date_col()}"
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

    def position_frequency_dataframe(self, filters: SearchFilters, require_people: list[int] | None = None):
        self._require_pandas()
        self.ensure_expected_schema()
        entry_ids = self._entry_ids_for_all_people(filters, require_people or [])
        if not entry_ids:
            return pd.DataFrame(columns=["position", "count"])

        try:
            with self.db.cursor() as cur:
                position_map = self._fetch_id_name_map("positions")
                counts: Counter[str] = Counter()
                for entry_id in entry_ids:
                    position_ids = self._position_ids_for_entry(cur, int(entry_id))
                    for pid in position_ids:
                        counts[position_map.get(pid, f"Unknown({pid})")] += 1
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to query position frequency data: {exc}") from exc

        return pd.DataFrame(
            [{"position": name, "count": count} for name, count in counts.items()]
        )

    def position_combinations_dataframe(self, filters: SearchFilters, require_people: list[int] | None = None):
        self._require_pandas()
        self.ensure_expected_schema()
        entry_ids = self._entry_ids_for_all_people(filters, require_people or [])
        if not entry_ids:
            return pd.DataFrame(columns=["combination", "count"])

        try:
            with self.db.cursor() as cur:
                position_map = self._fetch_id_name_map("positions")
                combo_counter: Counter[str] = Counter()
                for entry_id in entry_ids:
                    position_ids = sorted(set(self._position_ids_for_entry(cur, int(entry_id))))
                    if not position_ids:
                        continue
                    label = " + ".join(position_map.get(pid, f"Unknown({pid})") for pid in position_ids)
                    combo_counter[label] += 1
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to query position combination data: {exc}") from exc

        return pd.DataFrame(
            [{"combination": name, "count": count} for name, count in combo_counter.items()]
        )


    def position_upset_dataframe(self, filters: SearchFilters, max_positions: int = 6, min_instances: int = 1, require_people: list[int] | None = None):
        self._require_pandas()
        self.ensure_expected_schema()
        entry_ids = self._entry_ids_for_all_people(filters, require_people or [])
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
                entry_positions: list[list[int]] = []

                for entry_id in entry_ids:
                    position_ids = sorted(set(self._position_ids_for_entry(cur, int(entry_id))))
                    if not position_ids:
                        continue
                    entry_positions.append(position_ids)
                    position_counter.update(position_ids)
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to query upset data: {exc}") from exc

        top_positions = [pid for pid, _ in position_counter.most_common(max_positions)]
        if not top_positions:
            return pd.DataFrame()

        binary_matrix: list[tuple[int, ...]] = []
        for positions in entry_positions:
            binary_row = tuple(1 if pid in positions else 0 for pid in top_positions)
            binary_matrix.append(binary_row)

        if min_instances > 1:
            counts = Counter(binary_matrix)
            binary_matrix = [item for item in binary_matrix if counts[item] >= min_instances]

        top_position_names = [position_map.get(pid, f"Unknown({pid})") for pid in top_positions]
        df = pd.DataFrame(binary_matrix, columns=top_position_names)
        if df.empty:
            return df

        return df[df.sum().sort_values(ascending=True).keys()]

    def location_room_sankey_dataframe(self, filters: SearchFilters):
        self._require_pandas()
        self.ensure_expected_schema()
        entry_ids = self._entry_ids_for_filters(filters)
        if not entry_ids:
            return pd.DataFrame(columns=["location", "room", "count"])

        counter: Counter[tuple[str, str]] = Counter()
        try:
            with self.db.cursor() as cur:
                for entry_id in entry_ids:
                    place_ids = self._place_ids_for_entry(cur, int(entry_id))
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


    def duration_by_partner_dataframe(self, filters: SearchFilters):
        self._require_pandas()
        self.ensure_expected_schema()
        rows = self.search_entries(filters, limit=100000)
        records: list[dict[str, object]] = []
        for row in rows:
            duration = row.get("duration")
            if duration is None:
                continue
            try:
                duration_value = int(duration)
            except (TypeError, ValueError):
                continue

            raw_partners = str(row.get("partners") or "").strip()
            partners = [p.strip() for p in raw_partners.split(",") if p.strip()] or ["Unknown"]
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
            return pd.DataFrame(columns=["date", "value", "baseline", "rolling_std", "zscore", "is_anomaly"])

        if window_days < 2:
            window_days = 2

        out = daily.rename(columns={"total_org_partner": "value"}).copy()
        out["baseline"] = out["value"].rolling(window=window_days, min_periods=2).mean()
        out["rolling_std"] = out["value"].rolling(window=window_days, min_periods=2).std(ddof=0)
        out["zscore"] = 0.0

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
        entry_ids = self._entry_ids_for_all_people(filters, require_people or [])
        if not entry_ids:
            return pd.DataFrame(columns=["antecedent", "consequent", "support", "confidence", "lift", "count"])

        try:
            with self.db.cursor() as cur:
                position_map = self._fetch_id_name_map("positions")
                transactions: list[set[int]] = []
                for entry_id in entry_ids:
                    ids = set(self._position_ids_for_entry(cur, int(entry_id)))
                    if ids:
                        transactions.append(ids)
        except sqlite3.Error as exc:
            raise DataSourceError(f"Failed to query association data: {exc}") from exc

        transaction_count = len(transactions)
        if transaction_count == 0:
            return pd.DataFrame(columns=["antecedent", "consequent", "support", "confidence", "lift", "count"])

        if min_support <= 0:
            min_support = 0.01
        if min_confidence <= 0:
            min_confidence = 0.01

        single_counts: Counter[int] = Counter()
        pair_counts: Counter[tuple[int, int]] = Counter()
        for t in transactions:
            single_counts.update(t)
            ordered = sorted(t)
            for i, left in enumerate(ordered):
                for right in ordered[i + 1 :]:
                    pair_counts[(left, right)] += 1

        rules: list[dict[str, object]] = []
        for (left, right), both_count in pair_counts.items():
            support = both_count / transaction_count
            if support < min_support:
                continue

            left_count = single_counts[left]
            right_count = single_counts[right]
            if left_count <= 0 or right_count <= 0:
                continue

            confidence_lr = both_count / left_count
            confidence_rl = both_count / right_count
            support_right = right_count / transaction_count
            support_left = left_count / transaction_count

            if confidence_lr >= min_confidence and support_right > 0:
                rules.append(
                    {
                        "antecedent": position_map.get(left, f"Unknown({left})"),
                        "consequent": position_map.get(right, f"Unknown({right})"),
                        "support": support,
                        "confidence": confidence_lr,
                        "lift": confidence_lr / support_right,
                        "count": both_count,
                    }
                )

            if confidence_rl >= min_confidence and support_left > 0:
                rules.append(
                    {
                        "antecedent": position_map.get(right, f"Unknown({right})"),
                        "consequent": position_map.get(left, f"Unknown({left})"),
                        "support": support,
                        "confidence": confidence_rl,
                        "lift": confidence_rl / support_left,
                        "count": both_count,
                    }
                )

        if not rules:
            return pd.DataFrame(columns=["antecedent", "consequent", "support", "confidence", "lift", "count"])

        df = pd.DataFrame(rules)
        return df.sort_values(["lift", "confidence", "support"], ascending=False).reset_index(drop=True)

    def summary_metrics(self, filters: SearchFilters) -> dict[str, int]:
        rows = self.search_entries(filters, limit=100000)
        return {
            "entries": len(rows),
            "total_partner_orgasms": sum(int(r.get("total_org_partner") or 0) for r in rows),
            "total_my_orgasms": sum(int(r.get("total_org") or 0) for r in rows),
        }

    def summary_metrics_by_person(self, filters: SearchFilters) -> dict[str, int]:
        rows = self.search_entries(filters, limit=100000)
        totals: Counter[str] = Counter()
        for row in rows:
            orgasms = row.get("person_orgasms") or {}
            if isinstance(orgasms, dict):
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

    def year_in_review(self, filters: SearchFilters, person_ids: list[int] | None = None) -> dict:
        """Compute stats for the Rendezvous Report card."""
        if person_ids:
            entry_ids = set(self._entry_ids_for_all_people(filters, person_ids))
            rows = [r for r in self.search_entries(filters, limit=100000)
                    if int(r.get("entry_id") or 0) in entry_ids]
        else:
            rows = self.search_entries(filters, limit=100000)

        if not rows:
            return {}

        # ── Dates ─────────────────────────────────────────────────────────────
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

        # ── Duration ──────────────────────────────────────────────────────────
        durations = [int(r.get("duration") or 0) for r in rows if r.get("duration")]
        total_minutes = sum(durations)
        avg_minutes = round(total_minutes / len(durations)) if durations else 0

        # ── Orgasms — only for selected people (or all if none selected) ───────
        person_map = self.person_name_map()
        if person_ids:
            selected_names = {person_map[pid] for pid in person_ids if pid in person_map}
        else:
            selected_names = set(person_map.values())

        by_person_total: Counter[str] = Counter()
        by_person_max: Counter[str] = Counter()
        for row in rows:
            orgasms = row.get("person_orgasms") or {}
            if isinstance(orgasms, dict):
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

        # ── Positions ─────────────────────────────────────────────────────────
        position_counter: Counter[str] = Counter()
        combo_counter: Counter[str] = Counter()
        for row in rows:
            positions = sorted([p.strip() for p in str(row.get("positions") or "").split(",") if p.strip()])
            for pos in positions:
                position_counter[pos] += 1
            if positions:
                combo_counter[" + ".join(positions)] += 1
        n_distinct_positions = len(position_counter)
        n_distinct_combos = len(combo_counter)
        top_position, top_position_count = (position_counter.most_common(1)[0] if position_counter else (None, 0))

        # ── Places ────────────────────────────────────────────────────────────
        place_counter: Counter[str] = Counter()
        for row in rows:
            for pl in [p.strip() for p in str(row.get("places") or "").split(",") if p.strip()]:
                place_counter[pl] += 1
        top_place, top_place_count = (place_counter.most_common(1)[0] if place_counter else (None, 0))

        # ── Streaks ───────────────────────────────────────────────────────────
        streaks_df = self.sex_streaks_dataframe(filters)
        longest_sex_streak = 0
        longest_no_sex_streak = 0
        if not streaks_df.empty:
            sex_rows_df = streaks_df[streaks_df["type"] == "sex"]
            no_sex_rows_df = streaks_df[streaks_df["type"] == "no_sex"]
            longest_sex_streak = int(sex_rows_df["length"].max()) if not sex_rows_df.empty else 0
            longest_no_sex_streak = int(no_sex_rows_df["length"].max()) if not no_sex_rows_df.empty else 0

        # ── Day of week & month ───────────────────────────────────────────────
        dow_counter: Counter[str] = Counter()
        month_counter: Counter[str] = Counter()
        for d in dates:
            dow_counter[d.strftime("%A")] += 1
            month_counter[d.strftime("%B")] += 1
        top_dow, top_dow_count = (dow_counter.most_common(1)[0] if dow_counter else (None, 0))
        top_month, top_month_count = (month_counter.most_common(1)[0] if month_counter else (None, 0))
        least_month, least_month_count = (month_counter.most_common()[-1] if len(month_counter) > 1 else (None, 0))

        return {
            "date_min": date_min,
            "date_max": date_max,
            "n_sessions": n_sessions,
            "sessions_per_week": sessions_per_week,
            "total_minutes": total_minutes,
            "avg_minutes": avg_minutes,
            "orgasms_by_person_total": dict(by_person_total),
            "orgasms_by_person_avg": by_person_avg,
            "orgasms_by_person_max": dict(by_person_max),
            "n_distinct_positions": n_distinct_positions,
            "n_distinct_combos": n_distinct_combos,
            "top_position": top_position,
            "top_position_count": top_position_count,
            "top_place": top_place,
            "top_place_count": top_place_count,
            "longest_sex_streak": longest_sex_streak,
            "longest_no_sex_streak": longest_no_sex_streak,
            "top_day_of_week": top_dow,
            "top_day_of_week_count": top_dow_count,
            "top_month": top_month,
            "top_month_count": top_month_count,
            "least_month": least_month,
            "least_month_count": least_month_count,
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

from __future__ import annotations

from dataclasses import dataclass
import sqlite3

import pandas as pd

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


class DataSourceError(RuntimeError):
    """Raised when the configured SQLite file is missing expected tables."""


@dataclass
class SearchFilters:
    start_date: str | None = None
    end_date: str | None = None
    note_keyword: str | None = None


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
        expected = {"entries", "entry_partner", "entry_position", "entry_place", "partners", "positions"}
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

    def search_entries(self, filters: SearchFilters, limit: int = 300) -> list[dict]:
        self.ensure_expected_schema()
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

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

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

    def partner_orgasms_timeseries(self, filters: SearchFilters) -> pd.DataFrame:
        self.ensure_expected_schema()
        clauses = []
        params: list[object] = []

        if filters.start_date:
            clauses.append("date >= ?")
            params.append(filters.start_date)
        if filters.end_date:
            clauses.append("date <= ?")
            params.append(filters.end_date)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        query = f"SELECT date, total_org_partner FROM entries {where} ORDER BY date"

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

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from webapp.db import ReadOnlyDatabase
from webapp.services import DataSourceError, SearchFilters, StatsService


def _build_test_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE entries (
            entry_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            date TEXT,
            duration INTEGER,
            note TEXT,
            rating INTEGER,
            initiator INTEGER,
            safety_status INTEGER,
            total_org INTEGER,
            total_org_partner INTEGER
        )
        """
    )
    cur.execute("CREATE TABLE entry_partner (entry_id INTEGER, partner_id INTEGER)")
    cur.execute("CREATE TABLE entry_position (entry_id INTEGER, position_id INTEGER)")
    cur.execute("CREATE TABLE entry_place (entry_id INTEGER, place_id INTEGER)")
    cur.execute("CREATE TABLE partners (partner_id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT)")
    cur.execute("CREATE TABLE positions (position_id INTEGER PRIMARY KEY, user_id INTEGER, name TEXT)")

    cur.execute("INSERT INTO partners VALUES (1, 1, 'Alice')")
    cur.execute("INSERT INTO partners VALUES (2, 1, 'Beth')")
    cur.execute("INSERT INTO positions VALUES (10, 1, 'Position A')")
    cur.execute("INSERT INTO positions VALUES (11, 1, 'Position B')")

    cur.execute(
        "INSERT INTO entries VALUES (1,1,'2024.01.01',30,'hello world',4,1,1,1,2)"
    )
    cur.execute(
        "INSERT INTO entries VALUES (2,1,'2024.01.02',20,'second note',5,1,1,1,1)"
    )

    cur.execute("INSERT INTO entry_partner VALUES (1,1)")
    cur.execute("INSERT INTO entry_partner VALUES (2,2)")
    cur.execute("INSERT INTO entry_position VALUES (1,10)")
    cur.execute("INSERT INTO entry_position VALUES (2,11)")
    cur.execute("INSERT INTO entry_place VALUES (1,0)")
    cur.execute("INSERT INTO entry_place VALUES (2,1)")

    conn.commit()
    conn.close()


class StatsServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "test.db"
        _build_test_db(self.db_path)
        self.service = StatsService(ReadOnlyDatabase(self.db_path))

    def tearDown(self) -> None:
        self.service.db.close()
        self.tmpdir.cleanup()

    def test_search_entries_filters_partner(self):
        rows = self.service.search_entries(SearchFilters(partner_id=1))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["entry_id"], 1)

    def test_search_entries_filters_note_keyword(self):
        rows = self.service.search_entries(SearchFilters(note_keyword="second"))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["entry_id"], 2)

    def test_summary_metrics(self):
        metrics = self.service.summary_metrics(SearchFilters())
        self.assertEqual(metrics["entries"], 2)
        self.assertEqual(metrics["total_partner_orgasms"], 3)
        self.assertEqual(metrics["total_my_orgasms"], 2)

    def test_partner_orgasms_timeseries(self):
        try:
            df = self.service.partner_orgasms_timeseries(SearchFilters())
            self.assertEqual(len(df), 2)
            self.assertIn("trend", df.columns)
        except DataSourceError as exc:
            self.assertIn("pandas is required", str(exc))

    def test_export_csv_creates_file(self):
        out = self.service.export_entries_csv(SearchFilters())
        self.assertTrue(out.exists())
        self.assertGreater(os.path.getsize(out), 0)


    def test_additional_chart_dataframes(self):
        streak_df = self.service.sex_streaks_dataframe(SearchFilters())
        self.assertIn("signed_length", streak_df.columns)
        self.assertGreaterEqual(len(streak_df), 1)

        position_df = self.service.position_frequency_dataframe(SearchFilters())
        self.assertIn("position", position_df.columns)
        self.assertEqual(int(position_df["count"].sum()), 2)

        combo_df = self.service.position_combinations_dataframe(SearchFilters())
        self.assertIn("combination", combo_df.columns)
        self.assertEqual(int(combo_df["count"].sum()), 2)

        sankey_df = self.service.location_room_sankey_dataframe(SearchFilters())
        self.assertIn("location", sankey_df.columns)
        self.assertIn("room", sankey_df.columns)

    def test_build_report_and_export_json(self):
        report = self.service.build_report(SearchFilters())
        self.assertEqual(report["metrics"]["entries"], 2)
        self.assertEqual(report["date_range"]["min"], "2024.01.01")
        self.assertEqual(report["date_range"]["max"], "2024.01.02")
        self.assertIn("chart_summaries", report)
        self.assertIn("distinct_positions", report["chart_summaries"])

        json_path = self.service.export_report_json(SearchFilters())
        self.assertTrue(json_path.exists())
        self.assertGreater(os.path.getsize(json_path), 0)


if __name__ == "__main__":
    unittest.main()

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from webapp.db import ReadOnlyDatabase
from webapp.services import DataSourceError, SearchFilters, StatsService


def _build_master_test_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    cur = conn.cursor()

    cur.executescript(
        """
        CREATE TABLE people (person_id INTEGER PRIMARY KEY, name TEXT NOT NULL);
        CREATE TABLE canonical_positions (canonical_position_id INTEGER PRIMARY KEY, canonical_name TEXT NOT NULL);
        CREATE TABLE events (
            event_id INTEGER PRIMARY KEY,
            event_date TEXT NOT NULL,
            approx_duration INTEGER,
            report_count INTEGER
        );
        CREATE TABLE event_reports (
            report_id INTEGER PRIMARY KEY,
            event_id INTEGER NOT NULL,
            source_id INTEGER NOT NULL,
            source_entry_id INTEGER NOT NULL,
            reporter_person_id INTEGER NOT NULL,
            duration INTEGER,
            note TEXT,
            rating INTEGER,
            initiator INTEGER,
            safety_status INTEGER,
            total_org INTEGER,
            total_org_partner INTEGER
        );
        CREATE TABLE report_partners (
            report_id INTEGER NOT NULL,
            partner_person_id INTEGER NOT NULL,
            source_partner_id INTEGER,
            orgasms_attributed INTEGER
        );
        CREATE TABLE report_positions (
            report_id INTEGER NOT NULL,
            canonical_position_id INTEGER NOT NULL
        );
        CREATE TABLE report_places (
            report_id INTEGER NOT NULL,
            place_id INTEGER NOT NULL
        );
        """
    )

    cur.execute("INSERT INTO people VALUES (1, 'Taylor')")
    cur.execute("INSERT INTO people VALUES (2, 'Alex')")
    cur.execute("INSERT INTO people VALUES (3, 'Beth')")
    cur.execute("INSERT INTO canonical_positions VALUES (10, 'Position A')")
    cur.execute("INSERT INTO canonical_positions VALUES (11, 'Position B')")

    cur.execute("INSERT INTO events VALUES (1, '2024.01.01', 30, 1)")
    cur.execute("INSERT INTO events VALUES (2, '2024.01.02', 20, 1)")
    cur.execute("INSERT INTO events VALUES (3, '2024.01.03', 60, 1)")

    cur.execute("INSERT INTO event_reports VALUES (101,1,1,1,1,30,'hello world',4,1,1,1,2)")
    cur.execute("INSERT INTO event_reports VALUES (102,2,1,2,1,20,'second note',5,1,1,1,1)")
    cur.execute("INSERT INTO event_reports VALUES (103,3,1,3,1,60,'third note',5,1,1,2,3)")

    cur.execute("INSERT INTO report_partners VALUES (101,2,1,NULL)")
    cur.execute("INSERT INTO report_partners VALUES (102,3,2,NULL)")
    cur.execute("INSERT INTO report_partners VALUES (103,2,1,NULL)")

    cur.execute("INSERT INTO report_positions VALUES (101,10)")
    cur.execute("INSERT INTO report_positions VALUES (101,11)")
    cur.execute("INSERT INTO report_positions VALUES (102,11)")
    cur.execute("INSERT INTO report_positions VALUES (103,10)")
    cur.execute("INSERT INTO report_positions VALUES (103,11)")

    cur.execute("INSERT INTO report_places VALUES (101,0)")
    cur.execute("INSERT INTO report_places VALUES (102,1)")
    cur.execute("INSERT INTO report_places VALUES (103,10)")

    conn.commit()
    conn.close()


class StatsServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "master.db"
        _build_master_test_db(self.db_path)
        self.service = StatsService(ReadOnlyDatabase(self.db_path))

    def tearDown(self) -> None:
        self.service.db.close()
        self.tmpdir.cleanup()

    def test_search_entries_filters_partner(self):
        rows = self.service.search_entries(SearchFilters(person_ids=[2]))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["entry_id"], 103)

    def test_search_entries_people_list_includes_reporter(self):
        rows = self.service.search_entries(SearchFilters())
        by_id = {int(row["entry_id"]): row for row in rows}
        self.assertIn("Taylor", str(by_id[101]["partners"]))
        self.assertIn("Alex", str(by_id[101]["partners"]))

    def test_search_entries_filters_note_keyword(self):
        rows = self.service.search_entries(SearchFilters(note_keyword="second"))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["entry_id"], 102)

    def test_search_entries_filters_multiple_positions(self):
        rows = self.service.search_entries(SearchFilters(position_ids=[10, 11]))
        self.assertEqual(len(rows), 3)

        rows = self.service.search_entries(SearchFilters(position_ids=[10]))
        self.assertEqual(len(rows), 2)

    def test_summary_metrics(self):
        metrics = self.service.summary_metrics(SearchFilters())
        self.assertEqual(metrics["entries"], 3)
        self.assertEqual(metrics["total_partner_orgasms"], 6)
        self.assertEqual(metrics["total_my_orgasms"], 4)

    def test_summary_metrics_by_person_includes_reporter_orgasms(self):
        totals = self.service.summary_metrics_by_person(SearchFilters())
        self.assertEqual(totals.get("Taylor"), 4)
        self.assertEqual(totals.get("Alex"), 5)
        self.assertEqual(totals.get("Beth"), 1)

    def test_partner_orgasms_timeseries(self):
        try:
            df = self.service.partner_orgasms_timeseries(SearchFilters())
            self.assertEqual(len(df), 3)
            self.assertIn("trend", df.columns)
        except DataSourceError as exc:
            self.assertIn("pandas is required", str(exc))

    def test_export_csv_creates_file(self):
        out = self.service.export_entries_csv(SearchFilters())
        self.assertTrue(out.exists())
        self.assertGreater(os.path.getsize(out), 0)


if __name__ == "__main__":
    unittest.main()

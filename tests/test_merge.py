import sqlite3
import tempfile
import unittest
from pathlib import Path

from webapp.merge import SourceConfig, build_master_database


def _create_source_db(path: Path, owner_user_id: int, entries: list[tuple], partners: list[tuple], positions: list[tuple], entry_partner: list[tuple], entry_position: list[tuple], entry_place: list[tuple]) -> None:
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

    cur.executemany("INSERT INTO entries VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", entries)
    cur.executemany("INSERT INTO partners VALUES (?, ?, ?)", partners)
    cur.executemany("INSERT INTO positions VALUES (?, ?, ?)", positions)
    cur.executemany("INSERT INTO entry_partner VALUES (?, ?)", entry_partner)
    cur.executemany("INSERT INTO entry_position VALUES (?, ?)", entry_position)
    cur.executemany("INSERT INTO entry_place VALUES (?, ?)", entry_place)

    conn.commit()
    conn.close()


class MasterDbBuildTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        root = Path(self.tmpdir.name)

        self.mine_db = root / "mine.db"
        self.hers_db = root / "hers.db"
        self.single_db = root / "single.db"
        self.out_db = root / "master.db"
        self.single_out_db = root / "single_master.db"

        _create_source_db(
            path=self.mine_db,
            owner_user_id=1,
            entries=[
                (1, 1, "2025.01.01", 40, "mine matched", 4, 1, 1, 1, 15),
                (2, 1, "2025.01.02", 20, "mine solo", 5, 1, 1, 2, 1),
            ],
            partners=[(1, 1, "Sam")],
            positions=[(10, 1, "Cowgirl")],
            entry_partner=[(1, 1), (2, 1)],
            entry_position=[(1, 10), (2, 10)],
            entry_place=[(1, 0), (2, 1)],
        )

        _create_source_db(
            path=self.hers_db,
            owner_user_id=2,
            entries=[
                (10, 2, "2025.01.01", 42, "hers matched", 5, 1, 1, 2, 25),
                (11, 2, "2025.01.03", 15, "hers solo", 4, 1, 1, 1, 0),
            ],
            partners=[(1, 2, "Alex"), (2, 2, "Boyfriend")],
            positions=[(10, 2, "Cow Girl")],
            entry_partner=[(10, 1), (10, 2), (11, 2)],
            entry_position=[(10, 10), (11, 10)],
            entry_place=[(10, 0), (11, 2)],
        )

        _create_source_db(
            path=self.single_db,
            owner_user_id=3,
            entries=[(21, 3, "2025.01.05", 35, "single only", 3, 1, 1, 1, 2)],
            partners=[(1, 3, "Jordan")],
            positions=[(8, 3, "Missionary")],
            entry_partner=[(21, 1)],
            entry_position=[(21, 8)],
            entry_place=[(21, 11)],
        )

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_build_master_db_with_two_sources(self):
        summary = build_master_database(
            output_path=self.out_db,
            sources=[
                SourceConfig(source_key="mine", db_path=self.mine_db, owner_name="Alex"),
                SourceConfig(source_key="hers", db_path=self.hers_db, owner_name="Sam"),
            ],
            duration_tolerance=5,
            non_interactive=True,
        )

        self.assertEqual(summary["sources"], 2)
        self.assertEqual(summary["report_count"], 4)
        self.assertEqual(summary["event_count"], 3)
        self.assertEqual(summary["matched_events"], 1)

        conn = sqlite3.connect(self.out_db)
        metadata = dict(conn.execute("SELECT key, value FROM metadata").fetchall())
        self.assertEqual(metadata["schema_version"], "2")

        people = [r[0] for r in conn.execute("SELECT name FROM people ORDER BY name").fetchall()]
        self.assertIn("Alex", people)
        self.assertIn("Sam", people)
        self.assertIn("Boyfriend", people)

        event_counts = conn.execute(
            "SELECT report_count FROM events ORDER BY event_id"
        ).fetchall()
        self.assertEqual([r[0] for r in event_counts], [2, 1, 1])

        mapped_positions = conn.execute(
            "SELECT COUNT(*) FROM canonical_positions"
        ).fetchone()[0]
        self.assertEqual(mapped_positions, 2)

        partner_rows = conn.execute(
            "SELECT COUNT(*) FROM report_partners"
        ).fetchone()[0]
        self.assertGreaterEqual(partner_rows, 4)

        conn.close()

    def test_single_source_import_creates_valid_master_db(self):
        summary = build_master_database(
            output_path=self.single_out_db,
            sources=[
                SourceConfig(source_key="single", db_path=self.single_db, owner_name="Taylor"),
            ],
            non_interactive=True,
        )
        self.assertEqual(summary["sources"], 1)
        self.assertEqual(summary["event_count"], 1)
        self.assertEqual(summary["matched_events"], 0)

        conn = sqlite3.connect(self.single_out_db)
        event_reports = conn.execute("SELECT COUNT(*) FROM event_reports").fetchone()[0]
        self.assertEqual(event_reports, 1)
        source_rows = conn.execute("SELECT COUNT(*) FROM source_databases").fetchone()[0]
        self.assertEqual(source_rows, 1)
        conn.close()


if __name__ == "__main__":
    unittest.main()

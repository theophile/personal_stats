from __future__ import annotations

from dataclasses import dataclass
from difflib import get_close_matches
from pathlib import Path
import json
import sqlite3


@dataclass(frozen=True)
class SourceConfig:
    source_key: str
    db_path: Path
    owner_name: str


@dataclass(frozen=True)
class SourceEntryRecord:
    source_key: str
    entry_id: int
    date: str
    duration: int | None
    note: str | None
    rating: int | None
    initiator: int | None
    safety_status: int | None
    total_org: int | None
    total_org_partner: int | None
    partner_ids: tuple[int, ...]
    position_ids: tuple[int, ...]
    place_ids: tuple[int, ...]


@dataclass(frozen=True)
class EventAssignment:
    event_id: int
    source_key: str
    entry_id: int


def _connect_readonly(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(f"Database file not found: {path}")
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _fetch_required_tables(conn: sqlite3.Connection, required: set[str]) -> None:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    available = {str(row[0]) for row in rows}
    missing = sorted(required - available)
    if missing:
        raise ValueError(
            f"Source database missing required table(s): {', '.join(missing)}. "
            f"Available: {', '.join(sorted(available)) or '(none)'}"
        )


def _load_source_entries(source_key: str, db_path: Path) -> list[SourceEntryRecord]:
    conn = _connect_readonly(db_path)
    try:
        _fetch_required_tables(
            conn,
            {
                "entries",
                "entry_partner",
                "entry_position",
                "entry_place",
                "partners",
                "positions",
            },
        )
        entry_rows = conn.execute(
            """
            SELECT entry_id, date, duration, note, rating, initiator,
                   safety_status, total_org, total_org_partner
            FROM entries
            ORDER BY date, entry_id
            """
        ).fetchall()

        partner_map = _group_rows(conn, "entry_partner", "entry_id", "partner_id")
        position_map = _group_rows(conn, "entry_position", "entry_id", "position_id")
        place_map = _group_rows(conn, "entry_place", "entry_id", "place_id")

        return [
            SourceEntryRecord(
                source_key=source_key,
                entry_id=int(row["entry_id"]),
                date=str(row["date"]),
                duration=int(row["duration"]) if row["duration"] is not None else None,
                note=str(row["note"]) if row["note"] is not None else None,
                rating=int(row["rating"]) if row["rating"] is not None else None,
                initiator=int(row["initiator"]) if row["initiator"] is not None else None,
                safety_status=int(row["safety_status"]) if row["safety_status"] is not None else None,
                total_org=int(row["total_org"]) if row["total_org"] is not None else None,
                total_org_partner=(
                    int(row["total_org_partner"]) if row["total_org_partner"] is not None else None
                ),
                partner_ids=tuple(partner_map.get(int(row["entry_id"]), ())),
                position_ids=tuple(position_map.get(int(row["entry_id"]), ())),
                place_ids=tuple(place_map.get(int(row["entry_id"]), ())),
            )
            for row in entry_rows
        ]
    finally:
        conn.close()


def _group_rows(conn: sqlite3.Connection, table: str, key_col: str, value_col: str) -> dict[int, list[int]]:
    rows = conn.execute(f"SELECT {key_col}, {value_col} FROM {table}").fetchall()
    grouped: dict[int, list[int]] = {}
    for row in rows:
        grouped.setdefault(int(row[0]), []).append(int(row[1]))
    return grouped


def _load_named_lookup(conn: sqlite3.Connection, table: str, id_col: str, name_col: str) -> dict[int, str]:
    rows = conn.execute(f"SELECT {id_col}, {name_col} FROM {table}").fetchall()
    return {int(row[0]): str(row[1]) for row in rows}


def _entry_distance(a: SourceEntryRecord, b: SourceEntryRecord) -> int:
    duration_penalty = 0
    if a.duration is not None and b.duration is not None:
        duration_penalty = abs(a.duration - b.duration)

    rating_penalty = 0
    if a.rating is not None and b.rating is not None:
        rating_penalty = abs(a.rating - b.rating) * 2

    return duration_penalty + rating_penalty


def assign_events(entries: list[SourceEntryRecord], duration_tolerance: int = 15) -> list[EventAssignment]:
    next_event_id = 1
    events: list[dict] = []
    assignments: list[EventAssignment] = []

    for record in sorted(entries, key=lambda e: (e.date, e.entry_id, e.source_key)):
        candidates = []
        for event in events:
            if event["date"] != record.date:
                continue
            if record.source_key in event["sources"]:
                continue
            if record.duration is not None and event["duration"] is not None:
                if abs(record.duration - event["duration"]) > duration_tolerance:
                    continue
            candidates.append((event, _entry_distance(event["pivot"], record)))

        if candidates:
            chosen, _ = min(candidates, key=lambda t: t[1])
            chosen["sources"].add(record.source_key)
            chosen["records"].append(record)
            chosen["duration"] = _average_duration(chosen["records"])
            assignments.append(
                EventAssignment(event_id=int(chosen["event_id"]), source_key=record.source_key, entry_id=record.entry_id)
            )
            continue

        event = {
            "event_id": next_event_id,
            "date": record.date,
            "duration": record.duration,
            "sources": {record.source_key},
            "records": [record],
            "pivot": record,
        }
        events.append(event)
        assignments.append(EventAssignment(event_id=next_event_id, source_key=record.source_key, entry_id=record.entry_id))
        next_event_id += 1

    return assignments


def _average_duration(records: list[SourceEntryRecord]) -> int | None:
    values = [r.duration for r in records if r.duration is not None]
    if not values:
        return None
    return int(sum(values) / len(values))


def _upsert_person(conn: sqlite3.Connection, name: str) -> int:
    row = conn.execute("SELECT person_id FROM people WHERE LOWER(name) = LOWER(?)", (name,)).fetchone()
    if row:
        return int(row[0])
    cur = conn.execute("INSERT INTO people (name) VALUES (?)", (name,))
    return int(cur.lastrowid)


def _prompt_select(prompt: str, options: list[str], default_index: int = 0) -> int:
    while True:
        print(prompt)
        for idx, option in enumerate(options, start=1):
            marker = " (default)" if idx - 1 == default_index else ""
            print(f"  {idx}) {option}{marker}")
        raw = input("Select number: ").strip()
        if not raw:
            return default_index
        if raw.isdigit() and 1 <= int(raw) <= len(options):
            return int(raw) - 1
        print("Invalid selection. Please enter one of the listed numbers.")


def _choose_partner_mapping(
    partner_name: str,
    known_people: list[str],
    non_interactive: bool,
) -> tuple[str, bool]:
    if non_interactive:
        exact = next((p for p in known_people if p.casefold() == partner_name.casefold()), None)
        if exact:
            return exact, False
        return partner_name, True

    options = known_people + [f"Create new person named '{partner_name}'"]
    suggestion = get_close_matches(partner_name, known_people, n=1, cutoff=0.75)
    default_idx = known_people.index(suggestion[0]) if suggestion else len(options) - 1
    idx = _prompt_select(
        f"Partner '{partner_name}' appears in source DB. Which master person is this?", options, default_idx
    )
    if idx == len(options) - 1:
        return partner_name, True
    return known_people[idx], False


def _choose_position_mapping(
    position_name: str,
    existing_canonicals: list[str],
    non_interactive: bool,
) -> tuple[str, bool]:
    if non_interactive:
        exact = next((p for p in existing_canonicals if p.casefold() == position_name.casefold()), None)
        if exact:
            return exact, False
        return position_name, True

    options = existing_canonicals + [f"Create new canonical position '{position_name}'"]
    suggestion = get_close_matches(position_name, existing_canonicals, n=1, cutoff=0.72)
    default_idx = existing_canonicals.index(suggestion[0]) if suggestion else len(options) - 1
    idx = _prompt_select(
        f"Position '{position_name}' found in source DB. Map to canonical position:", options, default_idx
    )
    if idx == len(options) - 1:
        return position_name, True
    return existing_canonicals[idx], False


def _initialize_master_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE people (
            person_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        );

        CREATE TABLE source_databases (
            source_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_key TEXT NOT NULL UNIQUE,
            db_path TEXT NOT NULL,
            owner_person_id INTEGER NOT NULL,
            FOREIGN KEY (owner_person_id) REFERENCES people(person_id)
        );

        CREATE TABLE source_partners (
            source_id INTEGER NOT NULL,
            source_partner_id INTEGER NOT NULL,
            source_partner_name TEXT NOT NULL,
            mapped_person_id INTEGER NOT NULL,
            PRIMARY KEY (source_id, source_partner_id),
            FOREIGN KEY (source_id) REFERENCES source_databases(source_id),
            FOREIGN KEY (mapped_person_id) REFERENCES people(person_id)
        );

        CREATE TABLE canonical_positions (
            canonical_position_id INTEGER PRIMARY KEY AUTOINCREMENT,
            canonical_name TEXT NOT NULL UNIQUE
        );

        CREATE TABLE source_position_map (
            source_id INTEGER NOT NULL,
            source_position_id INTEGER NOT NULL,
            source_position_name TEXT NOT NULL,
            canonical_position_id INTEGER NOT NULL,
            PRIMARY KEY (source_id, source_position_id),
            FOREIGN KEY (source_id) REFERENCES source_databases(source_id),
            FOREIGN KEY (canonical_position_id) REFERENCES canonical_positions(canonical_position_id)
        );

        CREATE TABLE events (
            event_id INTEGER PRIMARY KEY,
            event_date TEXT NOT NULL,
            approx_duration INTEGER,
            report_count INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE event_reports (
            report_id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            total_org_partner INTEGER,
            UNIQUE (source_id, source_entry_id),
            FOREIGN KEY (event_id) REFERENCES events(event_id),
            FOREIGN KEY (source_id) REFERENCES source_databases(source_id),
            FOREIGN KEY (reporter_person_id) REFERENCES people(person_id)
        );

        CREATE TABLE report_partners (
            report_id INTEGER NOT NULL,
            partner_person_id INTEGER NOT NULL,
            source_partner_id INTEGER,
            orgasms_attributed INTEGER,
            PRIMARY KEY (report_id, partner_person_id),
            FOREIGN KEY (report_id) REFERENCES event_reports(report_id),
            FOREIGN KEY (partner_person_id) REFERENCES people(person_id)
        );

        CREATE TABLE report_positions (
            report_id INTEGER NOT NULL,
            canonical_position_id INTEGER NOT NULL,
            PRIMARY KEY (report_id, canonical_position_id),
            FOREIGN KEY (report_id) REFERENCES event_reports(report_id),
            FOREIGN KEY (canonical_position_id) REFERENCES canonical_positions(canonical_position_id)
        );

        CREATE TABLE report_places (
            report_id INTEGER NOT NULL,
            place_id INTEGER NOT NULL,
            PRIMARY KEY (report_id, place_id),
            FOREIGN KEY (report_id) REFERENCES event_reports(report_id)
        );

        CREATE TABLE raw_source_objects (
            source_id INTEGER NOT NULL,
            object_type TEXT NOT NULL,
            object_name TEXT NOT NULL,
            table_name TEXT,
            sql TEXT,
            PRIMARY KEY (source_id, object_type, object_name),
            FOREIGN KEY (source_id) REFERENCES source_databases(source_id)
        );

        CREATE TABLE raw_source_columns (
            source_id INTEGER NOT NULL,
            table_name TEXT NOT NULL,
            column_order INTEGER NOT NULL,
            column_name TEXT NOT NULL,
            declared_type TEXT,
            is_not_null INTEGER NOT NULL,
            default_value TEXT,
            is_primary_key INTEGER NOT NULL,
            PRIMARY KEY (source_id, table_name, column_order),
            FOREIGN KEY (source_id) REFERENCES source_databases(source_id)
        );

        CREATE TABLE raw_source_rows (
            source_id INTEGER NOT NULL,
            table_name TEXT NOT NULL,
            source_row_id INTEGER NOT NULL,
            row_data_json TEXT NOT NULL,
            PRIMARY KEY (source_id, table_name, source_row_id),
            FOREIGN KEY (source_id) REFERENCES source_databases(source_id)
        );
        """
    )


def _snapshot_source_raw_data(master_conn: sqlite3.Connection, source_id: int, db_path: Path) -> int:
    source_conn = _connect_readonly(db_path)
    try:
        objects = source_conn.execute(
            """
            SELECT type, name, tbl_name, sql
            FROM sqlite_master
            WHERE name NOT LIKE 'sqlite_%'
            ORDER BY type, name
            """
        ).fetchall()

        master_conn.executemany(
            """
            INSERT INTO raw_source_objects (source_id, object_type, object_name, table_name, sql)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (source_id, str(row["type"]), str(row["name"]), str(row["tbl_name"]), row["sql"])
                for row in objects
            ],
        )

        tables = [str(row["name"]) for row in objects if str(row["type"]) == "table"]
        total_rows = 0
        for table_name in tables:
            columns = source_conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            master_conn.executemany(
                """
                INSERT INTO raw_source_columns (
                    source_id, table_name, column_order, column_name,
                    declared_type, is_not_null, default_value, is_primary_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        source_id,
                        table_name,
                        int(col["cid"]),
                        str(col["name"]),
                        str(col["type"]) if col["type"] is not None else None,
                        int(col["notnull"]),
                        str(col["dflt_value"]) if col["dflt_value"] is not None else None,
                        int(col["pk"]),
                    )
                    for col in columns
                ],
            )

            rows = source_conn.execute(f'SELECT * FROM "{table_name}"').fetchall()
            total_rows += len(rows)
            master_conn.executemany(
                """
                INSERT INTO raw_source_rows (source_id, table_name, source_row_id, row_data_json)
                VALUES (?, ?, ?, ?)
                """,
                [
                    (
                        source_id,
                        table_name,
                        index,
                        json.dumps(dict(row), ensure_ascii=False, sort_keys=True),
                    )
                    for index, row in enumerate(rows, start=1)
                ],
            )
        return total_rows
    finally:
        source_conn.close()


def _validate_existing_master_schema(conn: sqlite3.Connection) -> None:
    row = conn.execute("SELECT value FROM metadata WHERE key = 'schema_version'").fetchone()
    if row is None:
        raise ValueError("Existing output DB is missing metadata.schema_version; not a compatible master DB")
    version = str(row[0])
    if version != "3":
        raise ValueError(
            f"Existing output DB has schema_version={version}, but this importer expects schema_version=3"
        )


def _load_existing_event_states(conn: sqlite3.Connection) -> tuple[list[dict], int]:
    event_rows = conn.execute(
        "SELECT event_id, event_date, approx_duration FROM events ORDER BY event_id"
    ).fetchall()
    events: list[dict] = []
    for row in event_rows:
        source_rows = conn.execute(
            """
            SELECT DISTINCT sd.source_key
            FROM event_reports er
            JOIN source_databases sd ON sd.source_id = er.source_id
            WHERE er.event_id = ?
            """,
            (int(row["event_id"]),),
        ).fetchall()
        events.append(
            {
                "event_id": int(row["event_id"]),
                "date": str(row["event_date"]),
                "duration": int(row["approx_duration"]) if row["approx_duration"] is not None else None,
                "sources": {str(r[0]) for r in source_rows},
            }
        )

    max_event_id = max((event["event_id"] for event in events), default=0)
    return events, max_event_id


def _assign_entries_to_master_events(
    entries: list[SourceEntryRecord],
    existing_events: list[dict],
    next_event_id_start: int,
    duration_tolerance: int,
) -> tuple[dict[tuple[str, int], int], dict[int, list[SourceEntryRecord]], int]:
    events = [
        {
            "event_id": event["event_id"],
            "date": event["date"],
            "duration": event["duration"],
            "sources": set(event["sources"]),
        }
        for event in existing_events
    ]
    next_event_id = next_event_id_start
    assignment_map: dict[tuple[str, int], int] = {}
    new_event_rollup: dict[int, list[SourceEntryRecord]] = {}

    for record in sorted(entries, key=lambda e: (e.date, e.entry_id, e.source_key)):
        candidates = []
        for event in events:
            if event["date"] != record.date:
                continue
            if record.source_key in event["sources"]:
                continue
            if record.duration is not None and event["duration"] is not None:
                if abs(record.duration - event["duration"]) > duration_tolerance:
                    continue
            penalty = 0
            if record.duration is not None and event["duration"] is not None:
                penalty = abs(record.duration - event["duration"])
            candidates.append((event, penalty))

        if candidates:
            chosen, _ = min(candidates, key=lambda t: t[1])
            chosen["sources"].add(record.source_key)
            assignment_map[(record.source_key, record.entry_id)] = int(chosen["event_id"])
            continue

        event_id = next_event_id
        next_event_id += 1
        events.append(
            {
                "event_id": event_id,
                "date": record.date,
                "duration": record.duration,
                "sources": {record.source_key},
            }
        )
        assignment_map[(record.source_key, record.entry_id)] = event_id
        new_event_rollup.setdefault(event_id, []).append(record)

    return assignment_map, new_event_rollup, next_event_id


def _insert_metadata(conn: sqlite3.Connection, duration_tolerance: int) -> None:
    conn.executemany(
        "INSERT INTO metadata (key, value) VALUES (?, ?)",
        [
            ("schema_version", "3"),
            ("duration_tolerance", str(duration_tolerance)),
            ("match_strategy", "same_date_plus_duration_tolerance"),
            ("source_snapshot_mode", "full_sqlite_object_and_row_snapshot"),
        ],
    )


def build_master_database(
    output_path: Path | str,
    sources: list[SourceConfig],
    duration_tolerance: int = 15,
    non_interactive: bool = False,
    update_existing: bool = False,
) -> dict[str, int]:
    if not sources:
        raise ValueError("At least one source database is required")

    out = Path(output_path)
    creating_new = True
    if out.exists() and not update_existing:
        out.unlink()
    elif out.exists() and update_existing:
        creating_new = False

    all_entries: list[SourceEntryRecord] = []
    source_partner_names: dict[str, dict[int, str]] = {}
    source_position_names: dict[str, dict[int, str]] = {}

    for source in sources:
        all_entries.extend(_load_source_entries(source.source_key, source.db_path))
        conn = _connect_readonly(source.db_path)
        try:
            source_partner_names[source.source_key] = _load_named_lookup(conn, "partners", "partner_id", "name")
            source_position_names[source.source_key] = _load_named_lookup(conn, "positions", "position_id", "name")
        finally:
            conn.close()

    conn = sqlite3.connect(out)
    conn.row_factory = sqlite3.Row
    try:
        if creating_new:
            _initialize_master_schema(conn)
            _insert_metadata(conn, duration_tolerance)
        else:
            _validate_existing_master_schema(conn)
            conn.execute(
                "UPDATE metadata SET value = ? WHERE key = 'duration_tolerance'",
                (str(duration_tolerance),),
            )

        source_ids: dict[str, int] = {}

        for source in sources:
            person_id = _upsert_person(conn, source.owner_name)
            existing_source = conn.execute(
                "SELECT source_id FROM source_databases WHERE source_key = ?", (source.source_key,)
            ).fetchone()
            if existing_source:
                source_id = int(existing_source[0])
                conn.execute(
                    "UPDATE source_databases SET db_path = ?, owner_person_id = ? WHERE source_id = ?",
                    (str(source.db_path), person_id, source_id),
                )
            else:
                cur = conn.execute(
                    "INSERT INTO source_databases (source_key, db_path, owner_person_id) VALUES (?, ?, ?)",
                    (source.source_key, str(source.db_path), person_id),
                )
                source_id = int(cur.lastrowid)
            source_ids[source.source_key] = source_id

        raw_rows_copied = 0
        for source in sources:
            source_id = source_ids[source.source_key]
            conn.execute("DELETE FROM raw_source_rows WHERE source_id = ?", (source_id,))
            conn.execute("DELETE FROM raw_source_columns WHERE source_id = ?", (source_id,))
            conn.execute("DELETE FROM raw_source_objects WHERE source_id = ?", (source_id,))
            raw_rows_copied += _snapshot_source_raw_data(
                master_conn=conn,
                source_id=source_id,
                db_path=source.db_path,
            )

        canonical_positions: dict[str, int] = {}

        for source in sources:
            source_id = source_ids[source.source_key]

            existing_partner_rows = conn.execute(
                "SELECT source_partner_id FROM source_partners WHERE source_id = ?",
                (source_id,),
            ).fetchall()
            existing_partner_ids = {int(row[0]) for row in existing_partner_rows}

            for partner_id, partner_name in sorted(source_partner_names[source.source_key].items()):
                if partner_id in existing_partner_ids:
                    continue
                known_people = [row["name"] for row in conn.execute("SELECT name FROM people ORDER BY name").fetchall()]
                mapped_name, _ = _choose_partner_mapping(partner_name, known_people, non_interactive)
                mapped_id = _upsert_person(conn, mapped_name)

                conn.execute(
                    """
                    INSERT INTO source_partners (source_id, source_partner_id, source_partner_name, mapped_person_id)
                    VALUES (?, ?, ?, ?)
                    """,
                    (source_id, partner_id, partner_name, mapped_id),
                )

            existing_position_rows = conn.execute(
                "SELECT source_position_id FROM source_position_map WHERE source_id = ?",
                (source_id,),
            ).fetchall()
            existing_position_ids = {int(row[0]) for row in existing_position_rows}

            for position_id, position_name in sorted(source_position_names[source.source_key].items()):
                if position_id in existing_position_ids:
                    continue
                if not canonical_positions:
                    rows = conn.execute(
                        "SELECT canonical_name, canonical_position_id FROM canonical_positions ORDER BY canonical_name"
                    ).fetchall()
                    canonical_positions = {str(row[0]): int(row[1]) for row in rows}

                existing = sorted(canonical_positions.keys())
                canonical_name, _ = _choose_position_mapping(position_name, existing, non_interactive)

                if canonical_name not in canonical_positions:
                    cur = conn.execute(
                        "INSERT INTO canonical_positions (canonical_name) VALUES (?)", (canonical_name,)
                    )
                    canonical_positions[canonical_name] = int(cur.lastrowid)

                conn.execute(
                    """
                    INSERT INTO source_position_map
                      (source_id, source_position_id, source_position_name, canonical_position_id)
                    VALUES (?, ?, ?, ?)
                    """,
                    (source_id, position_id, position_name, canonical_positions[canonical_name]),
                )

        new_entries: list[SourceEntryRecord] = []
        for entry in all_entries:
            source_id = source_ids[entry.source_key]
            already = conn.execute(
                "SELECT 1 FROM event_reports WHERE source_id = ? AND source_entry_id = ?",
                (source_id, entry.entry_id),
            ).fetchone()
            if already is None:
                new_entries.append(entry)

        existing_events, max_event_id = _load_existing_event_states(conn)
        assignment_map, new_event_rollup, _ = _assign_entries_to_master_events(
            entries=new_entries,
            existing_events=existing_events,
            next_event_id_start=max_event_id + 1,
            duration_tolerance=duration_tolerance,
        )

        for event_id, reports in sorted(new_event_rollup.items()):
            durations = [r.duration for r in reports if r.duration is not None]
            conn.execute(
                "INSERT INTO events (event_id, event_date, approx_duration, report_count) VALUES (?, ?, ?, 0)",
                (
                    event_id,
                    reports[0].date,
                    int(sum(durations) / len(durations)) if durations else None,
                ),
            )

        for entry in new_entries:
            source_id = source_ids[entry.source_key]
            event_id = assignment_map[(entry.source_key, entry.entry_id)]
            reporter_id = conn.execute(
                "SELECT owner_person_id FROM source_databases WHERE source_id = ?", (source_id,)
            ).fetchone()[0]

            cur = conn.execute(
                """
                INSERT INTO event_reports (
                  event_id, source_id, source_entry_id, reporter_person_id,
                  duration, note, rating, initiator, safety_status, total_org, total_org_partner
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    source_id,
                    entry.entry_id,
                    int(reporter_id),
                    entry.duration,
                    entry.note,
                    entry.rating,
                    entry.initiator,
                    entry.safety_status,
                    entry.total_org,
                    entry.total_org_partner,
                ),
            )
            report_id = int(cur.lastrowid)

            for partner_id in entry.partner_ids:
                mapped = conn.execute(
                    "SELECT mapped_person_id FROM source_partners WHERE source_id = ? AND source_partner_id = ?",
                    (source_id, partner_id),
                ).fetchone()
                if mapped is None:
                    continue
                conn.execute(
                    """
                    INSERT OR IGNORE INTO report_partners
                      (report_id, partner_person_id, source_partner_id, orgasms_attributed)
                    VALUES (?, ?, ?, NULL)
                    """,
                    (report_id, int(mapped[0]), partner_id),
                )

            for position_id in entry.position_ids:
                mapped = conn.execute(
                    "SELECT canonical_position_id FROM source_position_map WHERE source_id = ? AND source_position_id = ?",
                    (source_id, position_id),
                ).fetchone()
                if mapped is None:
                    continue
                conn.execute(
                    "INSERT OR IGNORE INTO report_positions (report_id, canonical_position_id) VALUES (?, ?)",
                    (report_id, int(mapped[0])),
                )

            for place_id in entry.place_ids:
                conn.execute(
                    "INSERT OR IGNORE INTO report_places (report_id, place_id) VALUES (?, ?)",
                    (report_id, place_id),
                )

        conn.execute(
            """
            UPDATE events
            SET report_count = (
                SELECT COUNT(*) FROM event_reports er WHERE er.event_id = events.event_id
            )
            """
        )

        conn.commit()
        total_events = int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0])
        matched_events = int(conn.execute("SELECT COUNT(*) FROM events WHERE report_count > 1").fetchone()[0])
        return {
            "sources": len(sources),
            "event_count": total_events,
            "report_count": len(new_entries),
            "matched_events": matched_events,
            "single_report_events": total_events - matched_events,
            "raw_rows_copied": raw_rows_copied,
        }
    finally:
        conn.close()


def merge_databases(
    mine_db_path: Path | str,
    hers_db_path: Path | str,
    output_path: Path | str,
    mine_name: str,
    hers_name: str,
    duration_tolerance: int = 15,
) -> dict[str, int]:
    return build_master_database(
        output_path=output_path,
        sources=[
            SourceConfig(source_key="mine", db_path=Path(mine_db_path), owner_name=mine_name),
            SourceConfig(source_key="hers", db_path=Path(hers_db_path), owner_name=hers_name),
        ],
        duration_tolerance=duration_tolerance,
        non_interactive=True,
    )

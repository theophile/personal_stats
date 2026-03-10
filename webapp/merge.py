from __future__ import annotations

from dataclasses import dataclass
from difflib import get_close_matches
from pathlib import Path
import json
import sqlite3


# ── Duration tolerance for high-confidence auto-merge (minutes) ───────────────
AUTO_MERGE_DURATION_TOLERANCE = 20


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
    partner_ids: tuple[int, ...]      # source-local partner IDs
    position_ids: tuple[int, ...]     # source-local position IDs
    place_ids: tuple[int, ...]
    sex_type_ids: tuple[int, ...]


# ── Helpers ───────────────────────────────────────────────────────────────────

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


def _group_rows(
    conn: sqlite3.Connection, table: str, key_col: str, value_col: str
) -> dict[int, list[int]]:
    rows = conn.execute(f"SELECT {key_col}, {value_col} FROM {table}").fetchall()
    grouped: dict[int, list[int]] = {}
    for row in rows:
        grouped.setdefault(int(row[0]), []).append(int(row[1]))
    return grouped


def _load_named_lookup(
    conn: sqlite3.Connection, table: str, id_col: str, name_col: str
) -> dict[int, str]:
    rows = conn.execute(f"SELECT {id_col}, {name_col} FROM {table}").fetchall()
    return {int(row[0]): str(row[1]) for row in rows}


def _upsert_person(conn: sqlite3.Connection, name: str) -> int:
    row = conn.execute(
        "SELECT person_id FROM people WHERE LOWER(name) = LOWER(?)", (name,)
    ).fetchone()
    if row:
        return int(row[0])
    cur = conn.execute("INSERT INTO people (name) VALUES (?)", (name,))
    return int(cur.lastrowid)


# ── Source loading ────────────────────────────────────────────────────────────

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
                "entry_sex_type",
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

        partner_map   = _group_rows(conn, "entry_partner",   "entry_id", "partner_id")
        position_map  = _group_rows(conn, "entry_position",  "entry_id", "position_id")
        place_map     = _group_rows(conn, "entry_place",     "entry_id", "place_id")
        sex_type_map  = _group_rows(conn, "entry_sex_type",  "entry_id", "sex_type_id")

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
                sex_type_ids=tuple(sex_type_map.get(int(row["entry_id"]), ())),
            )
            for row in entry_rows
        ]
    finally:
        conn.close()


# ── Confidence matching ───────────────────────────────────────────────────────

def _durations_compatible(a: int | None, b: int | None, tolerance: int) -> bool:
    """True if durations are within tolerance, or either is unknown."""
    if a is None or b is None:
        return True
    return abs(a - b) <= tolerance


def _mutual_cross_reference(
    entry_a: SourceEntryRecord,
    reporter_a_master_id: int,
    partners_a_master_ids: set[int],
    entry_b: SourceEntryRecord,
    reporter_b_master_id: int,
    partners_b_master_ids: set[int],
) -> bool:
    """True when A's reporter appears among B's participants and vice versa.

    This is the key signal that two entries describe the same two-person event
    from each participant's perspective.
    """
    # All people recorded in each entry (reporter + listed partners)
    people_a = partners_a_master_ids | {reporter_a_master_id}
    people_b = partners_b_master_ids | {reporter_b_master_id}

    # A's reporter must be a participant in B, and B's reporter in A
    a_in_b = reporter_a_master_id in people_b
    b_in_a = reporter_b_master_id in people_a
    return a_in_b and b_in_a


# ── Interactive prompts ───────────────────────────────────────────────────────

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
        exact = next(
            (p for p in known_people if p.casefold() == partner_name.casefold()), None
        )
        if exact:
            return exact, False
        return partner_name, True

    options = known_people + [f"Create new person named '{partner_name}'"]
    suggestion = get_close_matches(partner_name, known_people, n=1, cutoff=0.75)
    default_idx = known_people.index(suggestion[0]) if suggestion else len(options) - 1
    idx = _prompt_select(
        f"Partner '{partner_name}' appears in source DB. Which master person is this?",
        options,
        default_idx,
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
        exact = next(
            (p for p in existing_canonicals if p.casefold() == position_name.casefold()),
            None,
        )
        if exact:
            return exact, False
        return position_name, True

    options = existing_canonicals + [f"Create new canonical position '{position_name}'"]
    suggestion = get_close_matches(position_name, existing_canonicals, n=1, cutoff=0.72)
    default_idx = (
        existing_canonicals.index(suggestion[0]) if suggestion else len(options) - 1
    )
    idx = _prompt_select(
        f"Position '{position_name}' found in source DB. Map to canonical position:",
        options,
        default_idx,
    )
    if idx == len(options) - 1:
        return position_name, True
    return existing_canonicals[idx], False


# ── Schema ────────────────────────────────────────────────────────────────────

def _initialize_master_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE metadata (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE people (
            person_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name      TEXT NOT NULL UNIQUE
        );

        CREATE TABLE source_databases (
            source_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            source_key      TEXT NOT NULL UNIQUE,
            db_path         TEXT NOT NULL,
            owner_person_id INTEGER NOT NULL,
            FOREIGN KEY (owner_person_id) REFERENCES people(person_id)
        );

        CREATE TABLE source_partners (
            source_id           INTEGER NOT NULL,
            source_partner_id   INTEGER NOT NULL,
            source_partner_name TEXT    NOT NULL,
            mapped_person_id    INTEGER NOT NULL,
            PRIMARY KEY (source_id, source_partner_id),
            FOREIGN KEY (source_id)        REFERENCES source_databases(source_id),
            FOREIGN KEY (mapped_person_id) REFERENCES people(person_id)
        );

        CREATE TABLE canonical_positions (
            canonical_position_id INTEGER PRIMARY KEY AUTOINCREMENT,
            canonical_name        TEXT NOT NULL UNIQUE
        );

        CREATE TABLE source_position_map (
            source_id             INTEGER NOT NULL,
            source_position_id    INTEGER NOT NULL,
            source_position_name  TEXT    NOT NULL,
            canonical_position_id INTEGER NOT NULL,
            PRIMARY KEY (source_id, source_position_id),
            FOREIGN KEY (source_id)             REFERENCES source_databases(source_id),
            FOREIGN KEY (canonical_position_id) REFERENCES canonical_positions(canonical_position_id)
        );

        -- ── Events (one per real-world session) ──────────────────────────────
        CREATE TABLE events (
            event_id         INTEGER PRIMARY KEY,
            event_date       TEXT    NOT NULL,
            approx_duration  INTEGER,
            report_count     INTEGER NOT NULL DEFAULT 0,
            merge_confidence TEXT    -- 'auto', 'manual', or NULL (single-source)
        );

        -- ── Interactions (a specific participant pairing within an event) ────
        -- Each source entry that survives as a distinct pairing becomes one
        -- interaction.  When two entries are auto-merged into one event they
        -- still each produce their own interaction row (one per reporter).
        CREATE TABLE interactions (
            interaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id       INTEGER NOT NULL,
            note           TEXT,    -- optional human label e.g. "Chris + Rashell"
            FOREIGN KEY (event_id) REFERENCES events(event_id)
        );

        CREATE TABLE interaction_participants (
            interaction_id INTEGER NOT NULL,
            person_id      INTEGER NOT NULL,
            PRIMARY KEY (interaction_id, person_id),
            FOREIGN KEY (interaction_id) REFERENCES interactions(interaction_id),
            FOREIGN KEY (person_id)      REFERENCES people(person_id)
        );

        CREATE TABLE interaction_orgasms (
            interaction_id INTEGER NOT NULL,
            person_id      INTEGER NOT NULL,
            count          INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (interaction_id, person_id),
            FOREIGN KEY (interaction_id) REFERENCES interactions(interaction_id),
            FOREIGN KEY (person_id)      REFERENCES people(person_id)
        );

        CREATE TABLE interaction_positions (
            interaction_id        INTEGER NOT NULL,
            canonical_position_id INTEGER NOT NULL,
            PRIMARY KEY (interaction_id, canonical_position_id),
            FOREIGN KEY (interaction_id)        REFERENCES interactions(interaction_id),
            FOREIGN KEY (canonical_position_id) REFERENCES canonical_positions(canonical_position_id)
        );

        CREATE TABLE interaction_places (
            interaction_id INTEGER NOT NULL,
            place_id       INTEGER NOT NULL,
            PRIMARY KEY (interaction_id, place_id),
            FOREIGN KEY (interaction_id) REFERENCES interactions(interaction_id)
        );

        CREATE TABLE interaction_sex_types (
            interaction_id INTEGER NOT NULL,
            sex_type_id    INTEGER NOT NULL,
            PRIMARY KEY (interaction_id, sex_type_id),
            FOREIGN KEY (interaction_id) REFERENCES interactions(interaction_id)
        );

        -- ── Reports (one per source entry — subjective / attributed data) ────
        -- total_org and total_org_partner are intentionally absent here;
        -- orgasm counts live on interaction_orgasms.
        CREATE TABLE event_reports (
            report_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id           INTEGER NOT NULL,
            interaction_id     INTEGER NOT NULL,   -- the interaction this report describes
            source_id          INTEGER NOT NULL,
            source_entry_id    INTEGER NOT NULL,
            reporter_person_id INTEGER NOT NULL,
            duration           INTEGER,
            note               TEXT,
            rating             INTEGER,
            initiator          INTEGER,
            safety_status      INTEGER,
            UNIQUE (source_id, source_entry_id),
            FOREIGN KEY (event_id)           REFERENCES events(event_id),
            FOREIGN KEY (interaction_id)     REFERENCES interactions(interaction_id),
            FOREIGN KEY (source_id)          REFERENCES source_databases(source_id),
            FOREIGN KEY (reporter_person_id) REFERENCES people(person_id)
        );

        -- ── Raw source snapshots (unchanged) ─────────────────────────────────
        CREATE TABLE raw_source_objects (
            source_id   INTEGER NOT NULL,
            object_type TEXT    NOT NULL,
            object_name TEXT    NOT NULL,
            table_name  TEXT,
            sql         TEXT,
            PRIMARY KEY (source_id, object_type, object_name),
            FOREIGN KEY (source_id) REFERENCES source_databases(source_id)
        );

        CREATE TABLE raw_source_columns (
            source_id      INTEGER NOT NULL,
            table_name     TEXT    NOT NULL,
            column_order   INTEGER NOT NULL,
            column_name    TEXT    NOT NULL,
            declared_type  TEXT,
            is_not_null    INTEGER NOT NULL,
            default_value  TEXT,
            is_primary_key INTEGER NOT NULL,
            PRIMARY KEY (source_id, table_name, column_order),
            FOREIGN KEY (source_id) REFERENCES source_databases(source_id)
        );

        CREATE TABLE raw_source_rows (
            source_id    INTEGER NOT NULL,
            table_name   TEXT    NOT NULL,
            source_row_id INTEGER NOT NULL,
            row_data_json TEXT   NOT NULL,
            PRIMARY KEY (source_id, table_name, source_row_id),
            FOREIGN KEY (source_id) REFERENCES source_databases(source_id)
        );
        """
    )


def _insert_metadata(conn: sqlite3.Connection) -> None:
    conn.executemany(
        "INSERT INTO metadata (key, value) VALUES (?, ?)",
        [
            ("schema_version", "5"),
            ("match_strategy", "mutual_cross_reference_plus_duration_tolerance"),
            ("auto_merge_duration_tolerance", str(AUTO_MERGE_DURATION_TOLERANCE)),
            ("source_snapshot_mode", "full_sqlite_object_and_row_snapshot"),
        ],
    )


def _validate_existing_master_schema(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        "SELECT value FROM metadata WHERE key = 'schema_version'"
    ).fetchone()
    if row is None:
        raise ValueError(
            "Existing output DB is missing metadata.schema_version; "
            "not a compatible master DB."
        )
    version = str(row[0])
    if version != "5":
        raise ValueError(
            f"Existing output DB has schema_version={version}, "
            f"but this importer expects schema_version=5. "
            f"Run the appropriate migration script first."
        )


# ── Raw snapshot ──────────────────────────────────────────────────────────────

def _snapshot_source_raw_data(
    master_conn: sqlite3.Connection, source_id: int, db_path: Path
) -> int:
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
            INSERT INTO raw_source_objects
              (source_id, object_type, object_name, table_name, sql)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    source_id,
                    str(row["type"]),
                    str(row["name"]),
                    str(row["tbl_name"]),
                    row["sql"],
                )
                for row in objects
            ],
        )

        tables = [str(row["name"]) for row in objects if str(row["type"]) == "table"]
        total_rows = 0
        for table_name in tables:
            columns = source_conn.execute(
                f"PRAGMA table_info('{table_name}')"
            ).fetchall()
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
                INSERT INTO raw_source_rows
                  (source_id, table_name, source_row_id, row_data_json)
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


# ── Event assignment with confidence matching ─────────────────────────────────

def _resolve_master_partner_ids(
    conn: sqlite3.Connection,
    source_id: int,
    source_partner_ids: tuple[int, ...],
) -> set[int]:
    """Translate source-local partner IDs to master person_ids."""
    result: set[int] = set()
    for spid in source_partner_ids:
        row = conn.execute(
            "SELECT mapped_person_id FROM source_partners "
            "WHERE source_id = ? AND source_partner_id = ?",
            (source_id, spid),
        ).fetchone()
        if row:
            result.add(int(row[0]))
    return result


def _assign_new_entries_to_events(
    conn: sqlite3.Connection,
    new_entries: list[SourceEntryRecord],
    source_ids: dict[str, int],
    existing_event_ids: set[int],
) -> tuple[
    dict[tuple[str, int], int],   # (source_key, entry_id) → event_id
    dict[tuple[str, int], str],   # (source_key, entry_id) → confidence
]:
    """
    Assign each new entry to an event_id.

    High-confidence auto-merge when ALL of:
      1. Same date
      2. Mutual cross-reference (each reporter is in the other's participant list)
      3. Duration within AUTO_MERGE_DURATION_TOLERANCE (or either is None)

    Everything else gets its own new event.

    Returns:
        assignment_map  – (source_key, entry_id) → event_id
        confidence_map  – (source_key, entry_id) → 'auto' | None
    """
    # Pre-resolve master person IDs for all new entries
    reporter_ids: dict[tuple[str, int], int] = {}
    partner_master_ids: dict[tuple[str, int], set[int]] = {}

    for entry in new_entries:
        sid = source_ids[entry.source_key]
        rid = conn.execute(
            "SELECT owner_person_id FROM source_databases WHERE source_id = ?",
            (sid,),
        ).fetchone()[0]
        reporter_ids[(entry.source_key, entry.entry_id)] = int(rid)
        partner_master_ids[(entry.source_key, entry.entry_id)] = (
            _resolve_master_partner_ids(conn, sid, entry.partner_ids)
        )

    # Sort for determinism: process earlier dates first, then by source+entry_id
    sorted_entries = sorted(new_entries, key=lambda e: (e.date, e.source_key, e.entry_id))

    # Candidate pool: new events created during this run
    # { event_id: { date, duration, source_key, entry_key, reporter_id, partner_ids } }
    new_event_pool: dict[int, dict] = {}

    # Load max existing event_id so we don't collide
    row = conn.execute("SELECT MAX(event_id) FROM events").fetchone()
    next_event_id: int = (int(row[0]) + 1) if row[0] is not None else 1

    assignment_map: dict[tuple[str, int], int] = {}
    confidence_map: dict[tuple[str, int], str | None] = {}

    for entry in sorted_entries:
        key = (entry.source_key, entry.entry_id)
        rep_id = reporter_ids[key]
        par_ids = partner_master_ids[key]

        matched_event_id: int | None = None

        # Only try to match against events created in this same run that are
        # not yet from this source (each source can only contribute once per event)
        for ev_id, ev in new_event_pool.items():
            if ev["source_key"] == entry.source_key:
                continue
            if ev["date"] != entry.date:
                continue
            if not _durations_compatible(
                entry.duration, ev["duration"], AUTO_MERGE_DURATION_TOLERANCE
            ):
                continue
            # Mutual cross-reference check
            if _mutual_cross_reference(
                entry, rep_id, par_ids,
                ev["entry"],  # SourceEntryRecord stored for reference
                ev["reporter_id"],
                ev["partner_ids"],
            ):
                matched_event_id = ev_id
                break

        if matched_event_id is not None:
            assignment_map[key] = matched_event_id
            confidence_map[key] = "auto"
        else:
            # New standalone event
            eid = next_event_id
            next_event_id += 1
            new_event_pool[eid] = {
                "date": entry.date,
                "duration": entry.duration,
                "source_key": entry.source_key,
                "entry": entry,
                "reporter_id": rep_id,
                "partner_ids": par_ids,
            }
            assignment_map[key] = eid
            confidence_map[key] = None

    return assignment_map, confidence_map


# ── Interaction insertion ─────────────────────────────────────────────────────

def _insert_interaction(
    conn: sqlite3.Connection,
    event_id: int,
    reporter_master_id: int,
    partner_master_ids: set[int],
    entry: SourceEntryRecord,
    source_id: int,
    canonical_position_ids: list[int],
) -> int:
    """Insert one interaction and all its child rows. Returns interaction_id."""
    cur = conn.execute(
        "INSERT INTO interactions (event_id) VALUES (?)",
        (event_id,),
    )
    interaction_id = int(cur.lastrowid)

    # Participants: reporter + all mapped partners
    all_participants = partner_master_ids | {reporter_master_id}
    for pid in all_participants:
        conn.execute(
            "INSERT OR IGNORE INTO interaction_participants "
            "(interaction_id, person_id) VALUES (?, ?)",
            (interaction_id, pid),
        )

    # Orgasms
    if entry.total_org is not None and entry.total_org > 0:
        conn.execute(
            "INSERT OR IGNORE INTO interaction_orgasms "
            "(interaction_id, person_id, count) VALUES (?, ?, ?)",
            (interaction_id, reporter_master_id, entry.total_org),
        )

    if entry.total_org_partner is not None and entry.total_org_partner > 0 and partner_master_ids:
        # Distribute evenly across partners (best we can do from aggregate data)
        n = len(partner_master_ids)
        base, extra = divmod(entry.total_org_partner, n)
        for i, pid in enumerate(sorted(partner_master_ids)):
            count = base + (1 if i < extra else 0)
            if count > 0:
                # Use INSERT OR REPLACE so that if another entry already set a
                # value (e.g. from a more detailed report), the higher one wins
                existing = conn.execute(
                    "SELECT count FROM interaction_orgasms "
                    "WHERE interaction_id = ? AND person_id = ?",
                    (interaction_id, pid),
                ).fetchone()
                if existing is None:
                    conn.execute(
                        "INSERT INTO interaction_orgasms "
                        "(interaction_id, person_id, count) VALUES (?, ?, ?)",
                        (interaction_id, pid, count),
                    )
                # If already set by this same interaction (shouldn't happen here),
                # leave it — two separate reports on the same event will have
                # separate interactions anyway.

    # Positions
    for cp_id in canonical_position_ids:
        conn.execute(
            "INSERT OR IGNORE INTO interaction_positions "
            "(interaction_id, canonical_position_id) VALUES (?, ?)",
            (interaction_id, cp_id),
        )

    # Places
    for place_id in entry.place_ids:
        conn.execute(
            "INSERT OR IGNORE INTO interaction_places "
            "(interaction_id, place_id) VALUES (?, ?)",
            (interaction_id, place_id),
        )

    # Sex types
    for st_id in entry.sex_type_ids:
        conn.execute(
            "INSERT OR IGNORE INTO interaction_sex_types "
            "(interaction_id, sex_type_id) VALUES (?, ?)",
            (interaction_id, st_id),
        )

    return interaction_id


# ── Main build function ───────────────────────────────────────────────────────

def build_master_database(
    output_path: Path | str,
    sources: list[SourceConfig],
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

    # ── Load source data ──────────────────────────────────────────────────────
    all_entries: list[SourceEntryRecord] = []
    source_partner_names: dict[str, dict[int, str]] = {}
    source_position_names: dict[str, dict[int, str]] = {}

    for source in sources:
        all_entries.extend(_load_source_entries(source.source_key, source.db_path))
        sc = _connect_readonly(source.db_path)
        try:
            source_partner_names[source.source_key] = _load_named_lookup(
                sc, "partners", "partner_id", "name"
            )
            source_position_names[source.source_key] = _load_named_lookup(
                sc, "positions", "position_id", "name"
            )
        finally:
            sc.close()

    conn = sqlite3.connect(out)
    conn.row_factory = sqlite3.Row
    try:
        if creating_new:
            _initialize_master_schema(conn)
            _insert_metadata(conn)
        else:
            _validate_existing_master_schema(conn)

        # ── Register / update source DBs ─────────────────────────────────────
        source_ids: dict[str, int] = {}
        for source in sources:
            person_id = _upsert_person(conn, source.owner_name)
            existing = conn.execute(
                "SELECT source_id FROM source_databases WHERE source_key = ?",
                (source.source_key,),
            ).fetchone()
            if existing:
                source_id = int(existing[0])
                conn.execute(
                    "UPDATE source_databases SET db_path = ?, owner_person_id = ? "
                    "WHERE source_id = ?",
                    (str(source.db_path), person_id, source_id),
                )
            else:
                cur = conn.execute(
                    "INSERT INTO source_databases "
                    "(source_key, db_path, owner_person_id) VALUES (?, ?, ?)",
                    (source.source_key, str(source.db_path), person_id),
                )
                source_id = int(cur.lastrowid)
            source_ids[source.source_key] = source_id

        # ── Refresh raw snapshots ─────────────────────────────────────────────
        raw_rows_copied = 0
        for source in sources:
            sid = source_ids[source.source_key]
            conn.execute("DELETE FROM raw_source_rows    WHERE source_id = ?", (sid,))
            conn.execute("DELETE FROM raw_source_columns WHERE source_id = ?", (sid,))
            conn.execute("DELETE FROM raw_source_objects WHERE source_id = ?", (sid,))
            raw_rows_copied += _snapshot_source_raw_data(
                master_conn=conn, source_id=sid, db_path=source.db_path
            )

        # ── Build person + position mappings ──────────────────────────────────
        canonical_positions: dict[str, int] = {}

        for source in sources:
            sid = source_ids[source.source_key]

            existing_partner_rows = conn.execute(
                "SELECT source_partner_id FROM source_partners WHERE source_id = ?",
                (sid,),
            ).fetchall()
            existing_partner_ids = {int(r[0]) for r in existing_partner_rows}

            for partner_id, partner_name in sorted(
                source_partner_names[source.source_key].items()
            ):
                if partner_id in existing_partner_ids:
                    continue
                known_people = [
                    r["name"]
                    for r in conn.execute(
                        "SELECT name FROM people ORDER BY name"
                    ).fetchall()
                ]
                mapped_name, _ = _choose_partner_mapping(
                    partner_name, known_people, non_interactive
                )
                mapped_id = _upsert_person(conn, mapped_name)
                conn.execute(
                    "INSERT INTO source_partners "
                    "(source_id, source_partner_id, source_partner_name, mapped_person_id) "
                    "VALUES (?, ?, ?, ?)",
                    (sid, partner_id, partner_name, mapped_id),
                )

            existing_position_rows = conn.execute(
                "SELECT source_position_id FROM source_position_map WHERE source_id = ?",
                (sid,),
            ).fetchall()
            existing_position_ids = {int(r[0]) for r in existing_position_rows}

            for position_id, position_name in sorted(
                source_position_names[source.source_key].items()
            ):
                if position_id in existing_position_ids:
                    continue
                if not canonical_positions:
                    rows = conn.execute(
                        "SELECT canonical_name, canonical_position_id "
                        "FROM canonical_positions ORDER BY canonical_name"
                    ).fetchall()
                    canonical_positions = {str(r[0]): int(r[1]) for r in rows}

                canonical_name, _ = _choose_position_mapping(
                    position_name, sorted(canonical_positions.keys()), non_interactive
                )
                if canonical_name not in canonical_positions:
                    cur = conn.execute(
                        "INSERT INTO canonical_positions (canonical_name) VALUES (?)",
                        (canonical_name,),
                    )
                    canonical_positions[canonical_name] = int(cur.lastrowid)

                conn.execute(
                    "INSERT INTO source_position_map "
                    "(source_id, source_position_id, source_position_name, "
                    " canonical_position_id) "
                    "VALUES (?, ?, ?, ?)",
                    (
                        sid,
                        position_id,
                        position_name,
                        canonical_positions[canonical_name],
                    ),
                )

        # ── Filter to genuinely new entries ───────────────────────────────────
        new_entries: list[SourceEntryRecord] = []
        for entry in all_entries:
            sid = source_ids[entry.source_key]
            already = conn.execute(
                "SELECT 1 FROM event_reports "
                "WHERE source_id = ? AND source_entry_id = ?",
                (sid, entry.entry_id),
            ).fetchone()
            if already is None:
                new_entries.append(entry)

        # ── Assign entries to events (with confidence matching) ───────────────
        existing_event_ids = {
            int(r[0])
            for r in conn.execute("SELECT event_id FROM events").fetchall()
        }
        assignment_map, confidence_map = _assign_new_entries_to_events(
            conn, new_entries, source_ids, existing_event_ids
        )

        # ── Insert new events ─────────────────────────────────────────────────
        # Gather all event_ids that were assigned and aren't already in the DB
        new_event_ids = {
            eid
            for eid in assignment_map.values()
            if eid not in existing_event_ids
        }

        # Group entries by event_id to compute approx_duration and confidence
        entries_by_event: dict[int, list[SourceEntryRecord]] = {}
        for entry in new_entries:
            eid = assignment_map[(entry.source_key, entry.entry_id)]
            entries_by_event.setdefault(eid, []).append(entry)

        for eid in sorted(new_event_ids):
            group = entries_by_event.get(eid, [])
            durations = [e.duration for e in group if e.duration is not None]
            approx_dur = int(sum(durations) / len(durations)) if durations else None
            # confidence: 'auto' if any entry in this group was auto-merged
            group_confidences = {
                confidence_map.get((e.source_key, e.entry_id)) for e in group
            }
            merge_conf = "auto" if "auto" in group_confidences else None
            conn.execute(
                "INSERT INTO events "
                "(event_id, event_date, approx_duration, report_count, merge_confidence) "
                "VALUES (?, ?, ?, 0, ?)",
                (eid, group[0].date if group else "", approx_dur, merge_conf),
            )

        # ── Insert reports + interactions ─────────────────────────────────────
        auto_merged = 0
        single_source = 0

        for entry in new_entries:
            sid = source_ids[entry.source_key]
            eid = assignment_map[(entry.source_key, entry.entry_id)]

            reporter_id = int(
                conn.execute(
                    "SELECT owner_person_id FROM source_databases WHERE source_id = ?",
                    (sid,),
                ).fetchone()[0]
            )
            partner_mids = _resolve_master_partner_ids(conn, sid, entry.partner_ids)

            # Resolve canonical position IDs
            canonical_pos_ids: list[int] = []
            for spid in entry.position_ids:
                row = conn.execute(
                    "SELECT canonical_position_id FROM source_position_map "
                    "WHERE source_id = ? AND source_position_id = ?",
                    (sid, spid),
                ).fetchone()
                if row:
                    canonical_pos_ids.append(int(row[0]))

            # Create the interaction
            interaction_id = _insert_interaction(
                conn=conn,
                event_id=eid,
                reporter_master_id=reporter_id,
                partner_master_ids=partner_mids,
                entry=entry,
                source_id=sid,
                canonical_position_ids=canonical_pos_ids,
            )

            # Create the report (subjective fields only)
            conn.execute(
                """
                INSERT INTO event_reports (
                    event_id, interaction_id, source_id, source_entry_id,
                    reporter_person_id, duration, note, rating,
                    initiator, safety_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    eid,
                    interaction_id,
                    sid,
                    entry.entry_id,
                    reporter_id,
                    entry.duration,
                    entry.note,
                    entry.rating,
                    entry.initiator,
                    entry.safety_status,
                ),
            )

            conf = confidence_map.get((entry.source_key, entry.entry_id))
            if conf == "auto":
                auto_merged += 1
            else:
                single_source += 1

        # ── Update report_count on events ─────────────────────────────────────
        conn.execute(
            """
            UPDATE events
            SET report_count = (
                SELECT COUNT(*) FROM event_reports er
                WHERE er.event_id = events.event_id
            )
            """
        )

        conn.commit()

        total_events   = int(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0])
        merged_events  = int(
            conn.execute(
                "SELECT COUNT(*) FROM events WHERE merge_confidence = 'auto'"
            ).fetchone()[0]
        )

        return {
            "sources": len(sources),
            "source_entries_seen": len(all_entries),
            "new_entries_imported": len(new_entries),
            "skipped_existing": len(all_entries) - len(new_entries),
            "total_events": total_events,
            "auto_merged_events": merged_events,
            "single_source_events": total_events - merged_events,
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
) -> dict[str, int]:
    return build_master_database(
        output_path=output_path,
        sources=[
            SourceConfig(source_key="mine", db_path=Path(mine_db_path), owner_name=mine_name),
            SourceConfig(source_key="hers", db_path=Path(hers_db_path), owner_name=hers_name),
        ],
        non_interactive=True,
    )

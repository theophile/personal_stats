from __future__ import annotations

import argparse
from pathlib import Path
import sqlite3

from webapp.merge import SourceConfig, build_master_database


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Import one or more personal-stats SQLite exports into a master database "
            "with normalized event / interaction / report tables.\n\n"
            "Auto-merge: two entries are merged into one event when they share the "
            "same date, their reporters appear in each other's participant lists, and "
            "their durations differ by no more than 20 minutes."
        )
    )
    parser.add_argument(
        "--source",
        action="append",
        required=True,
        help=(
            "Source database spec in the form SOURCE_KEY=DB_PATH. "
            "Example: --source mine=/data/mine.db --source partner=/data/hers.db"
        ),
    )
    parser.add_argument(
        "--owner",
        action="append",
        default=[],
        help=(
            "Owner mapping in the form SOURCE_KEY=PERSON_NAME. "
            "If omitted you will be prompted interactively for each source."
        ),
    )
    parser.add_argument("--out", required=True, help="Output path for the master SQLite database")
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help=(
            "Disable prompts. Person and position mappings fall back to "
            "exact-name matching, otherwise create new canonical records."
        ),
    )
    parser.add_argument(
        "--update-existing",
        action="store_true",
        help=(
            "Update an existing master DB in place. Existing imported source "
            "rows are kept; new source entries are incrementally appended and "
            "raw source snapshots are refreshed."
        ),
    )
    return parser


def _parse_key_value(spec: str, flag_name: str) -> tuple[str, str]:
    if "=" not in spec:
        raise ValueError(f"{flag_name} must use KEY=VALUE format. Received: {spec!r}")
    key, value = spec.split("=", 1)
    key, value = key.strip(), value.strip()
    if not key or not value:
        raise ValueError(f"{flag_name} must have non-empty key/value. Received: {spec!r}")
    return key, value


def _parse_source_spec(spec: str, index: int) -> tuple[str, str]:
    if "=" in spec:
        return _parse_key_value(spec, "--source")
    return f"source{index + 1}", spec.strip()


def _load_existing_owner_names(master_path: Path) -> list[str]:
    if not master_path.exists():
        return []
    conn = sqlite3.connect(master_path)
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT p.name
            FROM source_databases sd
            JOIN people p ON p.person_id = sd.owner_person_id
            ORDER BY p.name
            """
        ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        conn.close()
    return [str(row[0]) for row in rows]


def _prompt_owner_selection(source_key: str, owner_names: list[str]) -> str:
    print(f"Select owner/user ('me') for source '{source_key}':")
    for index, name in enumerate(owner_names, start=1):
        print(f"  {index}) {name}")
    while True:
        choice = input("Select owner number: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(owner_names):
            return owner_names[int(choice) - 1]
        print("Invalid selection. Please enter a valid number.")


def _build_source_configs(args: argparse.Namespace) -> list[SourceConfig]:
    owner_map: dict[str, str] = {}
    for owner_spec in args.owner:
        key, name = _parse_key_value(owner_spec, "--owner")
        owner_map[key] = name

    known_owners: list[str] = []
    if args.update_existing:
        known_owners = _load_existing_owner_names(Path(args.out))

    sources: list[SourceConfig] = []
    seen_keys: set[str] = set()
    for idx, source_spec in enumerate(args.source):
        source_key, db_path_str = _parse_source_spec(source_spec, idx)
        if source_key in seen_keys:
            raise ValueError(f"Duplicate --source key: {source_key!r}")
        seen_keys.add(source_key)

        owner_name = owner_map.get(source_key)
        if not owner_name:
            if args.non_interactive:
                raise ValueError(
                    f"Missing --owner for source {source_key!r} in --non-interactive mode"
                )
            if args.update_existing and known_owners:
                owner_name = _prompt_owner_selection(source_key, known_owners)
            else:
                owner_name = input(
                    f"Who is the owner/user ('me') for source '{source_key}'? "
                ).strip()
            if not owner_name:
                raise ValueError(f"Owner name for source '{source_key}' cannot be empty")

        sources.append(
            SourceConfig(
                source_key=source_key,
                db_path=Path(db_path_str),
                owner_name=owner_name,
            )
        )
    return sources


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    sources = _build_source_configs(args)

    summary = build_master_database(
        output_path=Path(args.out),
        sources=sources,
        non_interactive=args.non_interactive,
        update_existing=args.update_existing,
    )

    print(f"\nMaster database written to: {args.out}")
    print(f"Sources imported:           {summary['sources']}")
    print(f"Source entries seen:        {summary['source_entries_seen']}")
    print(f"New entries imported:       {summary['new_entries_imported']}")
    print(f"Skipped (already present):  {summary['skipped_existing']}")
    print(f"Total events:               {summary['total_events']}")
    print(f"  Auto-merged events:       {summary['auto_merged_events']}")
    print(f"  Single-source events:     {summary['single_source_events']}")
    print(f"Raw source rows preserved:  {summary['raw_rows_copied']}")


if __name__ == "__main__":
    main()

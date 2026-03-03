from __future__ import annotations

import argparse
from pathlib import Path

from webapp.merge import SourceConfig, build_master_database


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Import one or more personal-stats SQLite exports into a scalable master database "
            "with normalized event/report/person tables."
        )
    )
    parser.add_argument(
        "--source",
        action="append",
        required=True,
        help=(
            "Source database spec in the form SOURCE_KEY=DB_PATH. "
            "For single-source imports you may also pass only DB_PATH. "
            "Example: --source mine=/data/mine.db --source wife=/data/wife.db"
        ),
    )
    parser.add_argument(
        "--owner",
        action="append",
        default=[],
        help=(
            "Owner mapping in the form SOURCE_KEY=PERSON_NAME. "
            "If omitted, you will be prompted interactively for each source."
        ),
    )
    parser.add_argument("--out", required=True, help="Output path for the master SQLite database")
    parser.add_argument(
        "--duration-tolerance",
        type=int,
        default=15,
        help="Max duration delta in minutes for considering reports to belong to the same event",
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help=(
            "Disable prompts. Partner/person and position mappings fall back to exact-name matching, "
            "otherwise create new canonical records."
        ),
    )
    return parser


def _parse_key_value(spec: str, flag_name: str) -> tuple[str, str]:
    if "=" not in spec:
        raise ValueError(f"{flag_name} must use KEY=VALUE format. Received: {spec}")
    key, value = spec.split("=", 1)
    key, value = key.strip(), value.strip()
    if not key or not value:
        raise ValueError(f"{flag_name} must have non-empty key/value. Received: {spec}")
    return key, value


def _parse_source_spec(spec: str, index: int) -> tuple[str, str]:
    if "=" in spec:
        return _parse_key_value(spec, "--source")
    return f"source{index + 1}", spec.strip()


def _build_source_configs(args: argparse.Namespace) -> list[SourceConfig]:
    owner_map: dict[str, str] = {}
    for owner_spec in args.owner:
        key, name = _parse_key_value(owner_spec, "--owner")
        owner_map[key] = name

    sources: list[SourceConfig] = []
    seen_keys: set[str] = set()
    for idx, source_spec in enumerate(args.source):
        source_key, db_path = _parse_source_spec(source_spec, idx)
        if not source_key:
            raise ValueError("--source key cannot be empty")
        if source_key in seen_keys:
            raise ValueError(f"Duplicate --source key detected: {source_key}")
        seen_keys.add(source_key)

        if not db_path:
            raise ValueError(f"--source path cannot be empty for key '{source_key}'")

        owner_name = owner_map.get(source_key)
        if not owner_name:
            owner_name = input(f"Who is the owner/user ('me') for source '{source_key}'? ").strip()
            if not owner_name:
                raise ValueError(f"Owner name for source '{source_key}' cannot be empty")

        sources.append(
            SourceConfig(source_key=source_key, db_path=Path(db_path), owner_name=owner_name)
        )
    return sources


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    sources = _build_source_configs(args)

    summary = build_master_database(
        output_path=Path(args.out),
        sources=sources,
        duration_tolerance=args.duration_tolerance,
        non_interactive=args.non_interactive,
    )

    print(f"Master database written to: {args.out}")
    print(f"Sources imported: {summary['sources']}")
    print(f"Event reports imported: {summary['report_count']}")
    print(f"Master events created: {summary['event_count']}")
    print(f"Events with >1 report (matched): {summary['matched_events']}")
    print(f"Single-report events: {summary['single_report_events']}")


if __name__ == "__main__":
    main()

# Personal Stats Webapp Rewrite

This repository includes a NiceGUI-based web application for browsing and charting an immutable personal-stats SQLite export.

## Run

```bash
pip install -r requirements.txt
python main.py
```

Optional environment variables:

- `PERSONAL_STATS_DB_PATH` (default: `ascdatabase2.db`)
- `PERSONAL_STATS_HOST` (default: `0.0.0.0`)
- `PERSONAL_STATS_PORT` (default: `8080`)

## Implemented (current stage)

- Read-only database connector (`mode=ro`) for immutable source DB.
- Schema validation with user-facing diagnostics if expected tables are missing.
- Filtered entry browser with:
  - date range,
  - note keyword,
  - partner,
  - position,
  - place.
- Summary metric cards for filtered datasets (entries, my orgasms, partner orgasms).
- Interactive Plotly charts:
  - partner orgasms over time with 30-day trend,
  - rating distribution histogram,
  - sex streaks timeline (sex vs no-sex streak segments),
  - position frequency bar chart,
  - position-combination frequency chart (top combinations),
  - position UpSet-style combination view,
  - location/room Sankey diagram.
- Export actions:
  - filtered table as CSV,
  - partner-orgasms chart as PNG,
  - structured summary report as JSON (including chart summary metrics).


## Import one or more source databases into a master DB

The new import script builds a **normalized master SQLite DB** designed for growth (multiple users, multiple source DBs, per-report partners, per-report positions, and event grouping).

### Why keep SQLite?

SQLite is still a good fit here:
- local-first and private,
- simple distribution/backups,
- zero server to run,
- enough relational power for the normalized schema.

The key improvement is **schema design**, not engine replacement: the master DB now stores event-level and report-level entities separately, which scales to multi-person / partial-overlap encounters.

### CLI usage

```bash
python merge_databases.py \
  --source /path/to/my.db \
  --update-existing \
  --out single_master.db
```

or with explicit source keys:

```bash
python merge_databases.py \
  --source mine=/path/to/my.db \
  --source wife=/path/to/wife.db \
  --out merged_master.db
```

You will be prompted for:
- who the source owner/user is for each DB (who "me" is),
- partner-identity mapping (e.g., that partner "Alex" in one DB is the same person as source owner "Alex" from another DB),
- position mapping into canonical position names (to handle user-defined position IDs/names across DBs).

For automation you can pass owner names and disable prompts:

```bash
python merge_databases.py \
  --source mine=/path/to/my.db \
  --source wife=/path/to/wife.db \
  --owner mine="Alex" \
  --owner wife="Sam" \
  --out merged_master.db \
  --non-interactive
```

### Master schema highlights

- `people`: canonical identities across all sources.
- `source_databases`: each imported DB + owner person.
- `source_partners`: per-source partner ID/name mapped to canonical `people`.
- `canonical_positions` + `source_position_map`: resolves custom position IDs/names across DBs.
- `events`: grouped likely-shared encounters.
- `event_reports`: one report per source entry (keeps each person's perspective).
- `report_partners`: who each report says was involved (foundation for future orgasm-attribution logic).
- `report_positions` and `report_places`: normalized per-report tags.
- `raw_source_objects`, `raw_source_columns`, `raw_source_rows`: full-fidelity snapshot of source schema objects and row data so currently-unused source fields remain available for future features.

### Matching behavior

- Event grouping currently requires same `date`.
- If both durations exist, they must be within `--duration-tolerance`.
- A source cannot contribute multiple reports to the same grouped event.
- This supports either **single-source imports** (app works with one DB) or multi-source merges.
- For periodic sync from an Android source DB that grows over time, use `--update-existing` to import only new source entries while preserving existing master rows and refreshing raw source snapshots.

## Troubleshooting

If you see an error such as `no such table: entries`, verify that `PERSONAL_STATS_DB_PATH`
points to the exported ASC SQLite file (not a different SQLite database).

You can inspect your DB with:

```bash
sqlite3 "$PERSONAL_STATS_DB_PATH" ".tables"
```

Expected core tables include:
`entries`, `entry_partner`, `entry_position`, `entry_place`, `partners`, and `positions`.

## Next steps

- Saved filter presets
- Multi-chart dashboard/report page layouts
- Better chart export naming and report bundles

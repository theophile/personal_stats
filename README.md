# Personal Stats Webapp Rewrite (Phase 1)

This repository now includes an initial NiceGUI-based web application scaffold.

## Run

```bash
pip install -r requirements.txt
python main.py
```

Optional environment variables:

- `PERSONAL_STATS_DB_PATH` (default: `ascdatabase2.db`)
- `PERSONAL_STATS_HOST` (default: `0.0.0.0`)
- `PERSONAL_STATS_PORT` (default: `8080`)

## What is implemented

- Read-only database connector (`mode=ro`) for immutable source DB.
- Filtered entry browser (date range + note keyword).
- Interactive Plotly chart for partner orgasms with 30-day trend.
- Friendly schema diagnostics in the UI when the selected DB path/file does not contain the expected tables.

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

- Additional filters (partner/position/place)
- Additional dynamic charts and report views
- Export controls for chart/data outputs

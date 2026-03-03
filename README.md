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
  - duration distribution violin/box by partner,
  - partner-orgasm rolling-baseline anomaly detection,
  - sex streaks timeline (sex vs no-sex streak segments),
  - position frequency bar chart,
  - position-combination frequency chart (top combinations),
  - position UpSet-style combination view,
  - location/room Sankey diagram,
  - position association rules chart (support/confidence/lift).
- Export actions:
  - filtered table as CSV,
  - partner-orgasms chart as PNG (including milestone lines),
  - structured summary report as JSON (including chart summary metrics).

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

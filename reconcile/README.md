# reconcile

## What this is for

Reconcile two Parquet files by key columns and export mismatches.

## Inputs

- **Files:** Exactly two Parquet files (`.parquet` / `.pq`), repeat **`-i` / `--input-file`**.
- **Keys:** Repeat **`-k` / `--key-column`**, or use **`--key-columns-json`**.
- **Compare columns:** Optional **`-c` / `--compare-column`** or **`--compare-columns-json`**; default is all shared non-key columns.

## Outputs

Under **`-o` / `--output-directory`** (default: folder of the first input):

- **`reconcile_detail_<timestamp>.<ext>`** — detail report. Use **`-O` / `--output-format` / `--output`**: `csv`, `parquet`, or `excel` (`xlsx`); default is **excel**.
  By default it includes all available non-key columns; use `--output-columns-mode compared_only` to narrow to compared columns only.
- For **`csv`** and **`parquet`** outputs, a separate **`reconcile_stats_<timestamp>.<ext>`** file is also written.
- For **`excel`** output, stats are included in the workbook as a **`stats`** sheet.
- **`--all-data`** adds:
  - **`reconcile_all_data_left_<timestamp>.<ext>`**
  - **`reconcile_all_data_right_<timestamp>.<ext>`**
  Each includes all columns from that input plus reconcile markers (`x_match_row`, `x_break_count`, `x_break_columns`).
- **`--only-breaks-data`** keeps only rows where `x_break_count > 0` in detail output and in `--all-data` outputs.
- **`--write-summary-json`** adds **`reconcile_summary_<timestamp>.json`**.

## Run

```bash
cd modules_data_processor/reconcile
pixi install

pixi run reconcile -- \
  -i /path/a.parquet \
  -i /path/b.parquet \
  -k id \
  --all-data

pixi run help
```

JSON result on stdout:

```bash
pixi run reconcile -- -i a.parquet -i b.parquet -k id -O parquet --all-data -j
```

## Layout

- `src/reconcile/cli.py` — Typer CLI, **`SharedResult`** / **`--json`**
- `src/reconcile/reconcile_core.py` — validation and CLI orchestration
- `src/reconcile/reconcile_engine.py` — DuckDB+pandas reconciliation and output writing

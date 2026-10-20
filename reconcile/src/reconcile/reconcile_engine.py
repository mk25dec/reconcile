"""DuckDB-first reconciliation core for exactly two Parquet inputs."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import duckdb
import numpy as np
import pandas as pd

from shared_lib import add_timestamp_to_path
from shared_lib.atomic_write import write_json_atomic
from shared_lib.dataframe_io import write_dataframe_atomic
from shared_lib.duckdb_sql import sql_double_quoted_ident as _qident
from shared_lib.duckdb_sql import sql_string_literal as _sql_str
from shared_lib.path_utils import ensure_directory, require_file
from shared_lib.time_utils import timestamp_compact_microseconds

SUPPORTED_INPUT_EXT = {".parquet", ".pq"}
_LOG = logging.getLogger(__name__)


def _stamp_output(path: Path) -> Path:
    ts = timestamp_compact_microseconds()
    res = add_timestamp_to_path(path, timestamp=ts)
    if not res.success:
        raise ValueError(res.error or res.message)
    return Path(str(res.data["path"]))


@dataclass
class ReconcileRunConfig:
    input_files: List[Path]
    key_columns: List[str]
    compare_columns: Optional[List[str]]
    output_dir: Path
    output_format: str
    join_result: str
    case_insensitive: bool
    output_columns_mode: str
    output_layout: str
    write_summary_json: bool
    all_data: bool
    only_breaks_data: bool
    threads: int
    memory_limit: str


def preflight_checks(cfg: ReconcileRunConfig) -> None:
    if len(cfg.input_files) != 2:
        raise ValueError("reconcile needs exactly two input files.")
    if not cfg.key_columns:
        raise ValueError("At least one key column is required.")
    if cfg.join_result not in {"outer", "inner", "left", "right"}:
        raise ValueError("join_result must be one of: outer, inner, left, right")
    if cfg.output_columns_mode not in {"compared_only", "all"}:
        raise ValueError("output_columns_mode must be one of: compared_only, all")
    if cfg.output_layout not in {"side_by_side", "stack"}:
        raise ValueError("output_layout must be one of: side_by_side, stack")
    if cfg.output_format not in {"csv", "parquet", "xlsx"}:
        raise ValueError("output_format must be one of: csv, parquet, xlsx")

    for p in cfg.input_files:
        require_file(p)
        suf = p.suffix.lower()
        if suf not in SUPPORTED_INPUT_EXT:
            raise ValueError(f"Unsupported input extension for {p}: expected one of {sorted(SUPPORTED_INPUT_EXT)}")

    ed = ensure_directory(cfg.output_dir)
    if not ed.success:
        raise ValueError(ed.error or ed.message)


def _normalize_column_name(name: str) -> str:
    return str(name).replace("\ufeff", "").strip().lower()


def _scalar_to_join_key_segment(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value)


def _compose_display_key_series(df: pd.DataFrame, key_columns: Sequence[str]) -> pd.Series:
    """Concatenate key column values per row using ``|``, for the leading ``__key__`` column."""
    if not key_columns:
        return pd.Series("", index=df.index, dtype=str)
    parts: List[pd.Series] = []
    for k in key_columns:
        s = df[k] if k in df.columns else pd.Series(pd.NA, index=df.index)
        parts.append(s.map(_scalar_to_join_key_segment))
    out = parts[0].copy()
    for p in parts[1:]:
        out = out + "|" + p
    return out


def _normalize_table_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> List[str]:
    cols = [row[1] for row in conn.execute(f"PRAGMA table_info({_qident(table_name)})").fetchall()]
    normalized = [_normalize_column_name(c) for c in cols]
    dupes = sorted({c for c in normalized if c and normalized.count(c) > 1})
    if dupes:
        raise ValueError(f"Duplicate columns after normalization in {table_name}: {dupes}")
    if normalized != cols:
        select_expr = ", ".join(f"{_qident(old)} AS {_qident(new)}" for old, new in zip(cols, normalized))
        conn.execute(
            f"CREATE OR REPLACE TEMP TABLE {_qident(table_name)} AS "
            f"SELECT {select_expr} FROM {_qident(table_name)}"
        )
    return normalized


def _build_two_file_outputs(
    conn: duckdb.DuckDBPyConnection,
    table_a: str,
    table_b: str,
    file_a: str,
    file_b: str,
    key_columns: Sequence[str],
    compare_cols: Sequence[str],
    join_result: str,
    case_insensitive: bool,
    output_columns_mode: str,
    output_layout: str,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df_a = conn.execute(f"SELECT * FROM {_qident(table_a)}").df()
    df_b = conn.execute(f"SELECT * FROM {_qident(table_b)}").df()

    cols_a_non_key = [c for c in df_a.columns if c not in key_columns]
    cols_b_non_key = [c for c in df_b.columns if c not in key_columns]
    ordered_non_key = cols_a_non_key + [c for c in cols_b_non_key if c not in cols_a_non_key]
    if output_columns_mode == "compared_only":
        display_cols = [c for c in compare_cols if c in ordered_non_key]
    else:
        display_cols = list(compare_cols) + [c for c in ordered_non_key if c not in compare_cols]

    join_keys = [f"__k{i}" for i in range(len(key_columns))]
    a_work = pd.DataFrame(index=df_a.index)
    b_work = pd.DataFrame(index=df_b.index)

    for i, k in enumerate(key_columns):
        a_raw = df_a[k] if k in df_a.columns else pd.Series([None] * len(df_a), index=df_a.index)
        b_raw = df_b[k] if k in df_b.columns else pd.Series([None] * len(df_b), index=df_b.index)
        a_work[f"{k}_A"] = a_raw
        b_work[f"{k}_B"] = b_raw
        if case_insensitive:
            a_work[join_keys[i]] = a_raw.fillna("").astype(str).str.lower()
            b_work[join_keys[i]] = b_raw.fillna("").astype(str).str.lower()
        else:
            a_work[join_keys[i]] = a_raw.fillna("").astype(str)
            b_work[join_keys[i]] = b_raw.fillna("").astype(str)

    for c in ordered_non_key:
        a_work[f"{c}_A"] = df_a[c] if c in df_a.columns else pd.Series([None] * len(df_a), index=df_a.index)
        b_work[f"{c}_B"] = df_b[c] if c in df_b.columns else pd.Series([None] * len(df_b), index=df_b.index)

    merged = pd.merge(
        a_work,
        b_work,
        on=join_keys,
        how="outer",
        indicator=True,
    )
    for k in key_columns:
        merged[k] = merged[f"{k}_A"].combine_first(merged[f"{k}_B"])

    mismatch_map: Dict[str, pd.Series] = {}
    for col in compare_cols:
        ca = f"{col}_A"
        cb = f"{col}_B"
        if ca in merged.columns and cb in merged.columns:
            sa = merged[ca].fillna("").astype(str)
            sb = merged[cb].fillna("").astype(str)
            if case_insensitive:
                sa = sa.str.lower()
                sb = sb.str.lower()
            mismatch_map[col] = sa != sb
        else:
            mismatch_map[col] = pd.Series([True] * len(merged), index=merged.index)

    mismatch_df = pd.DataFrame(mismatch_map, index=merged.index) if mismatch_map else pd.DataFrame(index=merged.index)
    both_mask = merged["_merge"] == "both"
    mismatch_count = mismatch_df.sum(axis=1).astype(int) if not mismatch_df.empty else pd.Series([0] * len(merged), index=merged.index)

    file_stats_df = pd.DataFrame(
        [
            {
                "source_file": file_a,
                "rows_loaded": int(len(df_a)),
                "keys_found_in_both": int(both_mask.sum()),
                "no_break_rows": int((both_mask & (mismatch_count == 0)).sum()),
                "break_rows": int((both_mask & (mismatch_count > 0)).sum()),
                "only_in_file": int((merged["_merge"] == "left_only").sum()),
            },
            {
                "source_file": file_b,
                "rows_loaded": int(len(df_b)),
                "keys_found_in_both": int(both_mask.sum()),
                "no_break_rows": int((both_mask & (mismatch_count == 0)).sum()),
                "break_rows": int((both_mask & (mismatch_count > 0)).sum()),
                "only_in_file": int((merged["_merge"] == "right_only").sum()),
            },
        ]
    )

    merged["x_match_row"] = np.where(
        both_mask,
        "yes",
        np.where(
            merged["_merge"] == "left_only",
            f"only in {file_a}",
            f"only in {file_b}",
        ),
    )
    merged["x_break_count"] = np.where(both_mask, mismatch_count, len(compare_cols)).astype(int)
    if compare_cols:
        break_arr = np.where(mismatch_df.to_numpy(), np.array(compare_cols, dtype=object), "")
        break_cols_series = pd.Series(
            [";".join([v for v in row if v]) for row in break_arr],
            index=merged.index,
        )
        merged["x_break_columns"] = np.where(both_mask, break_cols_series, ";".join(compare_cols))
    else:
        merged["x_break_columns"] = ""

    # Always keep full all-data rows for each input from full outer baseline.
    left_rows_all = merged[merged["_merge"].isin({"both", "left_only"})].copy()
    right_rows_all = merged[merged["_merge"].isin({"both", "right_only"})].copy()
    left_all = pd.DataFrame({k: left_rows_all[k] for k in key_columns})
    right_all = pd.DataFrame({k: right_rows_all[k] for k in key_columns})
    for c in ordered_non_key:
        left_all[c] = left_rows_all.get(f"{c}_A")
        right_all[c] = right_rows_all.get(f"{c}_B")
    left_all["source_file"] = file_a
    right_all["source_file"] = file_b
    left_all["x_match_row"] = left_rows_all["x_match_row"].values
    right_all["x_match_row"] = right_rows_all["x_match_row"].values
    left_all["x_break_count"] = left_rows_all["x_break_count"].values
    right_all["x_break_count"] = right_rows_all["x_break_count"].values
    left_all["x_break_columns"] = left_rows_all["x_break_columns"].values
    right_all["x_break_columns"] = right_rows_all["x_break_columns"].values
    left_all.insert(0, "__key__", _compose_display_key_series(left_all, list(key_columns)))
    right_all.insert(0, "__key__", _compose_display_key_series(right_all, list(key_columns)))
    all_cols = ["__key__"] + list(key_columns) + ordered_non_key + [
        "source_file",
        "x_match_row",
        "x_break_count",
        "x_break_columns",
    ]
    left_all = left_all[all_cols]
    right_all = right_all[all_cols]

    merge_filter_map = {
        "outer": {"both", "left_only", "right_only"},
        "inner": {"both"},
        "left": {"both", "left_only"},
        "right": {"both", "right_only"},
    }
    keep_mask = merged["_merge"].isin(merge_filter_map[join_result])
    merged_f = merged.loc[keep_mask].copy()
    mismatch_f = mismatch_df.loc[keep_mask].copy() if not mismatch_df.empty else pd.DataFrame(index=merged_f.index)

    if output_layout == "stack":
        left_rows = merged_f[merged_f["_merge"].isin({"both", "left_only"})].copy()
        right_rows = merged_f[merged_f["_merge"].isin({"both", "right_only"})].copy()

        left_out = pd.DataFrame({k: left_rows[k] for k in key_columns})
        right_out = pd.DataFrame({k: right_rows[k] for k in key_columns})
        for c in display_cols:
            left_out[c] = left_rows[f"{c}_A"] if f"{c}_A" in left_rows.columns else None
            right_out[c] = right_rows[f"{c}_B"] if f"{c}_B" in right_rows.columns else None

        for frame, source in ((left_out, file_a), (right_out, file_b)):
            frame["source_file"] = source
        left_out["x_match_row"] = left_rows["x_match_row"].values
        right_out["x_match_row"] = right_rows["x_match_row"].values
        left_out["x_break_count"] = left_rows["x_break_count"].values
        right_out["x_break_count"] = right_rows["x_break_count"].values
        left_out["x_break_columns"] = left_rows["x_break_columns"].values
        right_out["x_break_columns"] = right_rows["x_break_columns"].values

        left_out.insert(0, "__key__", _compose_display_key_series(left_out, list(key_columns)))
        right_out.insert(0, "__key__", _compose_display_key_series(right_out, list(key_columns)))
        out_df = pd.concat([left_out, right_out], ignore_index=True, sort=False)
        ordered_cols = ["__key__"] + list(key_columns) + display_cols + [
            "source_file",
            "x_match_row",
            "x_break_count",
            "x_break_columns",
        ]
        return out_df[ordered_cols], pd.DataFrame(), file_stats_df, left_all, right_all

    merged_k = merged_f.copy()
    merged_k.insert(0, "__key__", _compose_display_key_series(merged_k, list(key_columns)))
    out_cols: List[str] = ["__key__"] + list(key_columns)
    for col in display_cols:
        out_cols.extend([f"{col}_A", f"{col}_B"])
    out_cols.extend(["x_match_row", "x_break_count", "x_break_columns"])
    return merged_k[out_cols], mismatch_f, file_stats_df, left_all, right_all


def _write_colored_diff_excel(
    df: pd.DataFrame,
    path: Path,
    compare_cols: Sequence[str],
    mismatch_df: pd.DataFrame,
    stats_df: Optional[pd.DataFrame] = None,
) -> None:
    def _is_missing(v: Any) -> bool:
        # Treat NaN/None and blank strings as missing for display coloring.
        if pd.isna(v):
            return True
        return isinstance(v, str) and v.strip() == ""

    def _safe_cell_value(v: Any) -> Any:
        return None if _is_missing(v) else v

    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="Sheet1", index=False)
        workbook = writer.book
        worksheet = writer.sheets["Sheet1"]
        yellow_fmt = workbook.add_format({"bg_color": "#FFFF00"})
        red_fmt = workbook.add_format({"bg_color": "#FF9999"})
        headers = list(df.columns)
        col_index = {str(h): i for i, h in enumerate(headers) if h is not None}

        side_by_side_mode = any(f"{c}_A" in col_index and f"{c}_B" in col_index for c in compare_cols)
        for data_idx in range(len(df)):
            row_idx = data_idx + 1
            if side_by_side_mode:
                for col in compare_cols:
                    should_color = True
                    if col in mismatch_df.columns and data_idx < len(mismatch_df):
                        should_color = bool(mismatch_df.iloc[data_idx][col])
                    if should_color:
                        key_a = f"{col}_A"
                        key_b = f"{col}_B"
                        val_a = df.iloc[data_idx].get(key_a)
                        val_b = df.iloc[data_idx].get(key_b)
                        miss_a = _is_missing(val_a)
                        miss_b = _is_missing(val_b)

                        # Missing-on-one-side mismatches: mark missing side red, present side yellow.
                        if key_a in col_index:
                            fmt_a = red_fmt if miss_a and not miss_b else yellow_fmt
                            worksheet.write(row_idx, col_index[key_a], _safe_cell_value(val_a), fmt_a)
                        if key_b in col_index:
                            fmt_b = red_fmt if miss_b and not miss_a else yellow_fmt
                            worksheet.write(row_idx, col_index[key_b], _safe_cell_value(val_b), fmt_b)
            else:
                break_cols_raw = df.iloc[data_idx].get("x_break_columns", "")
                break_cols = [c for c in str(break_cols_raw).split(";") if c]
                match_marker = str(df.iloc[data_idx].get("x_match_row", "")).strip().lower()
                only_in_side = match_marker.startswith("only in ")
                for bc in break_cols:
                    if bc in col_index:
                        value = df.iloc[data_idx].get(bc)
                        # In stack layout, "only in <file>" means the counterpart cell is missing;
                        # mark those break columns red even when this side has a value.
                        fmt = red_fmt if only_in_side or _is_missing(value) else yellow_fmt
                        worksheet.write(row_idx, col_index[bc], _safe_cell_value(value), fmt)

        if stats_df is not None and not stats_df.empty:
            stats_df.to_excel(writer, sheet_name="stats", index=False)


def _register_input_as_table(conn: duckdb.DuckDBPyConnection, file_path: Path, table_name: str) -> List[str]:
    ident = _qident(table_name)
    conn.execute(
        f"CREATE OR REPLACE TEMP TABLE {ident} AS "
        f"SELECT * FROM read_parquet({_sql_str(str(file_path))})"
    )
    cols = [row[1] for row in conn.execute(f"PRAGMA table_info({ident})").fetchall()]
    return cols


def reconcile_files(
    cfg: ReconcileRunConfig,
    logger: Optional[logging.Logger] = None,
    on_stage: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    log = logger or _LOG
    if on_stage:
        on_stage("validate")
    preflight_checks(cfg)

    conn = duckdb.connect()
    conn.execute(f"PRAGMA threads={max(1, cfg.threads)}")
    conn.execute(f"PRAGMA memory_limit={_sql_str(cfg.memory_limit)}")

    loaded: List[Tuple[str, str, List[str], int]] = []
    key_columns = [_normalize_column_name(c) for c in cfg.key_columns]
    compare_columns_requested = (
        [_normalize_column_name(c) for c in cfg.compare_columns] if cfg.compare_columns else None
    )

    if on_stage:
        on_stage("read")
    for idx, file_path in enumerate(cfg.input_files):
        table_name = f"in_{idx}"
        cols = _register_input_as_table(conn, file_path, table_name)
        cols = _normalize_table_columns(conn, table_name)
        row_count = int(conn.execute(f"SELECT COUNT(*) FROM {_qident(table_name)}").fetchone()[0])
        loaded.append((table_name, file_path.name, cols, row_count))
        log.info("Loaded %s rows from %s", row_count, file_path.name)

    if on_stage:
        on_stage("reconcile")
    for _, file_name, cols, _ in loaded:
        missing = [k for k in key_columns if k not in cols]
        if missing:
            raise ValueError(f"Key columns {missing} missing in {file_name}")

    cols_a = list(loaded[0][2])
    cols_b = list(loaded[1][2])
    set_a, set_b = set(cols_a), set(cols_b)
    # Union of non-key columns (preserve left file order, then right-only extras).
    # Intersection-only would drop columns present in one file only (e.g. color in b only),
    # so empty vs value would never be compared.
    ordered_union_non_key = [c for c in cols_a if c not in key_columns] + [
        c for c in cols_b if c not in key_columns and c not in set_a
    ]
    if compare_columns_requested:
        compare_cols = [
            c
            for c in compare_columns_requested
            if c not in key_columns and (c in set_a or c in set_b)
        ]
    else:
        compare_cols = list(ordered_union_non_key)
    if not compare_cols:
        raise ValueError("No compare columns: add non-key columns to both files or pass -c/--compare-column.")

    table_a, file_a, _, _ = loaded[0]
    table_b, file_b, _, _ = loaded[1]
    detail_out_df, mismatch_df, stats_sheet_df, left_all_df, right_all_df = _build_two_file_outputs(
        conn=conn,
        table_a=table_a,
        table_b=table_b,
        file_a=file_a,
        file_b=file_b,
        key_columns=key_columns,
        compare_cols=compare_cols,
        join_result=cfg.join_result,
        case_insensitive=cfg.case_insensitive,
        output_columns_mode=cfg.output_columns_mode,
        output_layout=cfg.output_layout,
    )
    if cfg.only_breaks_data:
        detail_out_df = detail_out_df[detail_out_df["x_break_count"] > 0].copy()

    summary = {
        "inputs_requested": [str(p) for p in cfg.input_files],
        "inputs_loaded": [name for _, name, _, _ in loaded],
        "rows_loaded_total": int(sum(r for _, _, _, r in loaded)),
        "detail_rows": int(len(detail_out_df)),
        "compare_columns": compare_cols,
        "key_columns": key_columns,
        "join_result": cfg.join_result,
        "case_insensitive": cfg.case_insensitive,
        "output_columns_mode": cfg.output_columns_mode,
        "output_layout": cfg.output_layout,
        "output_format": cfg.output_format,
        "all_data": cfg.all_data,
        "only_breaks_data": cfg.only_breaks_data,
    }

    detail_path = _stamp_output(cfg.output_dir / f"reconcile_detail.{cfg.output_format}")
    stats_path = _stamp_output(cfg.output_dir / f"reconcile_stats.{cfg.output_format}")
    summary_path = _stamp_output(cfg.output_dir / "reconcile_summary.json")

    if on_stage:
        on_stage("write")
    if cfg.output_format == "xlsx":
        _write_colored_diff_excel(detail_out_df, detail_path, compare_cols, mismatch_df, stats_sheet_df)
    else:
        wr = write_dataframe_atomic(str(detail_path), detail_out_df, output_format=cfg.output_format)
        if not wr.success:
            raise RuntimeError(wr.error or wr.message)
        wr_stats = write_dataframe_atomic(str(stats_path), stats_sheet_df, output_format=cfg.output_format)
        if not wr_stats.success:
            raise RuntimeError(wr_stats.error or wr_stats.message)

    all_data_paths: list[str] = []
    if cfg.all_data:
        if cfg.only_breaks_data:
            left_all_df = left_all_df[left_all_df["x_break_count"] > 0].copy()
            right_all_df = right_all_df[right_all_df["x_break_count"] > 0].copy()
        left_path = _stamp_output(cfg.output_dir / f"reconcile_all_data_left.{cfg.output_format}")
        right_path = _stamp_output(cfg.output_dir / f"reconcile_all_data_right.{cfg.output_format}")
        for frame, out in ((left_all_df, left_path), (right_all_df, right_path)):
            wr = write_dataframe_atomic(str(out), frame, output_format=cfg.output_format)
            if not wr.success:
                raise RuntimeError(wr.error or wr.message)
            all_data_paths.append(str(out))

    summary_path_str: Optional[str] = None
    if cfg.write_summary_json:
        jr = write_json_atomic(str(summary_path), summary, indent=2)
        if not jr.success:
            raise RuntimeError(jr.error or jr.message)
        summary_path_str = str(summary_path)

    log.info("Reconciliation complete. detail=%s summary=%s", detail_path, summary_path_str)
    if on_stage:
        on_stage("done")
    return {
        "summary": summary,
        "detail_path": str(detail_path),
        "stats_path": str(stats_path) if cfg.output_format != "xlsx" else None,
        "summary_path": summary_path_str,
        "all_data_paths": all_data_paths,
    }

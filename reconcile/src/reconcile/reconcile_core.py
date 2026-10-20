"""CLI-facing reconcile orchestration and SharedResult wrapping."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from time import perf_counter
from types import SimpleNamespace
from typing import Any

from shared_lib import ensure_directory, format_duration, validate_input_file
from shared_lib.cli_token_lists import parse_comma_separated_cli_tokens, resolve_comma_or_json_cli_list
from shared_lib.results import EXIT_ERROR, EXIT_VALIDATION_ERROR, SharedResult, failure, success

from .reconcile_engine import ReconcileRunConfig, reconcile_files

_LOG = logging.getLogger(__name__)

SUPPORTED_INPUT_FORMATS = ("parquet",)


def _validate_inputs(paths: list[Path]) -> SharedResult | None:
    for p in paths:
        vr = validate_input_file(
            p,
            input_format=None,
            supported_formats=SUPPORTED_INPUT_FORMATS,
            require_non_empty=False,
        )
        if not vr.success:
            return vr
    return None


def run_reconcile(args: SimpleNamespace) -> SharedResult:
    """Run reconciliation; ``args`` mirrors flags from ``cli.py``."""
    t0 = perf_counter()
    try:
        return _run_reconcile_impl(args, t0)
    except Exception as exc:  # noqa: BLE001
        return failure(str(exc), exit_code=EXIT_ERROR, error=str(exc))


def _run_reconcile_impl(args: SimpleNamespace, t0: float) -> SharedResult:
    raw_inputs = list(getattr(args, "input_files", None) or [])
    flattened = parse_comma_separated_cli_tokens(str(p) for p in raw_inputs)
    if len(flattened) != 2:
        return failure(
            "Pass exactly two input paths: repeat -i/--input-file or use commas in one -i "
            "(e.g. -i left.parquet -i right.parquet or -i 'left.parquet,right.parquet').",
            exit_code=EXIT_VALIDATION_ERROR,
        )
    paths = [Path(p).expanduser().resolve() for p in flattened]

    bad_in = _validate_inputs(paths)
    if bad_in is not None:
        return bad_in

    key_res = resolve_comma_or_json_cli_list(
        list(getattr(args, "key_columns", []) or []),
        getattr(args, "key_columns_json", None),
        mutual_exclusive=True,
        comma_option_desc="-k/--key-column",
        json_option_desc="--key-columns-json",
        data_key="key_columns",
        non_empty=True,
        empty_error="Pass at least one -k/--key-column (or --key-columns-json).",
        json_label="--key-columns-json",
    )
    if not key_res.success:
        return key_res
    key_columns = list(key_res.data["key_columns"])

    compare_res = resolve_comma_or_json_cli_list(
        list(getattr(args, "compare_columns", []) or []),
        getattr(args, "compare_columns_json", None),
        mutual_exclusive=True,
        comma_option_desc="-c/--compare-column",
        json_option_desc="--compare-columns-json",
        data_key="compare_columns",
        non_empty=False,
        empty_error=None,
        json_label="--compare-columns-json",
    )
    if not compare_res.success:
        return compare_res
    compare_flat = list(compare_res.data.get("compare_columns") or [])
    compare_columns = compare_flat if compare_flat else None

    out_arg = getattr(args, "output_directory", None)
    if out_arg is None:
        output_dir = paths[0].parent.resolve()
    else:
        output_dir = Path(out_arg).expanduser().resolve()

    ed = ensure_directory(output_dir)
    if not ed.success:
        return ed

    output_format = str(getattr(args, "output_format", "excel") or "excel").lower().strip()
    if output_format == "excel":
        output_format = "xlsx"
    if output_format not in {"csv", "parquet", "xlsx"}:
        return failure(
            "output_format must be one of: csv, parquet, excel (or xlsx).",
            exit_code=EXIT_VALIDATION_ERROR,
        )

    join_result = str(getattr(args, "join_result", "outer") or "outer").lower().strip()
    if join_result not in {"outer", "inner", "left", "right"}:
        return failure(
            "join_result must be one of: outer, inner, left, right.",
            exit_code=EXIT_VALIDATION_ERROR,
        )

    output_columns_mode = str(getattr(args, "output_columns_mode", "all") or "all").lower().strip()
    if output_columns_mode not in {"compared_only", "all"}:
        return failure(
            "output_columns_mode must be compared_only or all.",
            exit_code=EXIT_VALIDATION_ERROR,
        )

    output_layout = str(getattr(args, "output_layout", "stack") or "stack").lower().strip()
    if output_layout not in {"side_by_side", "stack"}:
        return failure(
            "output_layout must be side_by_side or stack.",
            exit_code=EXIT_VALIDATION_ERROR,
        )

    threads = getattr(args, "threads", None)
    if threads is None:
        threads = max(1, (os.cpu_count() or 4) - 1)
    memory_limit = str(getattr(args, "memory_limit", "4GB") or "4GB").strip()

    cfg = ReconcileRunConfig(
        input_files=paths,
        key_columns=key_columns,
        compare_columns=compare_columns,
        output_dir=output_dir,
        output_format=output_format,
        join_result=join_result,
        case_insensitive=bool(getattr(args, "case_insensitive", False)),
        output_columns_mode=output_columns_mode,
        output_layout=output_layout,
        write_summary_json=bool(getattr(args, "write_summary_json", False)),
        all_data=bool(getattr(args, "all_data", False)),
        only_breaks_data=bool(getattr(args, "only_breaks_data", False)),
        threads=int(threads),
        memory_limit=memory_limit,
    )

    log = _LOG
    if bool(getattr(args, "debug", False)):
        logging.basicConfig(level=logging.DEBUG)
        log.setLevel(logging.DEBUG)

    try:
        result = reconcile_files(cfg, logger=log)
    except (FileNotFoundError, ValueError, OSError, RuntimeError) as exc:
        return failure(str(exc), exit_code=EXIT_VALIDATION_ERROR, error=str(exc))

    duration = perf_counter() - t0
    detail_path = str(result.get("detail_path") or "")
    stats_path = str(result.get("stats_path") or "")
    summary_path = str(result.get("summary_path") or "")
    all_data_paths = [str(p) for p in (result.get("all_data_paths") or []) if p]

    msg = f"Reconciliation wrote detail -> {detail_path}"
    if stats_path:
        msg += f"; stats -> {stats_path}"
    if all_data_paths:
        msg += f"; all-data files -> {len(all_data_paths)}"
    if summary_path:
        msg += f"; summary -> {summary_path}"
    msg += f" ({format_duration(duration)})"

    artifacts: list[dict[str, Any]] = [{"role": "reconcile_detail", "path": detail_path, "format": output_format}]
    if stats_path:
        artifacts.append({"role": "reconcile_stats", "path": stats_path, "format": output_format})
    for idx, p in enumerate(all_data_paths):
        role = "reconcile_all_data_left" if idx == 0 else "reconcile_all_data_right"
        artifacts.append({"role": role, "path": p, "format": output_format})
    if summary_path:
        artifacts.append({"role": "reconcile_summary", "path": summary_path, "format": "json"})

    return success(
        msg,
        duration_sec=round(duration, 4),
        duration_human=format_duration(duration),
        output_directory=str(output_dir),
        detail_path=detail_path,
        stats_path=stats_path or None,
        all_data_paths=all_data_paths,
        summary_path=summary_path or None,
        summary=result.get("summary"),
        artifacts=artifacts,
    )

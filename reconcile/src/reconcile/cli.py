from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Annotated, Optional

import click
import typer

from . import __version__
from .reconcile_core import run_reconcile

from shared_lib.cli_typer import JsonOpt, effective_quiet, finish_cli_shared_result

_APP_HELP = """\
Reconcile two Parquet files by key columns and export differences.

\b• Purpose: Compare row values by key and flag mismatches.
• Input: Exactly two Parquet files and key column(s).
• Output: Timestamped detail output; optional per-input all-data outputs with --all-data.
Use: reconcile compare --help
"""


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"reconcile {__version__}")
        raise typer.Exit(0)


app = typer.Typer(
    name="reconcile",
    help=_APP_HELP,
    no_args_is_help=True,
)


@app.callback()
def _root(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            callback=_version_callback,
            is_eager=True,
            help="Show version and exit.",
        ),
    ] = False,
) -> None:
    """See reconcile --help for purpose, inputs, and outputs."""


@app.command(
    "compare",
    epilog=(
        "Inputs must be exactly two Parquet files (.parquet/.pq). "
        "Output format can be csv, parquet, or excel. Default is excel. "
        "Use --all-data to also write one output file per input file."
    ),
)
def compare_cmd(
    input_files: Annotated[
        list[Path],
        typer.Option(
            "-i",
            "--input-file",
            resolve_path=True,
            help="Input Parquet path(s). Exactly two files total: repeat -i or comma-separate in one -i (e.g. -i 'a.parquet,b.parquet').",
        ),
    ],
    key_columns: Annotated[
        list[str],
        typer.Option(
            "-k",
            "--key-column",
            help="Key column name(s): repeat -k, comma-separate in one -k (e.g. -k 'id,name'), or --key-columns-json.",
        ),
    ] = [],
    key_columns_json: Annotated[
        Optional[str],
        typer.Option(
            "--key-columns-json",
            help='JSON array of key columns, e.g. \'["id","version"]\'. Mutually exclusive with -k.',
        ),
    ] = None,
    compare_columns: Annotated[
        list[str],
        typer.Option(
            "-c",
            "--compare-column",
            help="Columns to compare (repeat or comma-separated). Default: all shared non-key columns.",
        ),
    ] = [],
    compare_columns_json: Annotated[
        Optional[str],
        typer.Option(
            "--compare-columns-json",
            help="JSON array of compare columns. Mutually exclusive with -c.",
        ),
    ] = None,
    output_directory: Annotated[
        Optional[Path],
        typer.Option(
            "-o",
            "--output-directory",
            file_okay=False,
            dir_okay=True,
            resolve_path=True,
            help="Output folder (created if needed). Default: parent folder of the first input.",
        ),
    ] = None,
    output_format: Annotated[
        str,
        typer.Option(
            "-O",
            "--output-format",
            "--output",
            help="Output format for detail and all-data files: csv, parquet, or excel (default).",
            click_type=click.Choice(["csv", "parquet", "excel", "xlsx"], case_sensitive=False),
        ),
    ] = "excel",
    all_data: Annotated[
        bool,
        typer.Option(
            "--all-data/--no-all-data",
            help="Also write one output file per input file with full input columns and reconcile markers.",
        ),
    ] = False,
    only_breaks_data: Annotated[
        bool,
        typer.Option(
            "--only-breaks-data/--no-only-breaks-data",
            help="Keep only rows where x_break_count > 0 in detail output and in --all-data outputs.",
        ),
    ] = False,
    join_result: Annotated[
        str,
        typer.Option(
            "--join-result",
            help="Rows to include in detail output.",
            click_type=click.Choice(["outer", "inner", "left", "right"], case_sensitive=False),
        ),
    ] = "outer",
    case_insensitive: Annotated[
        bool,
        typer.Option(
            "--case-insensitive/--no-case-insensitive",
            help="Compare keys and values without case differences.",
        ),
    ] = False,
    output_columns_mode: Annotated[
        str,
        typer.Option(
            "--output-columns-mode",
            help="Detail columns: compared_only or all available non-key columns.",
            click_type=click.Choice(["compared_only", "all"], case_sensitive=False),
        ),
    ] = "all",
    output_layout: Annotated[
        str,
        typer.Option(
            "--output-layout",
            help="Detail layout: side_by_side or stack.",
            click_type=click.Choice(["side_by_side", "stack"], case_sensitive=False),
        ),
    ] = "stack",
    write_summary_json: Annotated[
        bool,
        typer.Option(
            "--write-summary-json/--no-write-summary-json",
            help="Also write reconcile_summary_<timestamp>.json next to the detail file.",
        ),
    ] = False,
    threads: Annotated[
        Optional[int],
        typer.Option("--threads", min=1, help="DuckDB thread count (default: CPU-based)."),
    ] = None,
    memory_limit: Annotated[
        Optional[str],
        typer.Option("--memory-limit", help='DuckDB memory limit (default "4GB").'),
    ] = None,
    debug: Annotated[bool, typer.Option("--debug/--no-debug", help="Verbose logging.")] = False,
    quiet: Annotated[bool, typer.Option("-q", "--quiet", help="Suppress success line (ignored with --json).")] = False,
    json_out: JsonOpt = False,
) -> None:
    """Reconcile two keyed Parquet files and write outputs."""
    ns = SimpleNamespace(
        input_files=input_files,
        key_columns=key_columns,
        key_columns_json=key_columns_json,
        compare_columns=compare_columns,
        compare_columns_json=compare_columns_json,
        output_directory=output_directory,
        output_format=output_format,
        all_data=all_data,
        only_breaks_data=only_breaks_data,
        join_result=join_result,
        case_insensitive=case_insensitive,
        output_columns_mode=output_columns_mode,
        output_layout=output_layout,
        write_summary_json=write_summary_json,
        threads=threads,
        memory_limit=memory_limit,
        debug=debug,
    )
    result = run_reconcile(ns)
    finish_cli_shared_result(result, json_out=json_out, quiet=effective_quiet(quiet=quiet, json_out=json_out))


def main(argv: list[str] | None = None) -> int:
    try:
        app(args=argv)
        return 0
    except SystemExit as e:
        code = e.code
        if code is None or code is False:
            return 0
        if isinstance(code, int):
            return code
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

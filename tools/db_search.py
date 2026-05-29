# ---
# jupyter:
#   jupytext:
#     formats: py:percent,ipynb
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.3
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # db_search — find values across every table in a Lake Database
#
# Edit the Parameters cell, then Run All. Every database is scanned
# concurrently; only rows matching `SEARCH_WORDS` are kept (matching is
# type-aware per column), and one Excel sheet per matching table is written
# to the abfss container, preceded by a summary sheet.

# %% [markdown]
# ## Parameters
#
# - `SEARCH_WORDS` — values to look for. On string columns they match by
#   substring; on numeric columns each term is parsed as a number and matched
#   exactly; on date/timestamp columns an ISO term (`YYYY-MM-DD`) matches that
#   date. A term that cannot apply to a column's type is skipped for it.
# - `COND` — `"OR"` (any term in any column) or `"AND"` (every term somewhere
#   in the row, possibly across different columns).
# - `DATE_RANGE` — `(start, end)` ISO dates filtering `INGESTED_COL`; either
#   side may be `""` to leave it open; `("", "")` disables the filter.
# - `INGESTED_COL` — ingestion-date column; tables without it are searched
#   unfiltered by date.
# - `ABFSS_OUT` — destination folder, e.g.
#   `abfss://container@account.dfs.core.windows.net/db_search`.
# - `SEARCH_DBS` — databases to scan; empty (`[]`) means every database in
#   the workspace. `MAX_WORKERS` / `CASE_SENSITIVE` / `PROJECT_MATCHED_ONLY` /
#   `MAX_ROWS_PER_SHEET` tune the scan and output.

# %% tags=["parameters"]
from __future__ import annotations

from typing import List, Tuple

SEARCH_WORDS: List[str] = ["example", "2024-01-01", "42"]
COND: str = "OR"
DATE_RANGE: Tuple[str, str] = ("", "")
INGESTED_COL: str = "ingested_at"
ABFSS_OUT: str = "abfss://container@account.dfs.core.windows.net/db_search"
SEARCH_DBS: List[str] = []
MAX_WORKERS: int = 16
CASE_SENSITIVE: bool = False
PROJECT_MATCHED_ONLY: bool = False
MAX_ROWS_PER_SHEET: int = 1_000_000

# %% [markdown]
# ## Library
# The search engine: type-aware predicates, a concurrent per-table scan, and
# the per-sheet Excel writer.

# %%
import datetime as dt
import functools
import logging
import operator
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Dict, List, NamedTuple, Optional, Set, Tuple

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession
    from pyspark.sql.types import StructField

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
LOGGER: logging.Logger = logging.getLogger("db_search")


def parse_number(word: str) -> Optional[float]:
    """Parse a search term as a number.

    Args:
        word: A raw search term.

    Returns:
        The term as an ``int`` or ``float``, or ``None`` if it is not numeric.
    """
    try:
        return int(word)
    except ValueError:
        pass
    try:
        return float(word)
    except ValueError:
        return None


def parse_date(word: str) -> Optional[str]:
    """Validate a search term as an ISO date.

    Args:
        word: A raw search term.

    Returns:
        The term unchanged if it parses as ``YYYY-MM-DD``, else ``None``.
    """
    try:
        dt.datetime.strptime(word, "%Y-%m-%d")
    except ValueError:
        return None
    return word


def word_column_predicate(
    word: str,
    field: "StructField",
    case_sensitive: bool,
) -> "Optional[Column]":
    """Build the predicate "this column matches this word", type-aware.

    Args:
        word: A single search term.
        field: The column's schema field.
        case_sensitive: Whether string matching respects case.

    Returns:
        A boolean ``Column``, or ``None`` when the term cannot apply to the
        column's data type so the caller can skip it.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        NumericType,
        StringType,
        TimestampType,
    )

    column = F.col(field.name)
    dtype = field.dataType

    if isinstance(dtype, StringType):
        if case_sensitive:
            return column.contains(word)
        return F.lower(column).contains(word.lower())

    if isinstance(dtype, NumericType):
        number: Optional[float] = parse_number(word)
        if number is None:
            return None
        return column == F.lit(number)

    if isinstance(dtype, (DateType, TimestampType)):
        iso: Optional[str] = parse_date(word)
        if iso is None:
            return None
        return column.cast("date") == F.to_date(F.lit(iso))

    if isinstance(dtype, BooleanType):
        low: str = word.lower()
        if low in ("true", "false"):
            return column == F.lit(low == "true")
        return None

    return None


def build_row_predicate(
    fields: "List[StructField]",
    words: List[str],
    cond: str,
    case_sensitive: bool,
) -> "Optional[Column]":
    """Fold per-column, per-word predicates into one row filter.

    ``OR`` matches a row if any term hits any searchable column. ``AND``
    matches a row if every term hits somewhere in the row, possibly across
    different columns.

    Args:
        fields: The table's schema fields.
        words: The search terms.
        cond: ``"OR"`` or ``"AND"`` (case-insensitive).
        case_sensitive: Whether string matching respects case.

    Returns:
        A boolean ``Column``, or ``None`` when no term can match any column,
        which means the table cannot match and should be skipped.
    """
    if cond.upper() == "AND":
        word_clauses: "List[Column]" = []
        for word in words:
            per_column: "List[Column]" = [
                predicate
                for predicate in (
                    word_column_predicate(word, field, case_sensitive)
                    for field in fields
                )
                if predicate is not None
            ]
            if not per_column:
                from pyspark.sql import functions as F

                return F.lit(False)
            word_clauses.append(functools.reduce(operator.or_, per_column))
        if not word_clauses:
            return None
        return functools.reduce(operator.and_, word_clauses)

    all_predicates: "List[Column]" = [
        predicate
        for predicate in (
            word_column_predicate(word, field, case_sensitive)
            for word in words
            for field in fields
        )
        if predicate is not None
    ]
    if not all_predicates:
        return None
    return functools.reduce(operator.or_, all_predicates)


def matched_columns(
    fields: "List[StructField]",
    words: List[str],
    case_sensitive: bool,
) -> List[str]:
    """Return the columns that contribute at least one search predicate.

    Args:
        fields: The table's schema fields.
        words: The search terms.
        case_sensitive: Whether string matching respects case.

    Returns:
        The names of columns any term could match, in schema order.
    """
    hit: List[str] = []
    for field in fields:
        for word in words:
            if word_column_predicate(word, field, case_sensitive) is not None:
                hit.append(field.name)
                break
    return hit


def list_databases(spark: "SparkSession", search: List[str]) -> List[str]:
    """Return the databases to scan.

    Args:
        spark: Active SparkSession.
        search: Database names to scan; empty means every database in the
            workspace. Names that do not exist are logged and ignored.

    Returns:
        The database names to scan, in catalog order.
    """
    available: List[str] = [db.name for db in spark.catalog.listDatabases()]
    if not search:
        return available
    available_set: Set[str] = set(available)
    wanted: Set[str] = set(search)
    missing: List[str] = [name for name in search if name not in available_set]
    if missing:
        LOGGER.warning("requested databases not found: %s", missing)
    return [name for name in available if name in wanted]


def list_table_pairs(
    spark: "SparkSession",
    databases: List[str],
) -> "List[Tuple[str, str]]":
    """Return a flat list of ``(database, table)`` pairs to scan."""
    pairs: "List[Tuple[str, str]]" = []
    for db in databases:
        try:
            tables = spark.catalog.listTables(db)
        except Exception as exc:
            LOGGER.warning("skipping db %s: %s", db, exc)
            continue
        for tbl in tables:
            pairs.append((db, tbl.name))
    return pairs


def table_location(spark: "SparkSession", database: str, table: str) -> Optional[str]:
    """Return a table's storage location via ``DESCRIBE FORMATTED``.

    Args:
        spark: Active SparkSession.
        database: Database name.
        table: Table name.

    Returns:
        The table's storage path, or ``None`` if it cannot be discovered.
    """
    try:
        rows = spark.sql(f"DESCRIBE FORMATTED `{database}`.`{table}`").collect()
    except Exception as exc:
        LOGGER.warning("no DESCRIBE for %s.%s: %s", database, table, exc)
        return None
    for row in rows:
        if (row["col_name"] or "").strip() == "Location":
            return (row["data_type"] or "").strip() or None
    return None


def read_table(
    spark: "SparkSession",
    database: str,
    table: str,
) -> "Optional[DataFrame]":
    """Read a table as parquet via its location, falling back to the catalog.

    Reading the parquet path directly skips the metastore but is not
    Delta-aware: a Delta table's folder may hold superseded parquet files, so
    counts can be inflated. The catalog fallback (``spark.table``) keeps such
    tables correct and also covers views and non-parquet formats.

    Args:
        spark: Active SparkSession.
        database: Database name.
        table: Table name.

    Returns:
        The table as a DataFrame, or ``None`` if it cannot be read.
    """
    location: Optional[str] = table_location(spark, database, table)
    if location:
        try:
            return spark.read.parquet(location)
        except Exception as exc:
            LOGGER.warning(
                "parquet read failed for %s.%s (%s); using table read",
                database,
                table,
                exc,
            )
    try:
        return spark.table(f"`{database}`.`{table}`")
    except Exception as exc:
        LOGGER.warning("table read failed for %s.%s: %s", database, table, exc)
        return None


class TableResult(NamedTuple):
    """One matching table and the matched rows.

    Attributes:
        database: Database name.
        table: Table name.
        matched_columns: Columns any search term could match.
        row_count: Number of matched rows.
        message: Human-readable summary of the match.
        frame: The filtered (and optionally projected) DataFrame.
    """

    database: str
    table: str
    matched_columns: List[str]
    row_count: int
    message: str
    frame: "DataFrame"


def apply_date_range(
    df: "DataFrame",
    ingested_col: str,
    date_range: "Tuple[str, str]",
) -> "DataFrame":
    """Filter rows to the ingestion window when the column is present.

    The column is compared at day granularity (cast to ``date``), so both
    bounds are inclusive even for a timestamp column.

    Args:
        df: The table being searched.
        ingested_col: The ingestion-date column name.
        date_range: ``(start, end)`` ISO bounds; either side may be ``""``.

    Returns:
        The date-filtered DataFrame, or ``df`` unchanged if the column is
        absent or both bounds are empty.
    """
    from pyspark.sql import functions as F

    start, end = date_range
    if ingested_col not in df.columns:
        return df
    ingested = F.col(ingested_col).cast("date")
    if start:
        df = df.filter(ingested >= F.to_date(F.lit(start)))
    if end:
        df = df.filter(ingested <= F.to_date(F.lit(end)))
    return df


def search_one_table(
    spark: "SparkSession",
    database: str,
    table: str,
    words: List[str],
    cond: str,
    ingested_col: str,
    date_range: "Tuple[str, str]",
    case_sensitive: bool,
    project_matched_only: bool,
) -> Optional[TableResult]:
    """Search one table and return a :class:`TableResult` if rows match.

    Args:
        spark: Active SparkSession.
        database: Database name.
        table: Table name.
        words: The search terms.
        cond: ``"OR"`` or ``"AND"``.
        ingested_col: The ingestion-date column for the date filter.
        date_range: ``(start, end)`` ISO bounds for the date filter.
        case_sensitive: Whether string matching respects case.
        project_matched_only: Keep only matched columns (plus the ingestion
            column) in the result frame.

    Returns:
        A :class:`TableResult` with at least one matched row, or ``None``.
    """
    df: "Optional[DataFrame]" = read_table(spark, database, table)
    if df is None:
        return None

    fields = list(df.schema.fields)
    predicate = build_row_predicate(fields, words, cond, case_sensitive)
    if predicate is None:
        return None

    matched: List[str] = matched_columns(fields, words, case_sensitive)
    filtered = apply_date_range(df, ingested_col, date_range).filter(predicate)

    if project_matched_only and matched:
        keep: List[str] = list(matched)
        if ingested_col in df.columns and ingested_col not in keep:
            keep.append(ingested_col)
        filtered = filtered.select(*keep)

    count: int = filtered.count()
    if count == 0:
        return None

    message: str = f"{count} rows matched {cond.upper()} {words} in {matched}"
    return TableResult(
        database=database,
        table=table,
        matched_columns=matched,
        row_count=count,
        message=message,
        frame=filtered,
    )


def search_database(
    spark: "SparkSession",
    words: List[str] = SEARCH_WORDS,
    cond: str = COND,
    ingested_col: str = INGESTED_COL,
    date_range: "Tuple[str, str]" = DATE_RANGE,
    search_dbs: List[str] = SEARCH_DBS,
    case_sensitive: bool = CASE_SENSITIVE,
    project_matched_only: bool = PROJECT_MATCHED_ONLY,
    max_workers: int = MAX_WORKERS,
) -> List[TableResult]:
    """Search every table in the lake database concurrently for the terms.

    Mirrors ``tools/db.py``: enumerate ``(database, table)`` pairs from the
    catalog, fan out across a thread pool, and collect the tables that match.

    Args:
        spark: Active SparkSession.
        words: The search terms.
        cond: ``"OR"`` or ``"AND"``.
        ingested_col: The ingestion-date column for the date filter.
        date_range: ``(start, end)`` ISO bounds for the date filter.
        search_dbs: Databases to scan; empty means every database.
        case_sensitive: Whether string matching respects case.
        project_matched_only: Keep only matched columns in each result frame.
        max_workers: Thread-pool size for the per-table scan.

    Returns:
        One :class:`TableResult` per matching table.
    """
    databases: List[str] = list_databases(spark, search_dbs)
    LOGGER.info("Searching %d databases: %s", len(databases), databases)

    pairs: "List[Tuple[str, str]]" = list_table_pairs(spark, databases)
    LOGGER.info("Found %d tables. Spawning %d workers.", len(pairs), max_workers)

    results: List[TableResult] = []
    completed: int = 0

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures: "Dict[Future[Optional[TableResult]], Tuple[str, str]]" = {
            pool.submit(
                search_one_table,
                spark,
                db,
                tbl,
                words,
                cond,
                ingested_col,
                date_range,
                case_sensitive,
                project_matched_only,
            ): (db, tbl)
            for db, tbl in pairs
        }

        for future in as_completed(futures):
            db, tbl = futures[future]
            completed += 1
            try:
                result: Optional[TableResult] = future.result()
                if result is not None:
                    results.append(result)
                    LOGGER.info("  match: %s.%s (%d rows)", db, tbl, result.row_count)
            except Exception as exc:
                LOGGER.warning("  error on %s.%s: %s", db, tbl, exc)

            if completed % 25 == 0 or completed == len(pairs):
                LOGGER.info("  ...%d/%d tables scanned", completed, len(pairs))

    LOGGER.info("Done. %d tables matched.", len(results))
    return results


def safe_sheet_name(name: str, used: Set[str]) -> str:
    """Return an Excel-safe, unique sheet name.

    Excel sheet names are at most 31 characters and may not contain
    ``[]:*?/\\``. Collisions are resolved with a numeric suffix.

    Args:
        name: The desired sheet name (e.g. ``"db.table"``).
        used: Names already taken; the chosen name is added to it.

    Returns:
        A sanitized, unique sheet name.
    """
    invalid: str = "[]:*?/\\"
    cleaned: str = "".join("_" if char in invalid else char for char in name)
    cleaned = cleaned[:31] or "sheet"
    candidate: str = cleaned
    suffix: int = 1
    while candidate in used:
        tail: str = f"_{suffix}"
        candidate = cleaned[: 31 - len(tail)] + tail
        suffix += 1
    used.add(candidate)
    return candidate


def write_workbook(
    results: List[TableResult],
    abfss_out: str,
    run_ts: "dt.datetime",
    max_rows_per_sheet: int = MAX_ROWS_PER_SHEET,
) -> Optional[str]:
    """Write a summary sheet plus one sheet per matching table to abfss.

    Each table's matched rows are collected to the driver with ``toPandas``
    and written into a single ``.xlsx`` (engine ``openpyxl``), which is then
    copied to ``abfss_out`` via ``mssparkutils.fs.cp``. Sheets exceeding
    ``max_rows_per_sheet`` are truncated and the truncation is logged.

    Args:
        results: Output of :func:`search_database`.
        abfss_out: Destination folder on the abfss container.
        run_ts: Timestamp stamped into the file name.
        max_rows_per_sheet: Row cap per sheet (Excel's hard limit is
            1,048,576).

    Returns:
        The destination path written, or ``None`` if there were no matches.
    """
    import pandas as pd

    if not results:
        LOGGER.info("No matches. Nothing written.")
        return None

    stamp: str = run_ts.strftime("%Y%m%d_%H%M%S")
    filename: str = f"db_search_{stamp}.xlsx"
    local_path: str = f"/tmp/{filename}"

    summary = pd.DataFrame(
        {
            "database": [r.database for r in results],
            "table": [r.table for r in results],
            "matched_columns": [", ".join(r.matched_columns) for r in results],
            "count": [r.row_count for r in results],
            "message": [r.message for r in results],
        }
    )

    used: Set[str] = {"summary"}
    with pd.ExcelWriter(local_path, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="summary", index=False)
        for result in results:
            pdf = result.frame.limit(max_rows_per_sheet).toPandas()
            sheet: str = safe_sheet_name(f"{result.database}.{result.table}", used)
            pdf.to_excel(writer, sheet_name=sheet, index=False)
            if result.row_count > max_rows_per_sheet:
                LOGGER.warning(
                    "%s.%s truncated to %d of %d rows for Excel",
                    result.database,
                    result.table,
                    max_rows_per_sheet,
                    result.count,
                )

    dest: str = abfss_out.rstrip("/") + "/" + filename
    mssparkutils.fs.cp(f"file:{local_path}", dest)
    LOGGER.info("Wrote %d sheets to %s", len(results), dest)
    return dest


# %% [markdown]
# ## Run
# `spark` and `mssparkutils` are provided by Synapse. On some runtimes
# `mssparkutils` must be imported first with
# `from notebookutils import mssparkutils`.

# %%
results: "List[TableResult]" = search_database(spark)
out_path: Optional[str] = write_workbook(results, ABFSS_OUT, dt.datetime.now())

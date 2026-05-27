"""Scan all Synapse Spark databases for column names with disallowed characters.

Walks every (database, table) pair concurrently and prints a tidy summary of
any column whose name contains characters outside ``[a-zA-Z0-9_-]``.  Run
this in a Synapse notebook to find the tables blocking Lake Database publish
validation.

Usage — paste into a Synapse notebook cell and run::

    dirty_columns = scan_workspace(spark)
    result_df = report(spark, dirty_columns)
"""
from __future__ import annotations

import re
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

BAD_CHAR: re.Pattern[str] = re.compile(r'[^a-zA-Z0-9_\-]')
SKIP_DBS: Set[str] = {'default', 'information_schema'}
DEFAULT_WORKERS: int = 16


@dataclass
class DirtyColumn:
    database: str
    table: str
    column: str
    bad_chars: str


def list_databases(spark: "SparkSession", skip: Set[str]) -> List[str]:
    """Return all database names except those in the skip set."""
    return [db.name for db in spark.catalog.listDatabases() if db.name not in skip]


def list_table_pairs(
    spark: "SparkSession",
    databases: List[str],
) -> List[Tuple[str, str]]:
    """Return a flat list of (database, table) pairs to scan."""
    pairs: List[Tuple[str, str]] = []
    for db in databases:
        try:
            tables = spark.catalog.listTables(db)
        except Exception as e:
            print(f"  skipping db '{db}': {e}")
            continue
        for tbl in tables:
            pairs.append((db, tbl.name))
    return pairs


def find_dirty_columns_in_table(
    spark: "SparkSession",
    database: str,
    table: str,
    pattern: re.Pattern[str],
) -> List[DirtyColumn]:
    """Scan one table's columns and return any with disallowed characters."""
    results: List[DirtyColumn] = []
    try:
        cols = spark.catalog.listColumns(tableName=table, dbName=database)
    except Exception as e:
        print(f"  skipping {database}.{table}: {e}")
        return results

    for col in cols:
        bad: List[str] = pattern.findall(col.name)
        if bad:
            results.append(DirtyColumn(
                database=database,
                table=table,
                column=col.name,
                bad_chars=''.join(sorted(set(bad))),
            ))
    return results


def scan_workspace(
    spark: "SparkSession",
    skip_dbs: Set[str] = SKIP_DBS,
    pattern: re.Pattern[str] = BAD_CHAR,
    max_workers: int = DEFAULT_WORKERS,
) -> List[DirtyColumn]:
    """Walk all (db, table) pairs concurrently and collect dirty columns."""
    databases: List[str] = list_databases(spark, skip_dbs)
    print(f"Scanning {len(databases)} databases: {databases}")

    pairs: List[Tuple[str, str]] = list_table_pairs(spark, databases)
    print(f"Found {len(pairs)} tables. Spawning {max_workers} workers.\n")

    dirty: List[DirtyColumn] = []
    completed: int = 0

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures: Dict[Future[List[DirtyColumn]], Tuple[str, str]] = {
            pool.submit(find_dirty_columns_in_table, spark, db, tbl, pattern): (db, tbl)
            for db, tbl in pairs
        }

        for fut in as_completed(futures):
            db, tbl = futures[fut]
            completed += 1
            try:
                dirty.extend(fut.result())
            except Exception as e:
                print(f"  unhandled error on {db}.{tbl}: {e}")

            if completed % 25 == 0 or completed == len(pairs):
                print(f"  ...{completed}/{len(pairs)} tables scanned")

    return dirty


def report(spark: "SparkSession", dirty: List[DirtyColumn]) -> "Optional[DataFrame]":
    """Print a summary and return a DataFrame for further use.

    Args:
        spark: Active SparkSession.
        dirty: Output of :func:`scan_workspace`.

    Returns:
        A DataFrame with columns ``database``, ``table``, ``column``,
        ``bad_chars``, or ``None`` if no dirty columns were found.
    """
    from pyspark.sql import Row

    if not dirty:
        print("All columns clean.")
        return None

    rows = [
        Row(database=d.database, table=d.table, column=d.column, bad_chars=d.bad_chars)
        for d in dirty
    ]
    df = spark.createDataFrame(rows)
    df.show(n=500, truncate=False)
    unique_tables: int = len({(d.database, d.table) for d in dirty})
    print(f"\n{len(dirty)} dirty columns across {unique_tables} tables")
    return df


dirty_columns: List[DirtyColumn] = scan_workspace(spark, max_workers=16)
result_df = report(spark, dirty_columns)

# if result_df is not None:
#     result_df.write.mode('overwrite').saveAsTable('audit.dirty_columns')

#!/usr/bin/env python3
"""
Analyze the contents of a Microsoft Access database.

Example:
    python analyze_access_db.py path/to/database.accdb --output-dir results

The script will generate JSON summaries and CSV previews for each table it finds.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List

try:
    import pyodbc
except ModuleNotFoundError as exc:
    print(
        "pyodbc is required to connect to Access databases. "
        "Install it with 'pip install pyodbc'",
        file=sys.stderr,
    )
    raise

try:
    import pandas as pd
except ModuleNotFoundError as exc:
    print(
        "pandas is required for data analysis. "
        "Install it with 'pip install pandas'",
        file=sys.stderr,
    )
    raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize tables in a Microsoft Access database."
    )
    parser.add_argument(
        "db_path",
        type=Path,
        help="Path to the Access database file (.accdb or .mdb).",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=Path("analysis_results"),
        help="Directory where summaries and previews will be written.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=50000,
        help=(
            "Maximum number of rows to read from each table for profiling. "
            "Use 0 to read the entire table."
        ),
    )
    parser.add_argument(
        "--preview-rows",
        type=int,
        default=50,
        help="Number of leading rows to include in each table preview CSV.",
    )
    return parser.parse_args()


def build_connection(db_path: Path) -> pyodbc.Connection:
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={db_path};"
    )
    try:
        return pyodbc.connect(conn_str, timeout=30)
    except pyodbc.InterfaceError as exc:
        print(
            "Unable to connect to the Access database. "
            "Ensure the 'Microsoft Access Database Engine' is installed.",
            file=sys.stderr,
        )
        raise
    except pyodbc.Error:
        raise


def get_table_names(connection: pyodbc.Connection) -> List[str]:
    cursor = connection.cursor()
    tables = []
    for row in cursor.tables(tableType="TABLE"):
        tables.append(row.table_name)
    cursor.close()
    return tables


def sanitize_table_name(table_name: str) -> str:
    safe_chars = []
    for char in table_name:
        if char.isalnum() or char in ("-", "_"):
            safe_chars.append(char)
        else:
            safe_chars.append("_")
    return "".join(safe_chars).strip("_") or "table"


def summarize_numeric_columns(df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    summary: Dict[str, Dict[str, float]] = {}
    numeric_df = df.select_dtypes(include="number")
    for column in numeric_df.columns:
        series = numeric_df[column].dropna()
        if series.empty:
            continue
        summary[column] = {
            "count": int(series.count()),
            "mean": float(series.mean()),
            "min": float(series.min()),
            "max": float(series.max()),
            "std": float(series.std(ddof=0)),
            "median": float(series.median()),
        }
    return summary


def summarize_categorical_columns(
    df: pd.DataFrame, top_n: int = 5
) -> Dict[str, List[Dict[str, object]]]:
    summary: Dict[str, List[Dict[str, object]]] = {}
    categorical_df = df.select_dtypes(exclude="number")
    for column in categorical_df.columns:
        value_counts = categorical_df[column].fillna("<<NULL>>").value_counts()
        top = value_counts.head(top_n)
        summary[column] = [
            {"value": str(idx), "count": int(count)} for idx, count in top.items()
        ]
    return summary


def analyze_table(
    connection: pyodbc.Connection,
    table_name: str,
    max_rows: int,
    preview_rows: int,
) -> Dict[str, object]:
    cursor = connection.cursor()
    count_query = f"SELECT COUNT(*) AS total_rows FROM [{table_name}]"
    total_rows = cursor.execute(count_query).fetchone()[0]
    cursor.close()

    if max_rows and max_rows > 0:
        select_query = f"SELECT TOP {max_rows} * FROM [{table_name}]"
    else:
        select_query = f"SELECT * FROM [{table_name}]"

    df = pd.read_sql(select_query, connection)

    summary: Dict[str, object] = {
        "table_name": table_name,
        "total_rows": int(total_rows),
        "sampled_rows": int(len(df)),
        "columns": {
            column: str(dtype) for column, dtype in df.dtypes.items()
        },
        "numeric_columns": summarize_numeric_columns(df),
        "categorical_columns": summarize_categorical_columns(df),
    }

    preview = df.head(preview_rows)
    return summary, preview


def main() -> int:
    args = parse_args()
    db_path = args.db_path.expanduser().resolve()

    if not db_path.exists():
        print(f"Database file not found: {db_path}", file=sys.stderr)
        return 1

    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        connection = build_connection(db_path)
    except pyodbc.Error as exc:
        print(f"Database connection failed: {exc}", file=sys.stderr)
        return 1

    tables = get_table_names(connection)
    if not tables:
        print("No tables found in the Access database.", file=sys.stderr)
        connection.close()
        return 1

    overall_summary = {
        "database": str(db_path),
        "tables": [],
    }

    for table_name in tables:
        print(f"Analyzing table: {table_name}")
        try:
            table_summary, preview = analyze_table(
                connection, table_name, args.max_rows, args.preview_rows
            )
        except Exception as exc:
            print(f"Failed to analyze table '{table_name}': {exc}", file=sys.stderr)
            continue

        safe_name = sanitize_table_name(table_name)
        summary_path = output_dir / f"{safe_name}_summary.json"
        preview_path = output_dir / f"{safe_name}_preview.csv"

        with summary_path.open("w", encoding="utf-8") as fh:
            json.dump(table_summary, fh, indent=2)

        preview.to_csv(preview_path, index=False)
        overall_summary["tables"].append(table_summary)

    overall_summary_path = output_dir / "database_summary.json"
    with overall_summary_path.open("w", encoding="utf-8") as fh:
        json.dump(overall_summary, fh, indent=2)

    connection.close()
    print(f"Analysis complete. Results saved to: {output_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())


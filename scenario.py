"""Module to update data in datapackages from external datasets."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

import duckdb
from frictionless import Package

import settings


def apply_element_data(
    data_path: Path | str,
    datapackage_name: str,
    scenario: str,
    datapackage_dir: Path = settings.DATAPACKAGE_DIR,
    scenario_column: str = "scenario",
    var_name_col: str = "var_name",
    var_value_col: str = "var_value",
) -> None:
    """Apply scenario data from CSV to an existing datapackage using DuckDB."""
    con = duckdb.connect(database=":memory:")

    # Register data CSV as a table and filter by scenario
    con.execute(
        f"CREATE TABLE raw_table AS SELECT * FROM read_csv_auto('{data_path}', sep=';', all_varchar=True) "
        f"WHERE {scenario_column} = '{scenario}' OR {scenario_column} = 'ALL'",
    )

    # Check if raw_table contains var_name and var_value columns
    columns = [
        col[1] for col in con.execute("PRAGMA table_info('raw_table')").fetchall()
    ]
    is_single_format = var_name_col in columns and var_value_col in columns

    if is_single_format:
        # Single format: pivot the data_table to get one row per name
        con.execute(
            f"CREATE TABLE data_table AS PIVOT ("
            f"SELECT name, {scenario_column}, {var_name_col}, {var_value_col} FROM raw_table"
            f") ON {var_name_col} USING ANY_VALUE({var_value_col})",
        )
    else:
        con.execute("CREATE TABLE data_table AS SELECT * FROM raw_table")

    pkg_path = datapackage_dir / datapackage_name / "datapackage.json"
    pkg = Package(pkg_path, allow_invalid=True)
    for res in pkg.resources:
        if "sequences" in res.path:
            continue

        res_full_path = datapackage_dir / datapackage_name / res.path

        # Load resource into a table
        con.execute(
            f"CREATE TABLE resource_table AS SELECT * FROM read_csv_auto('{res_full_path}', sep=';', all_varchar=True)",
        )

        # Find matching columns
        update_cols = _get_update_columns(
            con,
            excluded_columns=["name", scenario_column, "id"],
        )
        if update_cols:
            set_clause = ", ".join(
                [f"{col} = data_table.{col}" for col in update_cols],
            )
            con.execute(
                f"UPDATE resource_table SET {set_clause} FROM data_table WHERE resource_table.name = data_table.name",
            )

            # Save back to CSV
            con.execute(
                f"COPY resource_table TO '{res_full_path}' (HEADER, DELIMITER ';')",
            )

        con.execute("DROP TABLE IF EXISTS resource_table")


def apply_sequence_data(  # noqa: PLR0913
    data_path: Path | str,
    datapackage_name: str,
    sequence_name: str,
    datapackage_dir: Path = settings.DATAPACKAGE_DIR,
    scenario: str = "ALL",
    scenario_column: str = "scenario",
    var_name_col: str = "var_name",
    series_col: str = "series",
) -> None:
    """Apply scenario data from CSV to an existing datapackage."""
    res_full_path = _get_resource_by_name(
        datapackage_name,
        sequence_name,
        datapackage_dir,
    )

    con = duckdb.connect(database=":memory:")

    columns = [
        column[0]
        for column in con.execute(
            f"DESCRIBE SELECT * FROM read_csv_auto('{data_path}', sep=';', all_varchar=True)",
        ).fetchall()
    ]
    if var_name_col in columns and series_col in columns:
        _apply_sequence_data_rowwise(
            data_path=data_path,
            resource_path=res_full_path,
            scenario=scenario,
            scenario_column=scenario_column,
            var_name_col=var_name_col,
            series_col=series_col,
        )
    _apply_sequence_data_columnwise(data_path, res_full_path)


def _apply_sequence_data_columnwise(data_path: Path | str, resource_path: Path) -> None:
    """Apply scenario data from CSV to an existing datapackage."""
    con = duckdb.connect(database=":memory:")
    # Load source data
    con.execute(
        f"CREATE TABLE data_table AS SELECT * FROM read_csv_auto('{data_path}', sep=';', all_varchar=True)",
    )

    # Load existing resource data
    con.execute(
        f"CREATE TABLE resource_table AS SELECT * FROM read_csv_auto('{resource_path}', sep=';', all_varchar=True)",
    )

    # Find matching columns
    update_cols = _get_update_columns(con, excluded_columns=["timeindex"])
    if update_cols:
        set_clause = ", ".join(
            [f"{col} = data_table.{col}" for col in update_cols],
        )
        con.execute(
            f"UPDATE resource_table SET {set_clause} FROM data_table "
            f"WHERE resource_table.timeindex = data_table.timeindex",
        )

        # Save back to CSV
        con.execute(
            f"COPY resource_table TO '{resource_path}' (HEADER, DELIMITER ';')",
        )


def _apply_sequence_data_rowwise(
    data_path: Path | str,
    resource_path: Path,
    scenario: str = "ALL",
    scenario_column: str = "scenario_key",
    var_name_col: str = "var_name",
    series_col: str = "series",
) -> None:
    """
    Apply scenario data from CSV to an existing datapackage.

    Matches 'var_name_col' from 'data_path' with the column name in 'sequence_name'.
    Filters by 'scenario' in 'scenario_column'.
    The 'series_col' column must contain a list of values (e.g. '[1.0, 2.0, 3.0]').
    """
    con = duckdb.connect(database=":memory:")

    # Load source data and filter by scenario
    con.execute(
        f"CREATE TABLE raw_table AS SELECT * FROM read_csv_auto('{data_path}', sep=';', all_varchar=True) "
        f"WHERE {scenario_column} = '{scenario}' OR {scenario_column} = 'ALL'",
    )

    # Load existing resource data
    con.execute(
        f"CREATE TABLE resource_table AS SELECT * FROM read_csv_auto('{resource_path}', sep=';', all_varchar=True)",
    )

    # Get all column names from resource_table except timeindex
    res_columns = [
        col[1]
        for col in con.execute("PRAGMA table_info('resource_table')").fetchall()
        if col[1] != "timeindex"
    ]

    # For each matching var_name in raw_table that exists as a column in resource_table
    matching_vars = con.execute(
        f"SELECT {var_name_col}, {series_col} FROM raw_table",
    ).fetchall()

    for var_name, series_str in matching_vars:
        if var_name in res_columns:
            # Parse the series_str which is a JSON-like list: "[val1, val2, ...]"
            # We can use duckdb's json functions or just python's json.loads
            series_list = json.loads(series_str)

            # We need to update the column 'var_name' in 'resource_table'
            # The resource_table has a 'timeindex' column. We assume the order of series_list
            # matches the order of rows in resource_table.
            # To do this safely in SQL, we can create a temporary table with the series data and join it.

            con.execute(
                "CREATE OR REPLACE TABLE series_data (idx INTEGER, val VARCHAR)",
            )
            con.executemany(
                "INSERT INTO series_data VALUES (?, ?)",
                [(i, str(val)) for i, val in enumerate(series_list)],
            )

            # Update resource_table using a CTE or temporary table with row numbers
            con.execute(
                f"""
                WITH numbered_resource AS (
                    SELECT timeindex, row_number() OVER () - 1 as row_idx FROM resource_table
                )
                UPDATE resource_table
                SET "{var_name}" = series_data.val
                FROM numbered_resource, series_data
                WHERE numbered_resource.row_idx = series_data.idx
                AND resource_table.timeindex = numbered_resource.timeindex
                """,
            )

    # Save back to CSV
    con.execute(f"COPY resource_table TO '{resource_path}' (HEADER, DELIMITER ';')")


def _get_update_columns(
    con: duckdb.DuckDBPyConnection,
    excluded_columns: list[str],
) -> list[str]:
    res_columns = [
        col[1] for col in con.execute("PRAGMA table_info('resource_table')").fetchall()
    ]

    # Find which columns from source_table exist in resource_table (excluding name, scenario, id, etc.)
    source_columns = [
        col[1] for col in con.execute("PRAGMA table_info('data_table')").fetchall()
    ]
    update_cols = [
        col
        for col in source_columns
        if col in res_columns and col not in excluded_columns
    ]
    return update_cols


def _get_resource_by_name(
    datapackage_name: str,
    sequence_name: str,
    datapackage_dir: Path = settings.DATAPACKAGE_DIR,
) -> Path:
    """Find and return a datapackage resource by name."""
    pkg_path = datapackage_dir / datapackage_name / "datapackage.json"
    pkg = Package(pkg_path, allow_invalid=True)

    # Find the resource by name
    res = None
    for resource in pkg.resources:
        if resource.name == sequence_name:
            res = resource
            break

    if res is None:
        msg = f"Resource '{sequence_name}' not found in datapackage."
        raise ValueError(msg)

    res_full_path = datapackage_dir / datapackage_name / res.path
    return res_full_path

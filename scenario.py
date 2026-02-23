"""Module to generate data packages from scenario files."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import datetime

import json
from pathlib import Path

import duckdb
import yaml
from frictionless import Package

import settings
from builder import (
    Component,
    ElementResourceBuilder,
    PackageBuilder,
    SequenceResourceBuilder,
    hourly_range,
)


def create_scenario(
    scenario_name: str,
    scenario_dir: Path = settings.SCENARIO_DIR,
    datapackage_dir: Path = settings.DATAPACKAGE_DIR,
) -> None:
    """Read scenario from scenario folder and create a datapackage from it."""
    scenario_path = scenario_dir / f"{scenario_name}.yaml"
    with scenario_path.open("r") as f:
        scenario_data = yaml.safe_load(f)

    builder = PackageBuilder(scenario_name, datapackage_dir)

    _create_elements(builder, scenario_data)
    builder.infer_sequences_from_resources()
    _create_sequences(builder, scenario_data)
    builder.infer_busses_from_resources()
    builder.save_package()


def _create_elements(builder: PackageBuilder, scenario_data: dict) -> None:
    """Add elements from scenario data to package builder."""
    regions = scenario_data.get("regions")
    elements = scenario_data.get("elements", {})
    for res_name, config in elements.items():
        component_type = config.get("component")
        component = Component.from_name(component_type)
        sequences = config.get("sequences", [])
        attributes = config.get("attributes", [])
        if len(attributes) == 0:
            attributes = component.attributes

        if regions and "region" not in attributes:
            attributes.append("region")

        # Create resource builder
        resource = ElementResourceBuilder(
            component_name=component_type,
            resource_name=res_name,
            selected_attributes=attributes,
            sequences=sequences,
        )
        _add_instances(resource, config, attributes, regions)

        builder.add_resource(resource)


def _add_instances(  # noqa: C901
    resource: ElementResourceBuilder,
    config: dict,
    attributes: list[str],
    regions: list[str] | None,
) -> None:
    """Add all instances from scenario data to element resource."""

    def check_instance_attributes(instance_data: dict) -> None:
        """Check if all attributes are available."""
        for attr in instance_data:
            if attr not in attributes:
                raise KeyError(
                    f"Attribute '{attr}' not available for '{resource.name}'.",
                )

    def add_default_sequence_foreign_keys(instance_data: dict) -> dict:
        """Add default foreign keys to instance data."""
        for sequence_field in set(sequences + component.sequences):
            if sequence_field in instance_data or sequence_field not in attributes:
                continue
            instance_data[sequence_field] = f"{instance_data['name']}-profile"
        return instance_data

    def adapt_regions_in_busses(instance_data: dict) -> dict:
        """If region is set, bus names are adapted to be region-dependent."""
        for bus_attr in component.busses:
            if bus_attr in instance_data:
                copied_instance[bus_attr] = f"{region}-{copied_instance[bus_attr]}"
        return instance_data

    component_type = config.get("component")
    component = Component.from_name(component_type)
    instances = config.get("instances", [])
    sequences = config.get("sequences", [])

    # Add all instances to resource
    instance_regions = config.get("regions", regions)
    if instance_regions is None:
        # Instances are region-independent
        for instance in instances:
            check_instance_attributes(instance)
            instance_with_fks = add_default_sequence_foreign_keys(instance)
            resource.add_instance(instance_with_fks)
    else:
        for instance in instances:
            check_instance_attributes(instance)
            for region in instance_regions:
                copied_instance = instance.copy()
                copied_instance["region"] = region
                copied_instance["name"] = f"{region}-{copied_instance['name']}"
                instance_with_fks = add_default_sequence_foreign_keys(
                    copied_instance,
                )
                instance_with_region_busses = adapt_regions_in_busses(instance_with_fks)
                resource.add_instance(instance_with_region_busses)


def _create_sequences(builder: PackageBuilder, scenario_data: dict) -> None:
    """Add sequences from scenario data to package builder."""
    timeindex_info = scenario_data.get("timeindex", {})
    timeindex = list(
        (
            hourly_range(timeindex_info["start"], timeindex_info["periods"])
            if timeindex_info
            else None
        ),
    )

    # Add sequences explicitly set in scenario file
    sequences = scenario_data.get("sequences", {})
    for res_name, config in sequences.items():
        # Create resource builder
        resource = SequenceResourceBuilder(resource_name=res_name, timeindex=timeindex)

        # Add all instances to resource if timeindex exists
        if timeindex:
            columns = config.get("columns", [])
            for column in columns:
                resource.add_instance(column, [0 for _ in range(len(timeindex))])

        builder.add_resource(resource)

    _add_default_profiles(builder, timeindex)


def _add_default_profiles(builder: PackageBuilder, timeindex: list[datetime]) -> None:
    """Add default sequences from resource instances."""
    for resource in list(builder.resources.values()):
        if isinstance(resource, SequenceResourceBuilder):
            continue
        sequences = set()
        for foreign_key in resource.foreign_keys:
            if foreign_key["reference"]["resource"] == "bus":
                continue
            column = foreign_key["fields"]
            sequence_name = foreign_key["reference"]["resource"]
            for instance in resource.instances:
                if column not in instance:
                    continue
                sequences.add(instance[column])
        if sequences:
            for sequence in sorted(sequences):
                if sequence_name not in builder.resources:
                    builder.add_resource(
                        SequenceResourceBuilder(sequence_name, timeindex),
                    )
                builder.resources[sequence_name].add_instance(
                    sequence,
                    [0 for _ in range(len(timeindex))],
                )


def _get_component_names_and_paths_from_datapackage(
    datapackage: Package,
) -> dict[str, str]:
    """Read all names from element resources."""
    con = duckdb.connect(database=":memory:")

    name_to_path = {}
    for resource in datapackage.resources:
        if "sequences" in resource.path:
            # Skip sequences as they do not have a name column
            continue
        full_path = Path(datapackage.basepath) / resource.path
        names = con.execute(
            f"SELECT name FROM read_csv_auto('{full_path}', sep=';')",
        ).fetchall()
        for name in names:
            name_to_path[name[0]] = resource.path
    return name_to_path


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
    pkg_path = datapackage_dir / datapackage_name / "datapackage.json"
    pkg = Package(pkg_path, allow_invalid=True)

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


def apply_sequence_data(
    data_path: Path | str,
    datapackage_name: str,
    sequence_name: str,
    datapackage_dir: Path = settings.DATAPACKAGE_DIR,
) -> None:
    """Apply scenario data from CSV to an existing datapackage."""
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

    con = duckdb.connect(database=":memory:")

    # Load source data
    con.execute(
        f"CREATE TABLE data_table AS SELECT * FROM read_csv_auto('{data_path}', sep=';', all_varchar=True)",
    )

    # Load existing resource data
    con.execute(
        f"CREATE TABLE resource_table AS SELECT * FROM read_csv_auto('{res_full_path}', sep=';', all_varchar=True)",
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
            f"COPY resource_table TO '{res_full_path}' (HEADER, DELIMITER ';')",
        )


def apply_sequence_data_rowwise(  # noqa: PLR0913
    data_path: Path | str,
    datapackage_name: str,
    sequence_name: str,
    datapackage_dir: Path = settings.DATAPACKAGE_DIR,
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

    con = duckdb.connect(database=":memory:")

    # Load source data and filter by scenario
    con.execute(
        f"CREATE TABLE raw_table AS SELECT * FROM read_csv_auto('{data_path}', sep=';', all_varchar=True) "
        f"WHERE {scenario_column} = '{scenario}' OR {scenario_column} = 'ALL'",
    )

    # Load existing resource data
    con.execute(
        f"CREATE TABLE resource_table AS SELECT * FROM read_csv_auto('{res_full_path}', sep=';', all_varchar=True)",
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
    con.execute(f"COPY resource_table TO '{res_full_path}' (HEADER, DELIMITER ';')")


if __name__ == "__main__":
    create_scenario("regions")
    apply_element_data("raw/single.csv", "regions", "test")
    apply_element_data("raw/multiple.csv", "regions", "test")
    apply_sequence_data("raw/timeseries.csv", "regions", "liion_storage_profile")

"""Module to generate data packages from scenario files."""

from __future__ import annotations

import duckdb
import yaml

from pathlib import Path
import settings
from builder import PackageBuilder, ElementResourceBuilder, Component
from frictionless import Package


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

    elements = scenario_data.get("elements", {})
    for res_name, config in elements.items():
        component_type = config.get("component")
        instances = config.get("instances", [])
        sequences = config.get("sequences", [])
        attributes = config.get("attributes", [])
        if len(attributes) == 0:
            attributes = Component.from_name(component_type).attributes

        # Create resource builder
        resource = ElementResourceBuilder(
            component_name=component_type,
            resource_name=res_name,
            selected_attributes=attributes,
            sequences=sequences,
        )

        # Add all instances to resource
        for instance in instances:
            resource.add_instance(instance)

        builder.add_resource(resource)

    builder.infer_sequences_from_resources()
    builder.save_package()


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
            f"SELECT name FROM read_csv_auto('{full_path}', sep=';')",  # noqa: S608
        ).fetchall()
        for name in names:
            name_to_path[name[0]] = resource.path
    return name_to_path


def _get_update_columns(con: duckdb.DuckDBPyConnection) -> list[str]:
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
        if col in res_columns and col not in ["name", "scenario", "id"]
    ]
    return update_cols


def apply_scenario_data(
    data_path: Path | str,
    datapackage_name: str,
    scenario_key: str,
    datapackage_dir: Path = settings.DATAPACKAGE_DIR,
    var_name_col: str = "var_name",
    var_value_col: str = "var_value",
) -> None:
    """Apply scenario data from CSV to an existing datapackage using DuckDB."""
    pkg_path = datapackage_dir / datapackage_name / "datapackage.json"
    pkg = Package(pkg_path, allow_invalid=True)

    con = duckdb.connect(database=":memory:")

    # Register data CSV as a table and filter by scenario
    con.execute(
        f"CREATE TABLE raw_table AS SELECT * FROM read_csv_auto('{data_path}', sep=';', all_varchar=True) "  # noqa: S608
        f"WHERE scenario = '{scenario_key}' OR scenario = 'ALL'",
    )

    # Check if raw_table contains var_name and var_value columns
    columns = [
        col[1] for col in con.execute("PRAGMA table_info('raw_table')").fetchall()
    ]
    is_single_format = var_name_col in columns and var_value_col in columns

    if is_single_format:
        # Single format: pivot the data_table to get one row per name
        con.execute(
            f"CREATE TABLE data_table AS PIVOT raw_table ON {var_name_col} USING ANY_VALUE({var_value_col})",
        )
    else:
        con.execute("CREATE TABLE data_table AS SELECT * FROM raw_table")

    for res in pkg.resources:
        if "sequences" in res.path:
            continue

        res_full_path = datapackage_dir / datapackage_name / res.path

        # Load resource into a table
        con.execute(
            f"CREATE TABLE resource_table AS SELECT * FROM read_csv_auto('{res_full_path}', sep=';', all_varchar=True)",  # noqa: S608
        )

        update_cols = _get_update_columns(con)
        if update_cols:
            set_clause = ", ".join(
                [f"{col} = data_table.{col}" for col in update_cols],
            )
            con.execute(
                f"UPDATE resource_table SET {set_clause} FROM data_table WHERE resource_table.name = data_table.name",  # noqa: S608
            )

            # Save back to CSV
            con.execute(
                f"COPY resource_table TO '{res_full_path}' (HEADER, DELIMITER ';')",
            )

        con.execute("DROP TABLE IF EXISTS resource_table")


if __name__ == "__main__":
    create_scenario("test")
    apply_scenario_data("raw/single.csv", "test", "test")
    apply_scenario_data("raw/multiple.csv", "test", "test")

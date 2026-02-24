"""Module to generate data packages from scenario files."""

from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from datetime import datetime
    from frictionless import Package

from pathlib import Path

import duckdb
import yaml

import settings
from builder import (
    Component,
    ElementResourceBuilder,
    PackageBuilder,
    SequenceResourceBuilder,
    hourly_range,
)


def create_blueprint(
    blueprint_name: str,
    blueprint_dir: Path = settings.BLUEPRINT_DIR,
    datapackage_dir: Path = settings.DATAPACKAGE_DIR,
) -> None:
    """Read scenario from scenario folder and create a datapackage from it."""
    blueprint_path = blueprint_dir / f"{blueprint_name}.yaml"
    with blueprint_path.open("r") as f:
        blueprint_data = yaml.safe_load(f)

    builder = PackageBuilder(blueprint_name, datapackage_dir)

    _create_elements(builder, blueprint_data)
    builder.infer_sequences_from_resources()
    _create_sequences(builder, blueprint_data)
    builder.infer_busses_from_resources()
    builder.save_package()
    settings.logger.info(f"Successfully created datapackage '{blueprint_name}'.")


def _create_elements(builder: PackageBuilder, blueprint_data: dict) -> None:
    """Add elements from scenario data to package builder."""
    regions = blueprint_data.get("regions")
    elements = blueprint_data.get("elements", {})
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
    """Add all instances from blueprint data to element resource."""

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


def _create_sequences(builder: PackageBuilder, blueprint_data: dict) -> None:
    """Add sequences from blueprint data to package builder."""
    timeindex_info = blueprint_data.get("timeindex", {})
    timeindex = list(
        (
            hourly_range(timeindex_info["start"], timeindex_info["periods"])
            if timeindex_info
            else None
        ),
    )

    # Add sequences explicitly set in blueprint file
    sequences = blueprint_data.get("sequences", {})
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

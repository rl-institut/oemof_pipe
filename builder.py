"""Module to build empty datapackage from predefined components."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
import yaml
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator, Sized
from frictionless import Package, Resource, Schema
import csv
import settings


FRICTIONLESS_MAPPING = {
    "str": "string",
    "float": "number",
    "int": "integer",
    "dict": "object",
    "bool": "boolean",
}

REQUIRED_FIELDS = ("type", "name")

DEFAULT_BUS_INSTANCE = {"balanced": True}


def hourly_range(start: dt.datetime, periods: int) -> Iterator[dt.datetime]:
    """Create hourly range."""
    current = start
    for _ in range(periods):
        yield current
        current += dt.timedelta(hours=1)


@dataclass
class Component:
    """Dataclass to represent components."""

    name: str
    attributes: dict[str, dict[str, str]]
    busses: list[str] = field(default_factory=list)
    sequences: list[str] = field(default_factory=list)

    @classmethod
    def from_name(
        cls,
        component_name: str,
        component_dir: Path = settings.COMPONENTS_DIR,
    ) -> Component:
        """Create component looking up name in components directory."""
        with (component_dir / f"{component_name}.yaml").open("r") as f:
            data = yaml.safe_load(f)
        return cls(
            name=component_name,
            attributes=data.get("attributes", {}),
            busses=data.get("busses", []) or [],
            sequences=data.get("sequences", []) or [],
        )


def get_available_components(
    component_dir: Path = settings.COMPONENTS_DIR,
) -> list[str]:
    """Read components defined in components directory."""
    return [f.stem for f in component_dir.glob("*.yaml")]


def map_to_frictionless_resource(
    resource: ElementResourceBuilder | SequenceResourceBuilder,
) -> Resource:
    """Map resource to frictionless resource."""
    fields = []
    for field_data in resource.fields.values():
        field_data["type"] = FRICTIONLESS_MAPPING.get(
            field_data["type"],
            field_data["type"],
        )
        fields.append(field_data)
    schema = Schema.from_descriptor(
        {
            "fields": fields,
            "primaryKey": (
                "timeindex" if isinstance(resource, SequenceResourceBuilder) else "name"
            ),
            "foreignKeys": (
                resource.foreign_keys
                if isinstance(resource, ElementResourceBuilder)
                else []
            ),
        },
        allow_invalid=True,
    )
    description = (
        f"Derived from component: {resource.component.name}"
        if isinstance(resource, ElementResourceBuilder)
        else "Profiles"
    )
    return Resource(
        path=str(resource.path),
        name=resource.name,
        schema=schema,
        description=description,
    )


class ElementResourceBuilder:
    """Class to create empty element resource from predefined component."""

    def __init__(
        self,
        component_name: str,
        resource_name: str,
        selected_attributes: list[str] | None = None,
        sequences: list[str] | None = None,
    ) -> None:
        """Init element resource builder."""
        self.component: Component = Component.from_name(component_name)
        self.name: str = resource_name
        self.sequences: set[str] = (
            set(self.component.sequences + sequences)
            if sequences
            else set(self.component.sequences)
        )
        self.fields: dict[str, dict[str, Any]] = {}
        self.instances: list[dict] = []

        if selected_attributes is None:
            selected_attributes = self.component.attributes

        # Always add column "type"
        for attr in REQUIRED_FIELDS:
            if attr not in selected_attributes:
                selected_attributes.append(attr)
        for field_name in selected_attributes:
            self.add_field(field_name)

    def add_field(self, field_name: str) -> None:
        """Add field to resource."""
        if field_name not in self.component.attributes:
            raise KeyError(
                f"Attribute {field_name} not found in component {self.component.name}.",
            )

        attr_info = self.component.attributes[field_name]
        field_type = (
            "string"
            if field_name in self.sequences
            else attr_info.get("type", "string")
        )

        self.fields[field_name] = {
            "name": field_name,
            "type": field_type,
            "description": attr_info.get("description", ""),
            "custom": {"unit": attr_info.get("unit", "")},
        }

    def add_instance(self, data: dict) -> None:
        """Add instance (data row) to resource."""
        if "name" not in data:
            error_msg = "Missing 'name' in data."
            raise ValueError(error_msg)
        if "type" not in data:
            # Set data type automatically if not set
            data["type"] = self.component.name

        # Check type column consistency
        if data["type"] != self.component.name:
            raise ValueError(
                f"Type ('{data['type']}') cannot be different from component type ('{self.component.name}').",
            )

        # Check if all data fields match
        for key in data:
            if key not in self.fields:
                raise KeyError(
                    f"Attribute {key} not found in resource. Possible attributes are: {list(self.fields)}.",
                )
        self.instances.append(data)

    @property
    def foreign_keys(self) -> list[dict]:
        """Return foreign keys for busses and profiles in resource."""
        fks = []
        for bus in self.component.busses:
            fks.append(  # noqa: PERF401
                {
                    "fields": bus,
                    "reference": {"resource": "bus", "fields": "name"},
                },
            )
        for sequence in self.sequences:
            fks.append(  # noqa: PERF401
                {
                    "fields": sequence,
                    "reference": {"resource": f"{self.name}_profile"},
                },
            )
        return fks

    @property
    def path(self) -> Path:
        """Return relative path of resource."""
        return Path(
            f"data/elements/{self.name}.csv",
        )

    def save(self, package_path: Path) -> None:
        """Save element resource as CSV."""
        full_path = package_path / self.path
        headers = list(self.fields)
        with full_path.open("w", newline="") as f:
            writer = csv.writer(f, delimiter=";")
            writer.writerow(headers)
            for instance in self.instances:
                row = [instance.get(attr, "") for attr in self.fields]
                writer.writerow(row)


class SequenceResourceBuilder:
    """Class to build sequence resources."""

    def __init__(self, resource_name: str, timeindex: Sized | None = None) -> None:
        """Init."""
        self.name: str = resource_name
        self.fields: dict[str, dict[str, Any]] = {
            "timeindex": {
                "name": "timeindex",
                "type": "datetime",
                "unit": "n/a",
                "description": "Current timestep",
            },
        }
        self.timeindex = timeindex or list(
            hourly_range(start=dt.datetime(2026, 1, 1), periods=8760),
        )
        self.instances: dict[str, Sized] = {}

    def add_instance(self, name: str, timeseries: Sized) -> None:
        """Add instance (data column) to resource."""
        if len(timeseries) != len(self.timeindex):
            error_msg = (
                f"Timeseries length for column '{name}' does not match "
                f"({len(timeseries)} != {len(self.timeindex)})."
            )
            raise IndexError(error_msg)
        self.fields[name] = {
            "name": name,
            "type": "float",
            "unit": "n/a",
            "description": "Value for current timestep",
        }
        self.instances[name] = timeseries

    @property
    def path(self) -> Path:
        """Return relative path of resource."""
        return Path(
            f"data/sequences/{self.name}.csv",
        )

    def save(self, package_path: Path) -> None:
        """Save sequence resource as CSV."""
        full_path = package_path / self.path
        headers = list(self.fields)
        with full_path.open("w", newline="") as f:
            writer = csv.writer(f, delimiter=";")
            writer.writerow(headers)

            # Do not iterate timeindex if no other columns are present
            if len(self.instances) > 0:
                for row in zip(self.timeindex, *self.instances.values()):
                    writer.writerow(row)


class PackageBuilder:
    """Class to create empty datapackage from predefined components."""

    def __init__(
        self,
        package_name: str,
        base_dir: str | Path = settings.DATAPACKAGE_DIR,
    ) -> None:
        """Initialize the package builder."""
        self.package_name: str = package_name
        self.base_dir: Path = Path(base_dir) / package_name
        self.resources: dict[str, ElementResourceBuilder | SequenceResourceBuilder] = {}

    def add_resource(
        self,
        resource: ElementResourceBuilder | SequenceResourceBuilder,
    ) -> None:
        """Add resource to package from component."""
        self.resources[resource.name] = resource

    def infer_sequences_from_resources(self) -> None:
        """Add sequences based on attached resources to package."""
        for resource in list(self.resources.values()):
            if isinstance(resource, SequenceResourceBuilder):
                continue
            # Add resource for timeseries if at least one sequence is present
            if resource.sequences and any(
                sequence in resource.fields for sequence in resource.sequences
            ):
                if f"{resource.name}_profile" in self.resources:
                    # Do not add sequence automatically if sequence with same name exists
                    continue
                self.add_resource(
                    SequenceResourceBuilder(
                        f"{resource.name}_profile",
                    ),
                )

    def infer_busses_from_resources(self) -> None:
        """Add buses based on attached resources to package."""
        if "bus" not in self.resources:
            # Add default bus resource
            bus = ElementResourceBuilder(component_name="bus", resource_name="bus")
            self.add_resource(bus)
        else:
            bus = self.resources["bus"]

        # Scan all ElementResourceBuilder instances
        existing_bus_names = {inst["name"] for inst in bus.instances}
        for resource in self.resources.values():
            if not isinstance(resource, ElementResourceBuilder):
                continue

            for bus_fk in resource.component.busses:
                for instance in resource.instances:
                    bus_name = instance.get(bus_fk)
                    if bus_name and bus_name not in existing_bus_names:
                        bus_instance = DEFAULT_BUS_INSTANCE.copy()
                        bus_instance["name"] = bus_name
                        bus.add_instance(bus_instance)
                        existing_bus_names.add(bus_name)

    def save_package(self) -> None:
        """Save datapackage to datapackage directory."""
        self.base_dir.mkdir(parents=True, exist_ok=True)
        elements_dir = self.base_dir / "data" / "elements"
        elements_dir.mkdir(parents=True, exist_ok=True)
        sequences_dir = self.base_dir / "data" / "sequences"
        sequences_dir.mkdir(parents=True, exist_ok=True)

        package = Package()
        package.name = self.package_name

        for resource in self.resources.values():
            resource.save(self.base_dir)
            package.add_resource(map_to_frictionless_resource(resource))

        package.to_json(str(self.base_dir / "datapackage.json"))

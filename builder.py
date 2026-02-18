"""Module to build empty datapackage from predefined components."""

from __future__ import annotations

from dataclasses import dataclass, field
import yaml
from pathlib import Path
from typing import Any
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


@dataclass
class Component:
    """Dataclass to represent components."""

    name: str
    attributes: dict[str, dict[str, str]]
    busses: list[str] = field(default_factory=list)
    sequences: list[str] = field(default_factory=list)

    @classmethod
    def from_name(cls, component_name: str) -> Component:
        """Create component looking up name in components directory."""
        with (settings.COMPONENTS_DIR / f"{component_name}.yaml").open("r") as f:
            data = yaml.safe_load(f)
        return cls(
            name=component_name,
            attributes=data.get("attributes", {}),
            busses=data.get("busses", []),
            sequences=data.get("sequences", []),
        )


def get_available_components() -> list[str]:
    """Read components defined in components directory."""
    return [f.stem for f in settings.COMPONENTS_DIR.glob("*.yaml")]


class ResourceBuilder:
    """Class to create empty resource from predefined component."""

    def __init__(
        self,
        package: PackageBuilder,
        component_name: str,
        resource_name: str,
        selected_attributes: list[str],
        sequences: list[str] | None = None,
        *,
        is_sequence: bool = False,
    ) -> None:
        """Init resource builder."""
        self.package = package
        self.component: Component = Component.from_name(component_name)
        self.resource_name: str = resource_name
        self.is_sequence: bool = is_sequence
        self.sequences: set[str] = (
            set(self.component.sequences + sequences)
            if sequences
            else set(self.component.sequences)
        )
        self.fields: dict[str, dict[str, Any]] = {}
        self.add_fields(selected_attributes)

    def add_fields(self, selected_attributes: list[str]) -> None:
        """Add fields to resource."""
        for attr_name in selected_attributes:
            if attr_name not in self.component.attributes:
                raise KeyError(
                    f"Attribute {attr_name} not found in component {self.component.name}.",
                )

            attr_info = self.component.attributes[attr_name]
            field_type = (
                "string"
                if attr_name in self.sequences
                else attr_info.get("type", "string")
            )

            self.fields[attr_name] = {
                "name": attr_name,
                "type": FRICTIONLESS_MAPPING.get(field_type, field_type),
                "description": attr_info.get("description", ""),
                "custom": {"unit": attr_info.get("unit", "")},
            }

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
                    "reference": {"resource": f"{self.component.name}_profile"},
                },
            )
        return fks

    @property
    def path(self) -> Path:
        """Return relative path of resource."""
        return Path(
            f"data/{'sequences' if self.is_sequence else 'elements'}/{self.resource_name}.csv",
        )

    @property
    def full_path(self) -> Path:
        """Return full path of resource."""
        return self.package.base_dir / self.path

    def save(self) -> None:
        """Save empty resource as CSV."""
        headers = list(self.fields)
        with self.full_path.open("w", newline="") as f:
            writer = csv.writer(f, delimiter=";")
            writer.writerow(headers)

    @property
    def resource(self) -> Resource:
        """Return frictionless resource."""
        schema = Schema.from_descriptor(
            {
                "fields": list(self.fields.values()),
                "primaryKey": "timeindex" if self.is_sequence else "name",
                "foreignKeys": self.foreign_keys,
            },
            allow_invalid=True,
        )
        return Resource(
            path=str(self.path),
            name=self.resource_name,
            schema=schema,
            description=f"Derived from component: {self.component.name}",
        )


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
        self.resources: list[ResourceBuilder] = []

        # Add default bus resource
        self.add_resource("bus", "bus", ["name", "region", "type", "balanced"])

    def add_resource(
        self,
        component_name: str,
        resource_name: str,
        selected_attributes: list[str],
        sequences: list[str] | None = None,
    ) -> None:
        """Add resource to package from component."""
        self.resources.append(
            ResourceBuilder(
                self,
                component_name,
                resource_name,
                selected_attributes,
                sequences,
            ),
        )

    def save_package(self) -> None:
        """Save datapackage to datapackage directory."""
        self.base_dir.mkdir(parents=True, exist_ok=True)
        elements_dir = self.base_dir / "data" / "elements"
        elements_dir.mkdir(parents=True, exist_ok=True)
        sequences_dir = self.base_dir / "data" / "sequences"
        sequences_dir.mkdir(parents=True, exist_ok=True)

        package = Package()
        package.name = self.package_name

        for resource in self.resources:
            resource.save()
            package.add_resource(resource.resource)

            # Add resource for timeseries if at least one sequence is present
            if resource.sequences:
                profile = ResourceBuilder(
                    self,
                    "profile",
                    f"{resource.resource_name}_profile",
                    ["timeindex"],
                    is_sequence=True,
                )
                profile.save()
                package.add_resource(profile.resource)

        package.to_json(str(self.base_dir / "datapackage.json"))

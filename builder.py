"""Module to build empty datapackage from predefined components."""

from __future__ import annotations

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
}


def get_available_components() -> list[str]:
    """Read components defined in components directory."""
    return [f.stem for f in settings.COMPONENTS_DIR.glob("*.yaml")]


def load_component(component_name: str) -> dict[str, Any]:
    """Read component yaml file."""
    with (settings.COMPONENTS_DIR / f"{component_name}.yaml").open("r") as f:
        return yaml.safe_load(f)


class PackageBuilder:
    """Class to create empty datapackage from predefined components."""

    def __init__(
        self,
        package_name: str,
        base_dir: str | Path = settings.DATAPACKAGE_DIR,
    ) -> None:
        """Initialize the package builder."""
        self.package_name = package_name
        self.base_dir = Path(base_dir) / package_name
        self.resources = []

    def add_resource(
        self,
        component_name: str,
        resource_name: str,
        selected_attributes: list[str],
    ) -> None:
        """Add resource to package from component."""
        component_data = load_component(component_name)
        attributes = component_data.get("attributes", {})

        fields: list[dict[str, Any]] = []
        for attr_name in selected_attributes:
            if attr_name not in attributes:
                raise KeyError(
                    f"Attribute {attr_name} not found in component {component_name}.",
                )

            attr_info = attributes[attr_name]
            field_type = attr_info.get("type", "string")

            fields.append(
                {
                    "name": attr_name,
                    "type": FRICTIONLESS_MAPPING.get(field_type, field_type),
                    "description": attr_info.get("description", ""),
                    "custom": {"unit": attr_info.get("unit", "")},
                },
            )

        self.resources.append(
            {"name": resource_name, "component": component_name, "fields": fields},
        )

    def save_package(self) -> None:
        """Save datapackage to datapackage directory."""
        self.base_dir.mkdir(parents=True, exist_ok=True)
        data_dir = self.base_dir / "data" / "elements"
        data_dir.mkdir(parents=True, exist_ok=True)

        package = Package()
        package.name = self.package_name

        for res_info in self.resources:
            res_name = res_info["name"]
            csv_path = f"data/elements/{res_name}.csv"
            full_csv_path = self.base_dir / csv_path

            # Create empty CSV with headers
            headers = [field["name"] for field in res_info["fields"]]
            with full_csv_path.open("w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(headers)

            schema = Schema.from_descriptor({"fields": res_info["fields"]})
            resource = Resource(
                path=csv_path,
                name=res_name,
                schema=schema,
                description=f"Derived from component: {res_info['component']}",
            )
            package.add_resource(resource)

        package.to_json(str(self.base_dir / "datapackage.json"))

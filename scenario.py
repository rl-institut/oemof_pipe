"""Module to generate data packages from scenario files."""

import yaml
from pathlib import Path
import settings
from builder import PackageBuilder, ResourceBuilder


def load_scenario(
    scenario_name: str,
    scenario_dir: Path = settings.SCENARIO_DIR,
    datapackage_dir: Path = settings.DATAPACKAGE_DIR,
) -> None:
    """Read scenario from scenario folder and create a datapackage from it."""
    scenario_path = scenario_dir / f"{scenario_name}.yaml"
    with scenario_path.open("r") as f:
        scenario_data = yaml.safe_load(f)

    builder = PackageBuilder(scenario_name, datapackage_dir)

    components = scenario_data.get("components", {})
    for res_name, config in components.items():
        component_type = config.get("component")
        attributes = config.get("attributes", [])
        instances = config.get("instances", [])

        # Create resource builder
        resource = ResourceBuilder(
            component_name=component_type,
            resource_name=res_name,
            selected_attributes=attributes,
        )

        # Add all instances to resource
        for instance in instances:
            resource.add_instance(instance)

        builder.add_resource(resource)

    builder.save_package()

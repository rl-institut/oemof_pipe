"""Module to generate data packages from scenario files."""

import yaml
from pathlib import Path
import settings
from builder import PackageBuilder, ResourceBuilder, Component


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
        instances = config.get("instances", [])
        attributes = config.get("attributes", [])
        if len(attributes) == 0:
            attributes = Component.from_name(component_type).attributes

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

    builder.infer_sequences_from_resources()
    builder.save_package()


if __name__ == "__main__":
    load_scenario("bbb")

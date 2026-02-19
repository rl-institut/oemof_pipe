"""Module to test datapackage builder."""

import json
import pathlib

from builder import PackageBuilder, ResourceBuilder


def test_add_resource_and_save(tmp_path: pathlib.Path) -> None:
    """Add two resources and save the package; verify files and schemas are created correctly."""
    test_dir = tmp_path / "test_datapackages"
    builder = PackageBuilder("test-pkg", base_dir=str(test_dir))

    # Using an existing component for the test ('asymmetric_storage')
    resource = ResourceBuilder(
        "storage",
        "liion_storage",
        ["region", "capacity"],
    )
    builder.add_resource(resource)
    resource = ResourceBuilder(
        "load",
        "electricity_demand",
        ["region", "amount"],
    )
    resource.add_instance({"region": "BB", "amount": 100})
    builder.add_resource(resource)

    builder.save_package()

    pkg_dir = test_dir / "test-pkg"
    assert (pkg_dir / "datapackage.json").exists()
    assert (pkg_dir / "data/elements/liion_storage.csv").exists()
    assert (pkg_dir / "data/elements/electricity_demand.csv").exists()

    with (pkg_dir / "datapackage.json").open("r") as f:
        data = json.load(f)
        assert data["name"] == "test-pkg"
        assert len(data["resources"]) == 3  # noqa: PLR2004

        res1 = next(r for r in data["resources"] if r["name"] == "electricity_demand")
        assert len(res1["schema"]["fields"]) == 3  # noqa: PLR2004
        assert res1["schema"]["fields"][0]["name"] == "region"
        assert res1["schema"]["fields"][1]["name"] == "amount"
        assert res1["schema"]["fields"][2]["name"] == "type"

    with (pkg_dir / "data/elements/electricity_demand.csv").open("r") as f:
        lines = f.readlines()
        assert len(lines) == 2  # noqa: PLR2004
        assert lines[0].strip() == "region;amount;type"
        assert lines[1].strip() == "BB;100;load"

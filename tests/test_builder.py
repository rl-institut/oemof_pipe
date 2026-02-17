"""Module to test datapackage builder."""

import json
import pathlib

from builder import PackageBuilder


def test_add_resource_and_save(tmp_path: pathlib.Path) -> None:
    """Add two resources and save the package; verify files and schemas are created correctly."""
    test_dir = tmp_path / "test_datapackages"
    builder = PackageBuilder("test-pkg", base_dir=str(test_dir))

    # Using an existing component for the test ('asymmetric_storage')
    builder.add_resource(
        "asymmetric_storage",
        "res1",
        ["region", "capacity_charge"],
    )
    builder.add_resource("asymmetric_storage", "res2", ["name", "tech"])

    builder.save_package()

    pkg_dir = test_dir / "test-pkg"
    assert (pkg_dir / "datapackage.json").exists()
    assert (pkg_dir / "data/elements/res1.csv").exists()
    assert (pkg_dir / "data/elements/res2.csv").exists()

    with (pkg_dir / "datapackage.json").open("r") as f:
        data = json.load(f)
        assert data["name"] == "test-pkg"
        assert len(data["resources"]) == 2  # noqa: PLR2004

        res1 = next(r for r in data["resources"] if r["name"] == "res1")
        assert len(res1["schema"]["fields"]) == 2  # noqa: PLR2004
        assert res1["schema"]["fields"][0]["name"] == "region"
        assert res1["schema"]["fields"][1]["name"] == "capacity_charge"

    with (pkg_dir / "data/elements/res1.csv").open("r") as f:
        lines = f.readlines()
        assert len(lines) == 1
        assert lines[0].strip() == "region,capacity_charge"

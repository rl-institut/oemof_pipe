"""Module to test datapackage builder."""

import json
import pathlib
import datetime as dt

import builder
from builder import PackageBuilder, ElementResourceBuilder, SequenceResourceBuilder


def test_add_element_resource_and_save(tmp_path: pathlib.Path) -> None:
    """Add two resources and save the package; verify files and schemas are created correctly."""
    test_dir = tmp_path / "test_datapackages"
    builder = PackageBuilder("test-pkg", base_dir=str(test_dir))

    # Using an existing component for the test ('asymmetric_storage')
    resource = ElementResourceBuilder(
        "storage",
        "liion_storage",
        ["region", "capacity"],
    )
    builder.add_resource(resource)
    resource = ElementResourceBuilder(
        "load",
        "electricity_demand",
        ["region", "amount"],
    )
    resource.add_instance({"name": "d1", "region": "BB", "amount": 100})
    builder.add_resource(resource)

    builder.infer_sequences_from_resources()
    builder.save_package()

    pkg_dir = test_dir / "test-pkg"
    assert (pkg_dir / "datapackage.json").exists()
    assert (pkg_dir / "data/elements/liion_storage.csv").exists()
    assert (pkg_dir / "data/elements/electricity_demand.csv").exists()
    assert (pkg_dir / "data/sequences/electricity_demand_profile.csv").exists()

    with (pkg_dir / "datapackage.json").open("r") as f:
        data = json.load(f)
        assert data["name"] == "test-pkg"
        assert len(data["resources"]) == 4  # noqa: PLR2004

        res1 = next(r for r in data["resources"] if r["name"] == "electricity_demand")
        assert len(res1["schema"]["fields"]) == 4  # noqa: PLR2004
        assert res1["schema"]["fields"][0]["name"] == "region"
        assert res1["schema"]["fields"][1]["name"] == "amount"
        assert res1["schema"]["fields"][2]["name"] == "type"
        assert res1["schema"]["fields"][3]["name"] == "name"

    with (pkg_dir / "data/elements/electricity_demand.csv").open("r") as f:
        lines = f.readlines()
        assert len(lines) == 2  # noqa: PLR2004
        assert lines[0].strip() == "region;amount;type;name"
        assert lines[1].strip() == "BB;100;load;d1"

    with (pkg_dir / "data/sequences/electricity_demand_profile.csv").open("r") as f:
        lines = f.readlines()
        assert len(lines) == 1
        assert lines[0].strip() == "timeindex"


def test_add_sequence_resource_and_save(tmp_path: pathlib.Path) -> None:
    """Add two resources and save the package; verify files and schemas are created correctly."""
    test_dir = tmp_path / "test_datapackages"
    builder = PackageBuilder("test-pkg", base_dir=str(test_dir))

    # Using an existing component for the test ('asymmetric_storage')
    resource = SequenceResourceBuilder("electricity_demand_profile")
    resource.add_instance("demand", range(8760))
    builder.add_resource(resource)

    resource = SequenceResourceBuilder(
        "electricity_demand_profile_reduced",
        timeindex=[1, 2, 3],
    )
    resource.add_instance("demand", [4, 5, 6])
    builder.add_resource(resource)

    builder.save_package()

    pkg_dir = test_dir / "test-pkg"
    assert (pkg_dir / "datapackage.json").exists()
    assert (pkg_dir / "data/sequences/electricity_demand_profile.csv").exists()
    assert (pkg_dir / "data/sequences/electricity_demand_profile_reduced.csv").exists()

    with (pkg_dir / "datapackage.json").open("r") as f:
        data = json.load(f)
        assert data["name"] == "test-pkg"
        assert len(data["resources"]) == 3  # noqa: PLR2004

        res1 = next(
            r for r in data["resources"] if r["name"] == "electricity_demand_profile"
        )
        assert len(res1["schema"]["fields"]) == 2  # noqa: PLR2004
        assert res1["schema"]["fields"][0]["name"] == "timeindex"
        assert res1["schema"]["fields"][1]["name"] == "demand"

    with (pkg_dir / "data/sequences/electricity_demand_profile.csv").open("r") as f:
        lines = f.readlines()
        assert len(lines) == 8761  # noqa: PLR2004
        assert lines[0].strip() == "timeindex;demand"
        assert lines[4].strip() == "2026-01-01 03:00:00;3"

    with (pkg_dir / "data/sequences/electricity_demand_profile_reduced.csv").open(
        "r",
    ) as f:
        lines = f.readlines()
        assert len(lines) == 4  # noqa: PLR2004
        assert lines[0].strip() == "timeindex;demand"
        assert lines[3].strip() == "3;6"


def test_hourly_range() -> None:
    """Test creation of hourly range."""
    timesteps = list(builder.hourly_range(dt.datetime(2026, 1, 1), periods=8760))
    assert len(timesteps) == 8760  # noqa: PLR2004
    assert timesteps[0] == dt.datetime(2026, 1, 1)
    assert timesteps[1] == dt.datetime(2026, 1, 1, 1)
    assert timesteps[2] == dt.datetime(2026, 1, 1, 2)
    assert timesteps[-1] == dt.datetime(2026, 12, 31, 23)

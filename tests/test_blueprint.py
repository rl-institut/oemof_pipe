"""Module to test scenario generation."""

import json
import pathlib
from pathlib import Path

from blueprint import create_blueprint


def test_blueprint_creation(tmp_path: Path) -> None:
    """Load the blueprint from test blueprints and create datapackage."""
    # Setup temporary directories
    pkg_dir = tmp_path / "datapackages"
    blueprint_dir = pathlib.Path(__file__).parent / "test_data" / "blueprints"

    create_blueprint("test", blueprint_dir=blueprint_dir, datapackage_dir=pkg_dir)

    # Verify output
    expected_pkg_path = pkg_dir / "test"
    assert (expected_pkg_path / "datapackage.json").exists()
    assert (expected_pkg_path / "data/elements/electricity_demand.csv").exists()
    assert (expected_pkg_path / "data/elements/liion_storage.csv").exists()
    assert (expected_pkg_path / "data/elements/chp.csv").exists()
    assert (expected_pkg_path / "data/sequences/liion_storage_profile.csv").exists()

    with (expected_pkg_path / "datapackage.json").open("r") as f:
        data = json.load(f)
        assert data["name"] == "test"
        assert len(data["resources"]) == 5  # noqa: PLR2004

    with (expected_pkg_path / "data/elements/bus.csv").open("r") as f:
        lines = f.readlines()
        assert len(lines) == 4  # noqa: PLR2004
        assert lines[0].strip() == "region;name;type;balanced"
        assert lines[1].strip() == ";electricity;bus;True"
        assert lines[2].strip() == ";oil;bus;True"
        assert lines[3].strip() == ";heat;bus;True"

    with (expected_pkg_path / "data/elements/electricity_demand.csv").open("r") as f:
        lines = f.readlines()
        assert len(lines) == 3  # noqa: PLR2004
        assert lines[0].strip() == "region;amount;bus;type;name"
        assert lines[1].strip() == "BB;;electricity;load;d1"
        assert lines[2].strip() == "B;50;;load;d2"

    with (expected_pkg_path / "data/sequences/liion_storage_profile.csv").open(
        "r",
    ) as f:
        lines = f.readlines()
        assert len(lines) == 8761  # noqa: PLR2004
        assert lines[0].strip() == "timeindex;efficiency;loss_rate;liion-profile"
        assert lines[3].strip() == "2016-01-01 02:00:00;0;0;0"


def test_regions_blueprint(tmp_path: Path) -> None:
    """Load the region blueprint from test blueprints folder and create datapackage."""
    # Setup temporary directories
    pkg_dir = tmp_path / "datapackages"
    blueprint_dir = pathlib.Path(__file__).parent / "test_data" / "blueprints"

    create_blueprint("regions", blueprint_dir=blueprint_dir, datapackage_dir=pkg_dir)

    # Verify output
    expected_pkg_path = pkg_dir / "regions"
    assert (expected_pkg_path / "datapackage.json").exists()
    assert (expected_pkg_path / "data/elements/bus.csv").exists()
    assert (expected_pkg_path / "data/elements/electricity_demand.csv").exists()
    assert (expected_pkg_path / "data/elements/heat_demand.csv").exists()
    assert (expected_pkg_path / "data/elements/liion_storage.csv").exists()
    assert (
        expected_pkg_path / "data/sequences/electricity_demand_profile.csv"
    ).exists()
    assert (expected_pkg_path / "data/sequences/liion_storage_profile.csv").exists()

    with (expected_pkg_path / "datapackage.json").open("r") as f:
        data = json.load(f)
        assert data["name"] == "regions"
        assert len(data["resources"]) == 6  # noqa: PLR2004

    with (expected_pkg_path / "data/elements/bus.csv").open("r") as f:
        lines = f.readlines()
        assert len(lines) == 4  # noqa: PLR2004
        assert lines[0].strip() == "region;name;type;balanced"
        assert lines[1].strip() == ";BB-electricity;bus;True"
        assert lines[2].strip() == ";B-electricity;bus;True"
        assert lines[3].strip() == ";heat;bus;True"

    with (expected_pkg_path / "data/elements/electricity_demand.csv").open("r") as f:
        lines = f.readlines()
        assert len(lines) == 5  # noqa: PLR2004
        assert lines[0].strip() == "amount;bus;profile;region;type;name"
        assert lines[1].strip() == ";BB-electricity;BB-d1-profile;BB;load;BB-d1"
        assert lines[2].strip() == ";B-electricity;B-d1-profile;B;load;B-d1"
        assert lines[3].strip() == "50;;BB-d2-profile;BB;load;BB-d2"
        assert lines[4].strip() == "50;;B-d2-profile;B;load;B-d2"

    with (expected_pkg_path / "data/elements/heat_demand.csv").open("r") as f:
        lines = f.readlines()
        assert len(lines) == 3  # noqa: PLR2004
        assert lines[0].strip() == "amount;bus;region;type;name"
        assert lines[1].strip() == ";heat;;load;h1"
        assert lines[2].strip() == "60;;;load;h2"

    with (expected_pkg_path / "data/elements/liion_storage.csv").open("r") as f:
        lines = f.readlines()
        assert len(lines) == 2  # noqa: PLR2004
        assert "B-liion" in lines[1].strip()

    with (expected_pkg_path / "data/sequences/electricity_demand_profile.csv").open(
        "r",
    ) as f:
        lines = f.readlines()
        assert len(lines) == 8761  # noqa: PLR2004
        assert (
            lines[0].strip()
            == "timeindex;B-d1-profile;B-d2-profile;BB-d1-profile;BB-d2-profile"
        )

    with (expected_pkg_path / "data/sequences/liion_storage_profile.csv").open(
        "r",
    ) as f:
        lines = f.readlines()
        assert len(lines) == 8761  # noqa: PLR2004
        assert lines[0].strip() == "timeindex;efficiency;loss_rate;B-liion-profile"
        assert lines[3].strip() == "2016-01-01 02:00:00;0;0;0"

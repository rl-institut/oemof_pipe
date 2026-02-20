"""Module to test scenario generation."""

import json
import pathlib
from pathlib import Path

from scenario import create_scenario, apply_element_data, apply_sequence_data
import duckdb


def test_load_scenario(tmp_path: Path) -> None:
    """Load the scenario from test scenarios folder and create datapackage."""
    # Setup temporary directories
    pkg_dir = tmp_path / "datapackages"
    scenario_dir = pathlib.Path(__file__).parent / "test_data" / "scenarios"

    create_scenario("test", scenario_dir=scenario_dir, datapackage_dir=pkg_dir)

    # Verify output
    expected_pkg_path = pkg_dir / "test"
    assert (expected_pkg_path / "datapackage.json").exists()
    assert (expected_pkg_path / "data/elements/electricity_demand.csv").exists()
    assert (expected_pkg_path / "data/elements/liion_storage.csv").exists()
    assert (expected_pkg_path / "data/elements/chp.csv").exists()
    assert (
        expected_pkg_path / "data/sequences/electricity_demand_profile.csv"
    ).exists()
    assert (expected_pkg_path / "data/sequences/liion_storage_profile.csv").exists()

    with (expected_pkg_path / "datapackage.json").open("r") as f:
        data = json.load(f)
        assert data["name"] == "test"
        assert len(data["resources"]) == 6  # noqa: PLR2004

    with (expected_pkg_path / "data/elements/bus.csv").open("r") as f:
        lines = f.readlines()
        assert len(lines) == 4  # noqa: PLR2004
        assert lines[0].strip() == "region;name;type;balanced"
        assert lines[1].strip() == ";electricity;bus;True"
        assert lines[2].strip() == ";oil;bus;"
        assert lines[3].strip() == ";heat;bus;"

    with (expected_pkg_path / "data/sequences/electricity_demand_profile.csv").open(
        "r",
    ) as f:
        lines = f.readlines()
        assert len(lines) == 1
        assert lines[0].strip() == "timeindex"

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
        assert lines[0].strip() == "timeindex;efficiency;loss_rate"
        assert lines[3].strip() == "2016-01-01 02:00:00;0;0"


def test_regions_scenario(tmp_path: Path) -> None:
    """Load the region scenario from test scenarios folder and create datapackage."""
    # Setup temporary directories
    pkg_dir = tmp_path / "datapackages"
    scenario_dir = pathlib.Path(__file__).parent / "test_data" / "scenarios"

    create_scenario("regions", scenario_dir=scenario_dir, datapackage_dir=pkg_dir)

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
    assert (expected_pkg_path / "data/sequences/heat_demand_profile.csv").exists()
    assert (expected_pkg_path / "data/sequences/liion_storage_profile.csv").exists()

    with (expected_pkg_path / "datapackage.json").open("r") as f:
        data = json.load(f)
        assert data["name"] == "regions"
        assert len(data["resources"]) == 7  # noqa: PLR2004

    with (expected_pkg_path / "data/elements/electricity_demand.csv").open("r") as f:
        lines = f.readlines()
        assert len(lines) == 5  # noqa: PLR2004
        assert lines[0].strip() == "amount;region;type;name"
        assert lines[1].strip() == ";BB;load;BB-d1"
        assert lines[2].strip() == ";B;load;B-d1"
        assert lines[3].strip() == "50;BB;load;BB-d2"
        assert lines[4].strip() == "50;B;load;B-d2"

    with (expected_pkg_path / "data/elements/heat_demand.csv").open("r") as f:
        lines = f.readlines()
        assert len(lines) == 3  # noqa: PLR2004
        assert lines[0].strip() == "amount;region;type;name"
        assert lines[1].strip() == ";;load;h1"
        assert lines[2].strip() == "60;;load;h2"

    with (expected_pkg_path / "data/elements/liion_storage.csv").open("r") as f:
        lines = f.readlines()
        assert len(lines) == 2  # noqa: PLR2004
        assert "B-liion" in lines[1].strip()

    with (expected_pkg_path / "data/sequences/electricity_demand_profile.csv").open(
        "r",
    ) as f:
        lines = f.readlines()
        assert len(lines) == 1
        assert lines[0].strip() == "timeindex"

    with (expected_pkg_path / "data/sequences/liion_storage_profile.csv").open(
        "r",
    ) as f:
        lines = f.readlines()
        assert len(lines) == 8761  # noqa: PLR2004
        assert lines[0].strip() == "timeindex;efficiency;loss_rate"
        assert lines[3].strip() == "2016-01-01 02:00:00;0;0"


def test_apply_scenario_data_single(tmp_path: Path) -> None:
    """Test applying scenario data in single format."""
    pkg_dir = tmp_path / "datapackages"
    scenario_dir = pathlib.Path(__file__).parent / "test_data" / "scenarios"
    create_scenario("test", scenario_dir=scenario_dir, datapackage_dir=pkg_dir)

    data_path = pathlib.Path(__file__).parent / "test_data" / "raw" / "single.csv"

    # Apply data
    apply_element_data(data_path, "test", "ALL", datapackage_dir=pkg_dir)

    # Verify electricity_demand (l1) amount changed from default (none) to 10
    con = duckdb.connect(database=":memory:")
    csv_path = pkg_dir / "test" / "data/elements/electricity_demand.csv"
    res = con.execute(
        f"SELECT amount FROM read_csv_auto('{csv_path}', sep=';') WHERE name = 'd1'",  # noqa: S608
    ).fetchone()
    assert res[0] == 10  # noqa: PLR2004

    # Verify liion_storage (liion) capacity changed from 100 to 99
    csv_path = pkg_dir / "test" / "data/elements/liion_storage.csv"
    res = con.execute(
        f"SELECT capacity FROM read_csv_auto('{csv_path}', sep=';') WHERE name = 'liion'",  # noqa: S608
    ).fetchone()
    assert res[0] == 99  # noqa: PLR2004


def test_apply_scenario_data_multiple(tmp_path: Path) -> None:
    """Test applying scenario data in multiple format."""
    pkg_dir = tmp_path / "datapackages"
    scenario_dir = pathlib.Path(__file__).parent / "test_data" / "scenarios"

    # We need a scenario where liion has efficiency and loss_rate attributes
    # to test the multiple format update.
    # For now, let's just make sure the code runs without error.
    create_scenario("test", scenario_dir=scenario_dir, datapackage_dir=pkg_dir)

    data_path = pathlib.Path(__file__).parent / "test_data" / "raw" / "multiple.csv"

    apply_element_data(data_path, "test", "ALL", datapackage_dir=pkg_dir)

    # In test.yaml, liion instances only have region and capacity.
    # To test efficiency update, we'd need to add efficiency to attributes in test.yaml or have it inferred.
    # By default, Component.from_name("storage").attributes includes efficiency.
    # But in test.yaml, liion_storage doesn't specify attributes, so it uses all from storage.yaml.

    con = duckdb.connect(database=":memory:")
    csv_path = pkg_dir / "test" / "data/elements/liion_storage.csv"

    # Check if efficiency was updated
    res = con.execute(
        f"SELECT efficiency, loss_rate FROM read_csv_auto('{csv_path}', sep=';') WHERE name = 'liion'",  # noqa: S608
    ).fetchone()
    assert float(res[0]) == 0.9  # noqa: PLR2004
    assert float(res[1]) == 0.1  # noqa: PLR2004


def test_apply_sequence_data(tmp_path: Path) -> None:
    """Test applying sequence data to an existing datapackage."""
    pkg_dir = tmp_path / "datapackages"
    scenario_dir = pathlib.Path(__file__).parent / "test_data" / "scenarios"
    create_scenario("test", scenario_dir=scenario_dir, datapackage_dir=pkg_dir)

    data_path = pathlib.Path(__file__).parent / "test_data" / "raw" / "timeseries.csv"

    apply_sequence_data(
        data_path,
        "test",
        "liion_storage_profile",
        datapackage_dir=pkg_dir,
    )

    csv_path = pkg_dir / "test" / "data/sequences/liion_storage_profile.csv"

    con = duckdb.connect(database=":memory:")
    res = con.execute(
        f"SELECT efficiency, loss_rate FROM read_csv_auto('{csv_path}', sep=';') LIMIT 5",  # noqa: S608
    ).fetchall()

    assert float(res[0][0]) == 1.0
    assert float(res[4][0]) == 5.0  # noqa: PLR2004

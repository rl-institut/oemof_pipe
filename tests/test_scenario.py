"""Module to test scenario generation."""

import pathlib
from pathlib import Path

from scenario import load_scenario, apply_scenario_data
import duckdb


def test_load_scenario(tmp_path: Path) -> None:
    """Load the scenario from test scenarios folder and create datapackage."""
    # Setup temporary directories
    pkg_dir = tmp_path / "datapackages"
    scenario_dir = pathlib.Path(__file__).parent / "test_data" / "scenarios"

    load_scenario("test", scenario_dir=scenario_dir, datapackage_dir=pkg_dir)

    # Verify output
    expected_pkg_path = pkg_dir / "test"
    assert (expected_pkg_path / "datapackage.json").exists()
    assert (expected_pkg_path / "data/elements/electricity_demand.csv").exists()
    assert (expected_pkg_path / "data/elements/liion_storage.csv").exists()
    assert (
        expected_pkg_path / "data/sequences/electricity_demand_profile.csv"
    ).exists()


def test_apply_scenario_data_single(tmp_path: Path) -> None:
    """Test applying scenario data in single format."""
    pkg_dir = tmp_path / "datapackages"
    scenario_dir = pathlib.Path(__file__).parent / "test_data" / "scenarios"
    load_scenario("test", scenario_dir=scenario_dir, datapackage_dir=pkg_dir)

    data_path = pathlib.Path(__file__).parent / "test_data" / "raw" / "single.csv"

    # Apply data
    apply_scenario_data(data_path, "test", "ALL", datapackage_dir=pkg_dir)

    # Verify electricity_demand (l1) amount changed from default (none) to 10
    con = duckdb.connect(database=":memory:")
    csv_path = pkg_dir / "test" / "data/elements/electricity_demand.csv"
    res = con.execute(
        f"SELECT amount FROM read_csv_auto('{csv_path}', sep=';') WHERE name = 'l1'",  # noqa: S608
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
    load_scenario("test", scenario_dir=scenario_dir, datapackage_dir=pkg_dir)

    data_path = pathlib.Path(__file__).parent / "test_data" / "raw" / "multiple.csv"

    apply_scenario_data(data_path, "test", "ALL", datapackage_dir=pkg_dir)

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

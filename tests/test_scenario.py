"""Module to test scenario generation."""

import pathlib
from pathlib import Path

from scenario import load_scenario


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

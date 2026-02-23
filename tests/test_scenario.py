"""Module to test datapackage manipulation from scenario datasets."""

import pathlib
import shutil
from pathlib import Path

import duckdb

from scenario import apply_element_data, apply_sequence_data


def test_apply_scenario_data_single(tmp_path: Path) -> None:
    """Test applying blueprint data in single format."""
    tmp_package_dir = tmp_path / "datapackages"
    datapackage_dir = (
        pathlib.Path(__file__).parent / "test_data" / "datapackages" / "test"
    )
    shutil.copytree(datapackage_dir, tmp_package_dir / "test")

    data_path = pathlib.Path(__file__).parent / "test_data" / "raw" / "single.csv"
    apply_element_data(data_path, "test", "ALL", datapackage_dir=tmp_package_dir)

    # Verify electricity_demand (l1) amount changed from default (none) to 10
    con = duckdb.connect(database=":memory:")
    csv_path = tmp_package_dir / "test" / "data/elements/electricity_demand.csv"
    res = con.execute(
        f"SELECT amount FROM read_csv_auto('{csv_path}', sep=';') WHERE name = 'd1'",
    ).fetchone()
    assert res[0] == 10  # noqa: PLR2004

    # Verify liion_storage (liion) capacity changed from 100 to 99
    csv_path = tmp_package_dir / "test" / "data/elements/liion_storage.csv"
    res = con.execute(
        f"SELECT capacity FROM read_csv_auto('{csv_path}', sep=';') WHERE name = 'liion'",
    ).fetchone()
    assert res[0] == 99  # noqa: PLR2004


def test_apply_scenario_data_multiple(tmp_path: Path) -> None:
    """Test applying blueprint data in multiple format."""
    tmp_package_dir = tmp_path / "datapackages"
    datapackage_dir = (
        pathlib.Path(__file__).parent / "test_data" / "datapackages" / "test"
    )
    shutil.copytree(datapackage_dir, tmp_package_dir / "test")

    data_path = pathlib.Path(__file__).parent / "test_data" / "raw" / "multiple.csv"
    apply_element_data(data_path, "test", "ALL", datapackage_dir=tmp_package_dir)

    # In test.yaml, liion instances only have region and capacity.
    # To test efficiency update, we'd need to add efficiency to attributes in test.yaml or have it inferred.
    # By default, Component.from_name("storage").attributes includes efficiency.
    # But in test.yaml, liion_storage doesn't specify attributes, so it uses all from storage.yaml.

    con = duckdb.connect(database=":memory:")
    csv_path = tmp_package_dir / "test" / "data/elements/liion_storage.csv"

    # Check if efficiency was updated
    res = con.execute(
        f"SELECT efficiency, loss_rate FROM read_csv_auto('{csv_path}', sep=';') WHERE name = 'liion'",
    ).fetchone()
    assert float(res[0]) == 0.9  # noqa: PLR2004
    assert float(res[1]) == 0.1  # noqa: PLR2004


def test_apply_sequence_data_columnwise(tmp_path: Path) -> None:
    """Test applying sequence data to an existing datapackage."""
    tmp_package_dir = tmp_path / "datapackages"
    datapackage_dir = (
        pathlib.Path(__file__).parent / "test_data" / "datapackages" / "test"
    )
    shutil.copytree(datapackage_dir, tmp_package_dir / "test")

    data_path = pathlib.Path(__file__).parent / "test_data" / "raw" / "timeseries.csv"

    apply_sequence_data(
        data_path,
        "test",
        "liion_storage_profile",
        datapackage_dir=tmp_package_dir,
    )

    csv_path = tmp_package_dir / "test" / "data/sequences/liion_storage_profile.csv"

    con = duckdb.connect(database=":memory:")
    res = con.execute(
        f"SELECT efficiency, loss_rate FROM read_csv_auto('{csv_path}', sep=';') LIMIT 5",
    ).fetchall()

    assert float(res[0][0]) == 1.0
    assert float(res[4][0]) == 5.0  # noqa: PLR2004


def test_apply_sequence_data_rowwise(tmp_path: Path) -> None:
    """Test applying rowwise sequence data to an existing datapackage."""
    tmp_package_dir = tmp_path / "datapackages"
    datapackage_dir = (
        pathlib.Path(__file__).parent / "test_data" / "datapackages" / "regions"
    )
    shutil.copytree(datapackage_dir, tmp_package_dir / "regions")

    data_path = (
        pathlib.Path(__file__).parent / "test_data" / "raw" / "timeseries_single.csv"
    )

    apply_sequence_data(
        data_path,
        "regions",
        "electricity_demand_profile",
        datapackage_dir=tmp_package_dir,
        scenario="2050-el_eff",
        scenario_column="scenario_key",
    )

    csv_path = (
        tmp_package_dir / "regions" / "data/sequences/electricity_demand_profile.csv"
    )

    con = duckdb.connect(database=":memory:")
    res = con.execute(
        f"""SELECT "BB-d1-profile" FROM read_csv_auto('{csv_path}', sep=';') LIMIT 3""",
    ).fetchall()
    assert float(res[0][0]) == 4.586600128650665  # noqa: PLR2004
    assert float(res[1][0]) == 4.047  # noqa: PLR2004
    assert float(res[2][0]) == 3.3725000000000005  # noqa: PLR2004

"""Module to test datapackage manipulation from scenario datasets."""

import pathlib
import shutil
from pathlib import Path

import duckdb
import pandas as pd

from oemof_pipe import scenario
from oemof_pipe.scenario import apply_element_data, apply_sequence_data


def test_apply_scenario_data_single(tmp_path: Path) -> None:
    """Test applying blueprint data in single format."""
    tmp_package_dir = tmp_path / "datapackages"
    datapackage_dir = (
        pathlib.Path(__file__).parent / "test_data" / "datapackages" / "test"
    )
    shutil.copytree(datapackage_dir, tmp_package_dir / "test")

    data_path = pathlib.Path(__file__).parent / "test_data" / "raw" / "single.csv"
    apply_element_data(
        data_path,
        "test",
        ["2050-base", "2050-el_eff"],
        datapackage_dir=tmp_package_dir,
    )

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
    assert res[0] == 93  # noqa: PLR2004


def test_apply_scenario_data_from_df(tmp_path: Path) -> None:
    """Test applying blueprint data in single format."""
    tmp_package_dir = tmp_path / "datapackages"
    datapackage_dir = (
        pathlib.Path(__file__).parent / "test_data" / "datapackages" / "test"
    )
    shutil.copytree(datapackage_dir, tmp_package_dir / "test")

    data = pd.DataFrame([{"scenario": "ALL", "name": "d1", "amount": 22}])
    apply_element_data(data, "test", "ALL", datapackage_dir=tmp_package_dir)

    data = pd.DataFrame(
        [
            {
                "scenario": "ALL",
                "name": "electricity-demand-profile",
                "var_name": "electricity-demand-profile",
                "series": [31, 32, 33],
            },
        ],
    )
    apply_sequence_data(
        data,
        "test",
        "electricity_demand_profile",
        datapackage_dir=tmp_package_dir,
    )

    data = pd.DataFrame(
        {
            "timeindex": [
                "2026-01-01 00:00:00",
                "2026-01-01 01:00:00",
                "2026-01-01 02:00:00",
            ],
            "b": [55, 56, 57],
        },
    )
    apply_sequence_data(
        data,
        "test",
        "electricity_demand_profile",
        datapackage_dir=tmp_package_dir,
    )

    con = duckdb.connect(database=":memory:")

    csv_path = tmp_package_dir / "test" / "data/elements/electricity_demand.csv"
    res = con.execute(
        f"SELECT amount FROM read_csv_auto('{csv_path}', sep=';') WHERE name = 'd1'",
    ).fetchone()
    assert res[0] == 22  # noqa: PLR2004

    csv_path = (
        tmp_package_dir / "test" / "data/sequences/electricity_demand_profile.csv"
    )
    res = con.execute(
        f"SELECT * FROM read_csv_auto('{csv_path}', sep=';')",
    ).fetchall()
    assert res[0][1] == 31  # noqa: PLR2004
    assert res[0][2] == 55  # noqa: PLR2004


def test_apply_scenario_data_with_different_regions(tmp_path: Path) -> None:
    """Test applying blueprint data in single format."""
    tmp_package_dir = tmp_path / "datapackages"
    datapackage_dir = (
        pathlib.Path(__file__).parent / "test_data" / "datapackages" / "regions"
    )
    shutil.copytree(datapackage_dir, tmp_package_dir / "regions")

    data_path = pathlib.Path(__file__).parent / "test_data" / "raw" / "single.csv"
    apply_element_data(
        data_path,
        "regions",
        ["2050-base", "2050-el_eff"],
        datapackage_dir=tmp_package_dir,
    )

    with (tmp_package_dir / "regions/data/elements/electricity_demand.csv").open(
        "r",
    ) as f:
        lines = f.readlines()
        assert len(lines) == 5  # noqa: PLR2004
        assert lines[0].strip() == "amount;bus;profile;region;type;name"
        assert lines[1].strip() == "10;BB-electricity;BB-d1-profile;BB;load;BB-d1"
        assert lines[2].strip() == "10;B-electricity;B-d1-profile;B;load;B-d1"
        assert lines[3].strip() == "6;;BB-d2-profile;BB;load;BB-d2"
        assert lines[4].strip() == "5;;B-d2-profile;B;load;B-d2"


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
        scenario=["2050-base", "2050-el_eff"],
        scenario_column="scenario_key",
    )

    csv_path = (
        tmp_package_dir / "regions" / "data/sequences/electricity_demand_profile.csv"
    )

    con = duckdb.connect(database=":memory:")
    res = con.execute(
        f"""SELECT * FROM read_csv_auto('{csv_path}', sep=';') LIMIT 3""",
    ).fetchall()
    assert float(res[0][1]) == 3  # noqa: PLR2004
    assert float(res[1][1]) == 3  # noqa: PLR2004
    assert float(res[2][1]) == 3  # noqa: PLR2004
    assert float(res[0][2]) == 2  # noqa: PLR2004
    assert float(res[1][2]) == 2  # noqa: PLR2004
    assert float(res[2][2]) == 2  # noqa: PLR2004
    assert float(res[0][3]) == 4  # noqa: PLR2004
    assert float(res[1][3]) == 4  # noqa: PLR2004
    assert float(res[2][3]) == 4  # noqa: PLR2004
    assert float(res[0][4]) == 2  # noqa: PLR2004
    assert float(res[1][4]) == 2  # noqa: PLR2004
    assert float(res[2][4]) == 2  # noqa: PLR2004


def test_create_scenario() -> None:
    """Test applying scenario setup on datapackage."""
    datapackage_dir = pathlib.Path(__file__).parent / "test_data" / "datapackages"

    # Check before manipulation
    with (datapackage_dir / "test/data/elements/electricity_demand.csv").open("r") as f:
        lines = f.readlines()
        assert len(lines) == 3  # noqa: PLR2004
        assert lines[0].strip() == "region;amount;bus;type;name"
        assert lines[1].strip() == ";;electricity;load;d1"

    scenario.create_scenario(
        "test",
        scenario="2050-el_eff",
        datapackage_dir=datapackage_dir,
        scenario_dir=pathlib.Path(__file__).parent / "test_data" / "scenarios",
    )

    pkg_dir = datapackage_dir / "test_2050-el_eff"
    assert pkg_dir.exists()

    with (pkg_dir / "data/elements/electricity_demand.csv").open("r") as f:
        lines = f.readlines()
        assert len(lines) == 3  # noqa: PLR2004
        assert lines[0].strip() == "region;amount;bus;type;name"
        assert lines[1].strip() == ";10;electricity;load;d1"

    with (pkg_dir / "data/sequences/electricity_demand_profile.csv").open("r") as f:
        lines = f.readlines()
        assert len(lines) == 4  # noqa: PLR2004
        assert lines[0].strip() == "timeindex;electricity-demand-profile;b"
        assert lines[1].strip() == "2026-01-01 00:00:00;4.586600128650665;0"

    shutil.rmtree(pkg_dir)

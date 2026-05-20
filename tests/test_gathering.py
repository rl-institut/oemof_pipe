"""Tests for the gathering module."""

import pathlib

import pytest

from oemof_pipe.gathering import gather_element_data

TEST_DATAPACKAGE_DIR = pathlib.Path(__file__).parent / "test_data" / "datapackages"
TEST_DP_NAME = "test"


@pytest.fixture
def df():
    """Return gathered DataFrame for the test datapackage."""
    return gather_element_data(TEST_DP_NAME, datapackage_dir=TEST_DATAPACKAGE_DIR)


def test_output_columns(df):
    """Output must have exactly the expected columns in order."""
    expected = [
        "id",
        "scenario",
        "name",
        "var_name",
        "carrier",
        "region",
        "tech",
        "var_value",
        "var_unit",
        "source",
        "comment",
    ]
    assert list(df.columns) == expected


def test_type_column_absent(df):
    """The element type column must not appear as var_name."""
    assert "type" not in df["var_name"].values


def test_scenario_equals_datapackage_name(df):
    """Scenario column must equal the datapackage name."""
    assert (df["scenario"] == TEST_DP_NAME).all()


def test_empty_auxiliary_columns(df):
    """var_unit, source, and comment columns must be empty."""
    assert (df["var_unit"] == "").all()
    assert (df["source"] == "").all()
    assert (df["comment"] == "").all()


def test_carrier_region_tech_copied(df):
    """carrier/region/tech are copied as metadata, not transposed."""
    assert "carrier" not in df["var_name"].values
    assert "region" not in df["var_name"].values
    assert "tech" not in df["var_name"].values


def test_component_names_present(df):
    """Known component names from test datapackage must appear."""
    names = set(df["name"])
    assert "d1" in names
    assert "d2" in names
    assert "liion" in names
    assert "ex" in names


def test_var_name_value_transposed(df):
    """Specific known attribute values must be present."""
    liion_capacity = df[(df["name"] == "liion") & (df["var_name"] == "capacity")]
    assert len(liion_capacity) == 1
    assert liion_capacity.iloc[0]["var_value"] == "99"


def test_region_copied_for_component(df):
    """Region value must be copied from the source row."""
    d2_rows = df[df["name"] == "d2"]
    assert (d2_rows["region"] == "test_region").all()


def test_multiple_datapackages():
    """Two datapackage names produce rows from both."""
    df = gather_element_data(
        [TEST_DP_NAME, TEST_DP_NAME],
        datapackage_dir=TEST_DATAPACKAGE_DIR,
    )
    single = gather_element_data(TEST_DP_NAME, datapackage_dir=TEST_DATAPACKAGE_DIR)
    assert len(df) == 2 * len(single)


def test_missing_datapackage_returns_empty():
    """A non-existent datapackage name produces an empty DataFrame."""
    df = gather_element_data("does_not_exist", datapackage_dir=TEST_DATAPACKAGE_DIR)
    assert df.empty
    assert list(df.columns) == [
        "id",
        "scenario",
        "name",
        "var_name",
        "carrier",
        "region",
        "tech",
        "var_value",
        "var_unit",
        "source",
        "comment",
    ]


def test_id_is_sequential(df):
    """Id column must be a sequential integer index starting at 0."""
    assert list(df["id"]) == list(range(len(df)))


def test_empty_only_filters_non_empty():
    """empty_only=True must return only rows with an empty var_value."""
    full = gather_element_data(TEST_DP_NAME, datapackage_dir=TEST_DATAPACKAGE_DIR)
    filtered = gather_element_data(
        TEST_DP_NAME, datapackage_dir=TEST_DATAPACKAGE_DIR, empty_only=True
    )
    assert (filtered["var_value"] == "").all()
    assert len(filtered) < len(full)


def test_empty_only_id_is_sequential():
    """id must remain sequential after empty_only filtering."""
    filtered = gather_element_data(
        TEST_DP_NAME, datapackage_dir=TEST_DATAPACKAGE_DIR, empty_only=True
    )
    assert list(filtered["id"]) == list(range(len(filtered)))

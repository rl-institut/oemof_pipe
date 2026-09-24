"""Tests for the CLI entry point in oemof_pipe/main.py."""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from oemof_pipe import main


@pytest.mark.parametrize(
    ("extra_args", "expected_start", "expected_periods"),
    [
        ([], None, None),
        (
            ["--start", "2050-01-01 00:00:00", "--periods", "5"],
            "2050-01-01 00:00:00",
            "5",
        ),
    ],
    ids=["without_timeindex", "with_timeindex"],
)
def test_blueprint_command_forwards_timeindex_args(
    extra_args: list[str],
    expected_start: str | None,
    expected_periods: str | None,
) -> None:
    """Blueprint CLI command forwards timeindex args to create_blueprint, with/without timeindexing."""
    argv = ["main.py", "blueprint", "test_bp", *extra_args]
    with (
        patch.object(sys, "argv", argv),
        patch("oemof_pipe.main.check_overriding_of_datapackage") as mock_check,
        patch("oemof_pipe.blueprint.create_blueprint") as mock_create,
    ):
        main.main()

    mock_check.assert_called_once_with("test_bp", override=False)
    mock_create.assert_called_once_with(
        "test_bp",
        datapackage_name=None,
        timeindex_start=expected_start,
        timeindex_periods=expected_periods,
    )


def test_blueprint_command_force_flag_overrides_existing_datapackage() -> None:
    """The -f flag is forwarded as override=True to the overriding check."""
    argv = ["main.py", "blueprint", "test_bp", "-f"]
    with (
        patch.object(sys, "argv", argv),
        patch("oemof_pipe.main.check_overriding_of_datapackage") as mock_check,
        patch("oemof_pipe.blueprint.create_blueprint"),
    ):
        main.main()

    mock_check.assert_called_once_with("test_bp", override=True)


def test_check_overriding_of_datapackage_removes_existing_when_forced(
    tmp_path: Path,
) -> None:
    """Existing datapackage directory is removed when override is requested."""
    existing = tmp_path / "test_bp"
    existing.mkdir()

    main.check_overriding_of_datapackage(
        "test_bp", override=True, datapackage_dir=tmp_path
    )

    assert not existing.exists()


def test_check_overriding_of_datapackage_raises_without_force(tmp_path: Path) -> None:
    """Existing datapackage directory raises FileExistsError without override."""
    existing = tmp_path / "test_bp"
    existing.mkdir()

    with pytest.raises(FileExistsError):
        main.check_overriding_of_datapackage(
            "test_bp", override=False, datapackage_dir=tmp_path
        )

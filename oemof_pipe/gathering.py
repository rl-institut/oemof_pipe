"""Module to gather element data from datapackages into single CSV format."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

import pandas as pd

from . import settings


_METADATA_COLS: frozenset[str] = frozenset(
    {"name", "type", "carrier", "region", "tech"},
)
_COPY_COLS: tuple[str, ...] = ("carrier", "region", "tech")
_OUTPUT_COLS: tuple[str, ...] = (
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
)


def gather_element_data(
    datapackage_names: str | list[str],
    datapackage_dir: Path = settings.DATAPACKAGE_DIR,
    *,
    empty_only: bool = False,
) -> pd.DataFrame:
    """
    Gather element data from datapackages into single CSV format.

    Scans all element CSV files in ``data/elements/`` for each datapackage and
    transposes each component row into long format matching the single.csv
    input schema. The ``type`` column is omitted; ``carrier``, ``region``, and
    ``tech`` are copied as metadata; all remaining columns are transposed into
    ``var_name`` / ``var_value`` pairs.

    Args:
        datapackage_names: Name or list of datapackage names to scan.
        datapackage_dir: Base directory that contains datapackage folders.
        empty_only: If ``True``, return only rows where ``var_value`` is empty.

    Returns:
        DataFrame with columns: ``id``, ``scenario``, ``name``, ``var_name``,
        ``carrier``, ``region``, ``tech``, ``var_value``, ``var_unit``,
        ``source``, ``comment``.

    Raises:
        ValueError: If none of the requested datapackages exist.

    Examples:
        >>> df = gather_element_data("my_datapackage")
        >>> df = gather_element_data(["dp_a", "dp_b"], empty_only=True)

    """
    if isinstance(datapackage_names, str):
        datapackage_names = [datapackage_names]

    frames: list[pd.DataFrame] = []
    for dp_name in datapackage_names:
        elements_dir = datapackage_dir / dp_name / "data" / "elements"
        if not elements_dir.is_dir():
            settings.logger.warning(
                f"Elements directory not found for datapackage '{dp_name}': {elements_dir}",
            )
            continue

        for csv_path in sorted(elements_dir.glob("*.csv")):
            settings.logger.debug(f"Gathering element data from '{csv_path}'.")
            element_df = pd.read_csv(
                csv_path,
                sep=";",
                dtype=str,
                keep_default_na=False,
            )
            frames.append(_transpose_element(element_df, dp_name))

    if not frames:
        return pd.DataFrame(columns=list(_OUTPUT_COLS))

    result = pd.concat(frames, ignore_index=True)
    if empty_only:
        result = result[result["var_value"] == ""]
    result = result.reset_index(drop=True)
    result.insert(0, "id", range(len(result)))
    return result[list(_OUTPUT_COLS)]


def _transpose_element(df: pd.DataFrame, scenario: str) -> pd.DataFrame:
    """
    Transpose one element DataFrame from wide to long format.

    Args:
        df: Element DataFrame read from a single element CSV.
        scenario: Datapackage name used as the ``scenario`` column value.

    Returns:
        Long-format DataFrame with one row per (component, attribute) pair.

    """
    copy_cols = [col for col in _COPY_COLS if col in df.columns]
    value_cols = [col for col in df.columns if col not in _METADATA_COLS]

    id_vars = ["name", *copy_cols]
    melted = df[id_vars + value_cols].melt(
        id_vars=id_vars,
        value_vars=value_cols,
        var_name="var_name",
        value_name="var_value",
    )

    melted["scenario"] = scenario
    melted["var_unit"] = ""
    melted["source"] = ""
    melted["comment"] = ""

    for col in _COPY_COLS:
        if col not in melted.columns:
            melted[col] = ""

    return melted

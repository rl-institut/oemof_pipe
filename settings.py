"""Module to set up project from envs."""

import os
import pathlib
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

COMPONENTS_DIR = pathlib.Path(
    os.environ.get("COMPONENTS_DIR", pathlib.Path(__file__).parent / "components"),
)
DATAPACKAGE_DIR = pathlib.Path(
    os.environ.get("DATAPACKAGE_DIR", pathlib.Path.cwd() / "datapackages"),
)
BLUEPRINT_DIR = pathlib.Path(
    os.environ.get("BLUEPRINT_DIR", pathlib.Path.cwd() / "blueprints"),
)
SCENARIO_DIR = pathlib.Path(
    os.environ.get("SCENARIO_DIR", pathlib.Path.cwd() / "scenarios"),
)
RAW_DIR = pathlib.Path(
    os.environ.get("RAW_DIR", pathlib.Path(__file__).parent / "raw"),
)

logger.level("DEBUG")

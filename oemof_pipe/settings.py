"""Module to set up project from envs."""

import os
import sys
import pathlib
import dotenv
from loguru import logger

dotenv.load_dotenv(dotenv.find_dotenv(usecwd=True))

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
    os.environ.get("RAW_DIR", pathlib.Path.cwd() / "raw"),
)

DEBUG = os.environ.get("DEBUG", "False") == "True"
logger.remove()  # remove default DEBUG handler
if DEBUG:
    logger.add(sys.stderr, level="DEBUG")
    logger.info("Running oemof-pipe in debug mode.")
else:
    logger.add(sys.stderr, level="INFO")

# S3 Support in duckDB
S3_ENDPOINT = os.environ.get("S3_ENDPOINT")
S3_ACCES_KEY = os.environ.get("S3_ACCES_KEY")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY")

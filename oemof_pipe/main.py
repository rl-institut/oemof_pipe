"""Main module to run package from CLI."""

import argparse
import shutil
from pathlib import Path

from oemof_pipe import blueprint, scenario, settings


def check_overriding_of_datapackage(
    datapackage_name: str,
    *,
    override: bool = False,
    datapackage_dir: Path = settings.DATAPACKAGE_DIR,
) -> None:
    """Check if datapackage exists."""
    if (datapackage_dir / datapackage_name).exists():
        if override:
            shutil.rmtree(datapackage_dir / datapackage_name)
        else:
            error_msg = f"Datapackage '{datapackage_name}' already exists. Use -f (force) to override it."
            raise FileExistsError(error_msg)


def blueprint_command(args: argparse.Namespace) -> None:
    """Run blueprint command."""
    check_overriding_of_datapackage(args.blueprint_name, override=args.force)
    blueprint.create_blueprint(args.blueprint_name)


def scenario_command(args: argparse.Namespace) -> None:
    """Run scenario command."""
    datapackage_name = f"{args.datapackage_name}_{args.scenario_name}"
    check_overriding_of_datapackage(datapackage_name, override=args.force)
    scenario.create_scenario(args.datapackage_name, args.scenario_name)


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    blueprint_parser = subparsers.add_parser("blueprint")
    blueprint_parser.add_argument("blueprint_name")
    blueprint_parser.add_argument(
        "-f",
        dest="force",
        action="store_true",
        help="Override datapackage if it exists.",
    )
    blueprint_parser.set_defaults(func=blueprint_command)

    scenario_parser = subparsers.add_parser("scenario")
    scenario_parser.add_argument("datapackage_name")
    scenario_parser.add_argument("scenario_name")
    scenario_parser.add_argument(
        "-f",
        dest="force",
        action="store_true",
        help="Override datapackage if it exists.",
    )
    scenario_parser.set_defaults(func=scenario_command)

    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

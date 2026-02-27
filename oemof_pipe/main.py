"""Main module to run package from CLI."""

import argparse

from oemof_pipe import blueprint, scenario


def blueprint_command(args: argparse.Namespace) -> None:
    """Run blueprint command."""
    blueprint.create_blueprint(args.blueprint_name)


def scenario_command(args: argparse.Namespace) -> None:
    """Run scenario command."""
    scenario.create_scenario(args.datapackage_name, args.scenario_name)


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    blueprint_parser = subparsers.add_parser("blueprint")
    blueprint_parser.add_argument("blueprint_name")
    blueprint_parser.set_defaults(func=blueprint_command)

    scenario_parser = subparsers.add_parser("scenario")
    scenario_parser.add_argument("datapackage_name")
    scenario_parser.add_argument("scenario_name")
    scenario_parser.set_defaults(func=scenario_command)

    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

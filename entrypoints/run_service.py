#!/usr/bin/env python3
"""
Generic CLI to invoke a single service function directly — for ad-hoc single-restaurant
runs, backfills, or debugging a single step without running its full pipeline entrypoint.

Arguments are derived from the target function's signature: parameters with no default
become required positional args, bool defaults become --flags, everything else becomes
an optional --name value (using the parameter name verbatim, underscores included).

Usage:
    python -m entrypoints.run_service <module> <function> [args...]

Examples:
    python -m entrypoints.run_service services.instagram.fetch_ig_social fetch_social --slug my-restaurant
    python -m entrypoints.run_service services.instagram.fetch_ig_comments fetch_comments some-restaurant-id
    python -m entrypoints.run_service services.scraping.scrape_reviews scrape_reviews --slug my-restaurant
    python -m entrypoints.run_service services.scraping.embed_silver_restaurants embed --force
    python -m entrypoints.run_service services.wolt.match_wolt match_bronze_to_silver
"""

import argparse
import importlib
import inspect
import sys


def build_parser(func) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=f"{func.__module__}.{func.__name__}")
    for name, param in inspect.signature(func).parameters.items():
        if param.default is inspect.Parameter.empty:
            parser.add_argument(name)
        elif isinstance(param.default, bool):
            parser.add_argument(f"--{name}", action="store_true")
        elif isinstance(param.default, int):
            parser.add_argument(f"--{name}", type=int, default=param.default)
        else:
            parser.add_argument(f"--{name}", default=param.default)
    return parser


def main() -> int:
    if len(sys.argv) < 3:
        print(__doc__)
        return 1

    module_path, func_name, *rest = sys.argv[1:]

    try:
        module = importlib.import_module(module_path)
        func = getattr(module, func_name)
    except (ModuleNotFoundError, AttributeError) as error:
        print(f"❌ Could not resolve {module_path}.{func_name}: {error}")
        return 1

    args = build_parser(func).parse_args(rest)

    try:
        result = func(**vars(args))
    except (KeyError, RuntimeError) as error:
        print(f"❌ {error}")
        return 1

    if result is not None:
        print(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())

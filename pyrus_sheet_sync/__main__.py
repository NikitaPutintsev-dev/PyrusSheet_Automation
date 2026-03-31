from __future__ import annotations

import argparse
import logging
import os
import sys

from dotenv import load_dotenv


def main(argv: list[str] | None = None) -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Sync Google Sheets rows to Pyrus form tasks.")
    parser.add_argument(
        "--mapping",
        default=os.environ.get("MAPPING_CONFIG_PATH", "config/mapping.yaml"),
        help="Path to mapping YAML",
    )
    parser.add_argument(
        "--spreadsheet",
        default=os.environ.get("SPREADSHEET_ID"),
        help="Google Spreadsheet ID (or SPREADSHEET_ID env)",
    )
    parser.add_argument(
        "--log-file",
        default=os.environ.get("LOG_FILE_PATH"),
        help="Append logs to this file (optional)",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="DEBUG logging")
    args = parser.parse_args(argv)

    level = logging.DEBUG if args.verbose else logging.INFO
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if args.log_file:
        handlers.append(logging.FileHandler(args.log_file, encoding="utf-8"))
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=handlers,
    )

    if not args.spreadsheet:
        parser.error("SPREADSHEET_ID or --spreadsheet is required")

    os.environ["SPREADSHEET_ID"] = args.spreadsheet
    os.environ["MAPPING_CONFIG_PATH"] = args.mapping

    from pyrus_sheet_sync.runner import main_from_env

    main_from_env()


if __name__ == "__main__":
    main()

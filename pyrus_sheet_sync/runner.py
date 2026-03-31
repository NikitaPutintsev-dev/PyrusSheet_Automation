from __future__ import annotations

import logging
import os
from typing import Any

from pyrus_sheet_sync.config import AppConfig, env_int, load_mapping_yaml
from pyrus_sheet_sync.columns import col_letters_to_index as col_to_idx
from pyrus_sheet_sync.mapping import build_task_payload, row_has_mapped_data
from pyrus_sheet_sync.pyrus import PyrusApiError, PyrusClient
from pyrus_sheet_sync.sheets import (
    append_log_row,
    build_sheets_service,
    compute_data_a1_range,
    pad_row,
    read_range_data,
    update_state_cells,
)

logger = logging.getLogger(__name__)


def _task_id_from_row(row: list[Any], col_letter: str) -> str:
    idx = col_to_idx(col_letter)
    if idx >= len(row):
        return ""
    v = row[idx]
    if v is None:
        return ""
    return str(v).strip()


def run_sync(
    spreadsheet_id: str,
    mapping_path: str,
    pyrus_login: str,
    pyrus_security_key: str,
    form_id: int,
    pyrus_api_url: str | None = None,
) -> None:
    cfg = load_mapping_yaml(mapping_path)
    a1, _first_data, _last = compute_data_a1_range(cfg)
    max_width = col_to_idx(a1.split(":")[-1].rstrip("0123456789")) + 1

    sheets = build_sheets_service()
    sheet_name = cfg.sheet.name

    rows = read_range_data(sheets, spreadsheet_id, sheet_name, a1)
    logger.info("Read %s rows from %s!%s", len(rows), sheet_name, a1)

    with PyrusClient(
        pyrus_login, pyrus_security_key, api_base=pyrus_api_url or os.environ.get("PYRUS_API_URL")
    ) as pyrus:
        for idx, raw_row in enumerate(rows):
            sheet_row = cfg.sheet.header_row + idx
            if idx == 0:
                continue

            row = pad_row(list(raw_row), max_width)
            existing_id = _task_id_from_row(row, cfg.state.task_id_column)
            if existing_id:
                logger.debug("Skip row %s: already has task id %s", sheet_row, existing_id)
                continue

            if not row_has_mapped_data(row, cfg.field_mappings):
                logger.debug("Skip row %s: no mapped data", sheet_row)
                continue

            try:
                payload = build_task_payload(cfg, form_id, row)
            except Exception as e:
                msg = f"mapping error: {e}"
                logger.exception("Row %s: %s", sheet_row, msg)
                _log_sheet(cfg, sheets, spreadsheet_id, sheet_row, "ERROR", msg)
                update_state_cells(
                    sheets, spreadsheet_id, sheet_name, sheet_row, cfg, None, "error", msg
                )
                continue

            logger.info("Row %s: creating Pyrus task", sheet_row)
            try:
                resp = pyrus.create_task(payload)
            except PyrusApiError as e:
                msg = f"Pyrus API: {e}"
                if e.status_code:
                    msg = f"Pyrus HTTP {e.status_code}: {e}"
                logger.error("Row %s: %s", sheet_row, msg)
                _log_sheet(cfg, sheets, spreadsheet_id, sheet_row, "ERROR", msg)
                update_state_cells(
                    sheets, spreadsheet_id, sheet_name, sheet_row, cfg, None, "error", msg[:500]
                )
                continue
            except Exception as e:
                msg = f"unexpected: {e}"
                logger.exception("Row %s: %s", sheet_row, msg)
                _log_sheet(cfg, sheets, spreadsheet_id, sheet_row, "ERROR", msg)
                update_state_cells(
                    sheets, spreadsheet_id, sheet_name, sheet_row, cfg, None, "error", msg[:500]
                )
                continue

            task = resp.get("task") or {}
            new_id = task.get("id")
            if new_id is None:
                msg = "response missing task.id"
                logger.error("Row %s: %s", sheet_row, msg)
                _log_sheet(cfg, sheets, spreadsheet_id, sheet_row, "ERROR", msg)
                update_state_cells(
                    sheets, spreadsheet_id, sheet_name, sheet_row, cfg, None, "error", msg
                )
                continue

            tid = str(new_id)
            update_state_cells(sheets, spreadsheet_id, sheet_name, sheet_row, cfg, tid, "ok", "")
            ok_msg = f"created task {tid}"
            logger.info("Row %s: %s", sheet_row, ok_msg)
            _log_sheet(cfg, sheets, spreadsheet_id, sheet_row, "INFO", ok_msg)


def _log_sheet(
    cfg: AppConfig,
    sheets: Any,
    spreadsheet_id: str,
    sheet_row: int,
    level: str,
    message: str,
) -> None:
    name = cfg.logging.log_sheet_name
    if not name:
        return
    try:
        append_log_row(sheets, spreadsheet_id, name, sheet_row, level, message)
    except Exception:
        logger.exception("Failed to append log sheet row")


def main_from_env() -> None:
    spreadsheet_id = os.environ.get("SPREADSHEET_ID")
    if not spreadsheet_id:
        raise RuntimeError("SPREADSHEET_ID is required")
    mapping_path = os.environ.get("MAPPING_CONFIG_PATH", "config/mapping.yaml")
    login = os.environ["PYRUS_LOGIN"]
    key = os.environ["PYRUS_SECURITY_KEY"]
    form_id = env_int("PYRUS_FORM_ID")
    api_url = os.environ.get("PYRUS_API_URL") or None
    run_sync(spreadsheet_id, mapping_path, login, key, form_id, pyrus_api_url=api_url)

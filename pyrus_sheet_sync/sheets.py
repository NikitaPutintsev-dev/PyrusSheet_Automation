from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build

from pyrus_sheet_sync.columns import col_letters_to_index, index_to_col_letters, max_column_index
from pyrus_sheet_sync.config import AppConfig, load_google_credentials_from_env

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def build_sheets_service():
    info = load_google_credentials_from_env()
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _sheet_escape(name: str) -> str:
    return name.replace("'", "''")


def read_range_data(
    service: Any,
    spreadsheet_id: str,
    sheet_name: str,
    a1_range: str,
) -> list[list[Any]]:
    rng = f"'{_sheet_escape(sheet_name)}'!{a1_range}"
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=rng, majorDimension="ROWS")
        .execute()
    )
    return result.get("values") or []


def compute_data_a1_range(cfg: AppConfig) -> tuple[str, int, int]:
    """Return (A1 range without sheet prefix, first_data_row_1based, last_row_1based)."""
    letters = [cfg.state.task_id_column, cfg.state.status_column, cfg.state.error_column]
    letters.append(cfg.state.processed_at_column)
    for r in cfg.field_mappings:
        letters.append(r.column)
    if cfg.task_options.due_date_column:
        letters.append(cfg.task_options.due_date_column)
    if cfg.task_options.participants_column:
        letters.append(cfg.task_options.participants_column)

    indices = [col_letters_to_index(x) for x in letters]
    # Always read from column A so in-memory row indices match A=0, B=1, …
    min_col = "A"
    max_i = max_column_index(*indices)
    max_col = index_to_col_letters(max_i)

    header = cfg.sheet.header_row
    first_data = header + 1
    last = cfg.sheet.read_to_row
    a1 = f"{min_col}{header}:{max_col}{last}"
    return a1, first_data, last


def pad_row(row: list[Any], width: int) -> list[Any]:
    if len(row) >= width:
        return row
    return row + [""] * (width - len(row))


def update_state_cells(
    service: Any,
    spreadsheet_id: str,
    sheet_name: str,
    row_1based: int,
    cfg: AppConfig,
    task_id: str | None,
    status: str,
    error: str,
) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cols = [
        (cfg.state.task_id_column, task_id or ""),
        (cfg.state.status_column, status),
        (cfg.state.error_column, error[:500] if error else ""),
        (cfg.state.processed_at_column, now if status == "ok" else ""),
    ]
    for col_letter, val in cols:
        rng = f"'{_sheet_escape(sheet_name)}'!{col_letter}{row_1based}"
        body = {"values": [[val]]}
        (
            service.spreadsheets()
            .values()
            .update(
                spreadsheetId=spreadsheet_id,
                range=rng,
                valueInputOption="USER_ENTERED",
                body=body,
            )
            .execute()
        )


def append_log_row(
    service: Any,
    spreadsheet_id: str,
    log_sheet: str,
    sheet_row: int,
    level: str,
    message: str,
) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rng = f"'{_sheet_escape(log_sheet)}'!A:D"
    body = {"values": [[ts, str(sheet_row), level, message[:2000]]]}
    (
        service.spreadsheets()
        .values()
        .append(
            spreadsheetId=spreadsheet_id,
            range=rng,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body=body,
        )
        .execute()
    )

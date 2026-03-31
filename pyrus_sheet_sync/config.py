from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class SheetConfig:
    name: str
    header_row: int = 1
    read_to_row: int = 1002


@dataclass
class StateColumns:
    task_id_column: str = "I"
    status_column: str = "J"
    error_column: str = "K"
    processed_at_column: str = "L"


@dataclass
class FieldMappingRule:
    column: str
    type: str = "text"
    id: int | None = None
    code: str | None = None
    name: str | None = None


@dataclass
class TaskOptions:
    subject_template: str | None = None
    due_date_column: str | None = None
    participants_column: str | None = None


@dataclass
class LoggingConfig:
    log_sheet_name: str | None = None


@dataclass
class AppConfig:
    sheet: SheetConfig
    state: StateColumns
    field_mappings: list[FieldMappingRule]
    task_options: TaskOptions = field(default_factory=TaskOptions)
    logging: LoggingConfig = field(default_factory=LoggingConfig)


def load_mapping_yaml(path: str | Path) -> AppConfig:
    path = Path(path)
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("mapping yaml root must be a mapping")

    sc = raw.get("sheet") or {}
    sheet = SheetConfig(
        name=str(sc.get("name", "Sheet1")),
        header_row=int(sc.get("header_row", 1)),
        read_to_row=int(sc.get("read_to_row", 1002)),
    )

    st = raw.get("state") or {}
    state = StateColumns(
        task_id_column=str(st.get("task_id_column", "I")).upper(),
        status_column=str(st.get("status_column", "J")).upper(),
        error_column=str(st.get("error_column", "K")).upper(),
        processed_at_column=str(st.get("processed_at_column", "L")).upper(),
    )

    rules_raw = raw.get("field_mappings") or []
    field_mappings: list[FieldMappingRule] = []
    for item in rules_raw:
        if not isinstance(item, dict):
            continue
        col = str(item.get("column", "")).upper()
        if not col:
            continue
        field_mappings.append(
            FieldMappingRule(
                column=col,
                type=str(item.get("type", "text")).lower(),
                id=item.get("id"),
                code=item.get("code"),
                name=item.get("name"),
            )
        )

    to = raw.get("task_options") or {}
    task_options = TaskOptions(
        subject_template=to.get("subject_template"),
        due_date_column=(str(to["due_date_column"]).upper() if to.get("due_date_column") else None),
        participants_column=(
            str(to["participants_column"]).upper() if to.get("participants_column") else None
        ),
    )

    lg = raw.get("logging") or {}
    logging_cfg = LoggingConfig(log_sheet_name=lg.get("log_sheet_name"))

    return AppConfig(
        sheet=sheet,
        state=state,
        field_mappings=field_mappings,
        task_options=task_options,
        logging=logging_cfg,
    )


def load_google_credentials_from_env() -> dict[str, Any]:
    """Return service account info dict for google.auth."""
    path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if path and Path(path).is_file():
        return json.loads(Path(path).read_text(encoding="utf-8"))
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if raw:
        return json.loads(raw)
    raise RuntimeError(
        "Set GOOGLE_APPLICATION_CREDENTIALS to a JSON key file path "
        "or GOOGLE_SERVICE_ACCOUNT_JSON to the JSON string."
    )


def env_int(name: str, default: int | None = None) -> int:
    v = os.environ.get(name)
    if v is None or v == "":
        if default is None:
            raise RuntimeError(f"Missing required environment variable: {name}")
        return default
    return int(v)

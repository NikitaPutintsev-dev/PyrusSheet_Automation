from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from pyrus_sheet_sync.columns import col_letters_to_index
from pyrus_sheet_sync.config import AppConfig, FieldMappingRule


def _cell_str(row: list[Any], col_letter: str) -> str:
    idx = col_letters_to_index(col_letter)
    if idx >= len(row):
        return ""
    v = row[idx]
    if v is None:
        return ""
    return str(v).strip()


def _is_empty_cell(row: list[Any], col_letter: str) -> bool:
    return _cell_str(row, col_letter) == ""


def row_has_mapped_data(row: list[Any], rules: list[FieldMappingRule]) -> bool:
    return any(not _is_empty_cell(row, r.column) for r in rules)


def apply_subject_template(template: str, row: list[Any]) -> str:
    def repl(match: re.Match[str]) -> str:
        col = match.group(1).upper()
        return _cell_str(row, col)

    return re.sub(r"\{([A-Za-z]+)\}", repl, template)


def coerce_value(raw: str, field_type: str) -> Any:
    if raw == "":
        return None
    t = field_type.lower()
    if t in ("text", "string", "email", "phone"):
        return raw
    if t in ("number", "money", "integer"):
        cleaned = raw.replace(" ", "").replace(",", ".")
        if "." in cleaned:
            return float(cleaned)
        return int(cleaned)
    if t in ("date", "due_date"):
        for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(raw, fmt).date().isoformat()
            except ValueError:
                continue
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return parsed.date().isoformat()
        except ValueError:
            return raw
    if t == "catalog":
        # Expect numeric item_id in cell; advanced YAML object mapping can be added later
        try:
            return {"item_id": int(raw)}
        except ValueError:
            return raw
    if t == "checkmark":
        lower = raw.lower()
        return lower in ("1", "true", "yes", "да", "x", "v")
    return raw


def build_pyrus_field_entry(rule: FieldMappingRule, row: list[Any]) -> dict[str, Any] | None:
    raw = _cell_str(row, rule.column)
    if raw == "":
        return None
    value = coerce_value(raw, rule.type)
    if value is None:
        return None
    entry: dict[str, Any] = {"value": value}
    if rule.id is not None:
        entry["id"] = rule.id
    if rule.code:
        entry["code"] = rule.code
    if rule.name:
        entry["name"] = rule.name
    if not any(k in entry for k in ("id", "code", "name")):
        raise ValueError(f"Field mapping for column {rule.column} needs id, code, or name")
    return entry


def build_fields_for_row(cfg: AppConfig, row: list[Any]) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    for rule in cfg.field_mappings:
        entry = build_pyrus_field_entry(rule, row)
        if entry is not None:
            fields.append(entry)
    return fields


def build_participants(cfg: AppConfig, row: list[Any]) -> list[dict[str, str]] | None:
    col = cfg.task_options.participants_column
    if not col:
        return None
    raw = _cell_str(row, col)
    if not raw:
        return None
    emails = [p.strip() for p in raw.replace(";", ",").split(",") if p.strip()]
    if not emails:
        return None
    return [{"email": e} for e in emails]


def build_task_payload(cfg: AppConfig, form_id: int, row: list[Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {"form_id": form_id, "fields": build_fields_for_row(cfg, row)}
    to = cfg.task_options
    if to.subject_template:
        payload["text"] = apply_subject_template(to.subject_template, row)
    if to.due_date_column:
        due_raw = _cell_str(row, to.due_date_column)
        if due_raw:
            payload["due_date"] = coerce_value(due_raw, "date")
    participants = build_participants(cfg, row)
    if participants:
        payload["participants"] = participants
    return payload

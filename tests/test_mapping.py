from pathlib import Path

import pytest
import yaml

from pyrus_sheet_sync.config import (
    AppConfig,
    FieldMappingRule,
    SheetConfig,
    StateColumns,
    TaskOptions,
    load_mapping_yaml,
)
from pyrus_sheet_sync.mapping import (
    apply_subject_template,
    build_fields_for_row,
    build_task_payload,
    coerce_value,
    row_has_mapped_data,
)


def test_coerce_number_and_date() -> None:
    assert coerce_value("10", "number") == 10
    assert coerce_value("3.5", "number") == 3.5
    assert coerce_value("1 000,5", "money") == 1000.5
    # Google Sheets locale: NBSP / narrow NBSP as thousands separator
    assert coerce_value("1\u00a0223.00", "money") == 1223.0
    assert coerce_value("12\u202f040,5", "number") == 12040.5
    assert coerce_value("2025-12-01", "date") == "2025-12-01"
    assert coerce_value("31.03.2026", "date") == "2026-03-31"


def test_coerce_empty() -> None:
    assert coerce_value("", "text") is None


def test_row_has_mapped_data() -> None:
    rules = [FieldMappingRule(column="A", id=1)]
    assert not row_has_mapped_data([], rules)
    assert not row_has_mapped_data([""], rules)
    assert row_has_mapped_data(["x"], rules)


def test_build_fields_skips_empty() -> None:
    cfg = AppConfig(
        sheet=SheetConfig(name="S"),
        state=StateColumns(),
        field_mappings=[
            FieldMappingRule(column="A", id=1, type="text"),
            FieldMappingRule(column="B", code="C", type="text"),
        ],
    )
    row = ["hello", ""]
    fields = build_fields_for_row(cfg, row)
    assert fields == [{"id": 1, "value": "hello"}]


def test_subject_template() -> None:
    row = ["Acme", "ignored"]
    assert apply_subject_template("Клиент {A}", row) == "Клиент Acme"


def test_build_task_payload_participants(tmp_path: Path) -> None:
    cfg = AppConfig(
        sheet=SheetConfig(name="S"),
        state=StateColumns(),
        field_mappings=[FieldMappingRule(column="A", id=1, type="text")],
        task_options=TaskOptions(participants_column="B"),
    )
    row = ["t", "a@b.com, c@d.org"]
    p = build_task_payload(cfg, 99, row)
    assert p["form_id"] == 99
    assert p["participants"] == [{"email": "a@b.com"}, {"email": "c@d.org"}]


def test_load_mapping_yaml_roundtrip(tmp_path: Path) -> None:
    p = tmp_path / "m.yaml"
    p.write_text(
        yaml.dump(
            {
                "sheet": {"name": "D", "header_row": 2, "read_to_row": 50},
                "state": {"task_id_column": "z"},
                "field_mappings": [{"column": "a", "id": 3, "type": "number"}],
            }
        ),
        encoding="utf-8",
    )
    cfg = load_mapping_yaml(p)
    assert cfg.sheet.header_row == 2
    assert cfg.state.task_id_column == "Z"
    assert cfg.field_mappings[0].column == "A"

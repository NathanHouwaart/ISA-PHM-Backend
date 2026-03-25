from __future__ import annotations

from typing import Any, Tuple


def parse_numeric_if_possible(raw_value: Any) -> Tuple[Any, bool]:
    if raw_value is None:
        return raw_value, False

    if isinstance(raw_value, (int, float)):
        return raw_value, True

    normalized = str(raw_value).strip().replace(",", ".")

    try:
        return int(normalized), True
    except Exception:
        pass

    try:
        return float(normalized), True
    except Exception:
        pass

    return raw_value, False


def as_comment_value(value: Any) -> str:
    return "" if value is None else str(value)


def normalize_unit(raw_unit: Any) -> str:
    if raw_unit is None:
        return ""
    return str(raw_unit).strip().replace("\xa0", "").replace("\u00a0", "")

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID


def make_json_safe(value):  # type: ignore[no-untyped-def]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value

    if isinstance(value, UUID):
        return str(value)

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, Decimal):
        try:
            return float(value)
        except Exception:  # noqa: BLE001
            return str(value)

    if isinstance(value, dict):
        out: dict[str, object] = {}
        for k, v in value.items():
            sk = make_json_safe(k)
            if not isinstance(sk, (str, int, float, bool)) and sk is not None:
                sk = str(sk)
            out[str(sk)] = make_json_safe(v)
        return out

    if isinstance(value, (list, tuple, set)):
        return [make_json_safe(v) for v in value]

    # SQLAlchemy model/object fallback: attempt primitive column dump.
    mapper = getattr(value, "__mapper__", None)
    if mapper is not None:
        out: dict[str, object] = {}
        for col in getattr(mapper, "columns", []):
            name = getattr(col, "key", None)
            if not name:
                continue
            out[str(name)] = make_json_safe(getattr(value, name, None))
        return out

    return str(value)


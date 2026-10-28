"""Column types and value casting shared by every engine.

The Python engine uses `cast_value`; the Spark engine uses the matching `try_cast`
in SQL. The accepted boolean spellings are the ones Spark accepts so both engines
reject the same rows.
"""

from __future__ import annotations

import math
from datetime import date, datetime, timezone

import pyarrow as pa

TYPES = ("BIGINT", "VARCHAR", "DOUBLE", "TIMESTAMP", "DATE", "BOOLEAN")

ARROW = {
    "BIGINT": pa.int64(),
    "VARCHAR": pa.string(),
    "DOUBLE": pa.float64(),
    "TIMESTAMP": pa.timestamp("us"),
    "DATE": pa.date32(),
    "BOOLEAN": pa.bool_(),
}

SPARK_SQL = {
    "BIGINT": "BIGINT",
    "VARCHAR": "STRING",
    "DOUBLE": "DOUBLE",
    "TIMESTAMP": "TIMESTAMP",
    "DATE": "DATE",
    "BOOLEAN": "BOOLEAN",
}

SNOWFLAKE = {
    "BIGINT": "NUMBER(38,0)",
    "VARCHAR": "VARCHAR",
    "DOUBLE": "FLOAT",
    "TIMESTAMP": "TIMESTAMP_NTZ",
    "DATE": "DATE",
    "BOOLEAN": "BOOLEAN",
}

_TRUE = {"true", "t", "1", "yes", "y"}
_FALSE = {"false", "f", "0", "no", "n"}


class CastError(ValueError):
    pass


def parse_ts(value: object) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def cast_value(value: object, sql_type: str):
    """Cast `value` to `sql_type`. Empty strings and None become None. Raises CastError."""
    if value is None:
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    try:
        if sql_type == "VARCHAR":
            return value if isinstance(value, str) else str(value)
        if sql_type == "BIGINT":
            if isinstance(value, bool):
                raise ValueError("bool")
            if isinstance(value, float):
                if not value.is_integer():
                    raise ValueError("fractional")
                return int(value)
            return int(str(value).strip())
        if sql_type == "DOUBLE":
            if isinstance(value, bool):
                raise ValueError("bool")
            f = float(value)
            if not math.isfinite(f):
                raise ValueError("non-finite")
            return f
        if sql_type == "TIMESTAMP":
            return parse_ts(value)
        if sql_type == "DATE":
            return date.fromisoformat(str(value).strip()[:10])
        if sql_type == "BOOLEAN":
            if isinstance(value, bool):
                return value
            s = str(value).strip().lower()
            if s in _TRUE:
                return True
            if s in _FALSE:
                return False
            raise ValueError("boolean")
    except (ValueError, TypeError, OverflowError) as exc:
        raise CastError(f"{value!r} is not a valid {sql_type}") from exc
    raise CastError(f"unknown type {sql_type}")

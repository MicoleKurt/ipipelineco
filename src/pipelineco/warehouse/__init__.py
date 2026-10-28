from __future__ import annotations

from ..config import Config
from .base import MergeResult, Warehouse, utcnow


def open_warehouse(cfg: Config) -> Warehouse:
    kind = cfg.warehouse.get("type", "duckdb")
    if kind == "duckdb":
        from .duckdb_adapter import DuckDBWarehouse

        cfg.workdir.mkdir(parents=True, exist_ok=True)
        wh = DuckDBWarehouse(cfg.fmt(cfg.warehouse.get("database", "{workdir}/warehouse.duckdb")))
    elif kind == "snowflake":
        from .snowflake_adapter import SnowflakeWarehouse

        wh = SnowflakeWarehouse(cluster_by=cfg.warehouse.get("cluster_by"))
    else:
        raise ValueError(f"unknown warehouse type {kind!r}")
    wh.init_ops()
    return wh


__all__ = ["MergeResult", "Warehouse", "open_warehouse", "utcnow"]

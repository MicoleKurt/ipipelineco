from __future__ import annotations

from collections.abc import Callable

from ..config import Config
from .base import MergeResult, Warehouse, utcnow

Factory = Callable[[Config], Warehouse]
REGISTRY: dict[str, Factory] = {}


def register_warehouse(name: str, factory: Factory) -> None:
    """Plug in any database: subclass `Warehouse` (about ten methods) and register a factory.

    Then set `warehouse: { type: <name> }` in ipipelineco.yml. See warehouse/base.py.
    """
    REGISTRY[name] = factory


def _duckdb(cfg: Config) -> Warehouse:
    from .duckdb_adapter import DuckDBWarehouse

    cfg.workdir.mkdir(parents=True, exist_ok=True)
    return DuckDBWarehouse(cfg.fmt(cfg.warehouse.get("database", "{workdir}/warehouse.duckdb")))


def _snowflake(cfg: Config) -> Warehouse:
    from .snowflake_adapter import SnowflakeWarehouse

    return SnowflakeWarehouse(cluster_by=cfg.warehouse.get("cluster_by"))


def _postgres(cfg: Config) -> Warehouse:
    from .postgres_adapter import PostgresWarehouse

    return PostgresWarehouse(cfg.warehouse.get("dsn"))


register_warehouse("duckdb", _duckdb)
register_warehouse("snowflake", _snowflake)
register_warehouse("postgres", _postgres)
register_warehouse("supabase", _postgres)  # Supabase is Postgres

DBT_TARGET = {"duckdb": "duckdb", "snowflake": "snowflake", "postgres": "postgres", "supabase": "postgres"}


def open_warehouse(cfg: Config) -> Warehouse:
    kind = cfg.warehouse.get("type", "duckdb")
    try:
        factory = REGISTRY[kind]
    except KeyError as exc:
        raise ValueError(f"unknown warehouse type {kind!r}; known: {', '.join(sorted(REGISTRY))}") from exc
    wh = factory(cfg)
    wh.init_ops()
    return wh


__all__ = ["MergeResult", "Warehouse", "open_warehouse", "register_warehouse", "utcnow", "DBT_TARGET"]

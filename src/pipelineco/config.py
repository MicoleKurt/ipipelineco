"""Config loading. One YAML file describes the warehouse, retry policy and every source."""

from __future__ import annotations

from dataclasses import dataclass, field
from importlib import resources
from pathlib import Path

import yaml

from .types import TYPES


@dataclass
class SourceCfg:
    name: str
    type: str
    options: dict
    key: list[str]
    columns: dict[str, str]
    watermark: str | None = None
    on_new_column: str = "add"

    @property
    def target(self) -> str:
        return f"raw.{self.name}"


@dataclass
class RetryCfg:
    attempts: int = 3
    delay_s: float = 0.2
    backoff: float = 2.0


@dataclass
class Config:
    workdir: Path
    warehouse: dict
    engine: str
    retry: RetryCfg
    sla_s: float
    dq: dict
    sources: list[SourceCfg]
    vars: dict = field(default_factory=dict)

    def fmt(self, value):
        """Fill {workdir} / {api_url} style placeholders in a string option."""
        if not isinstance(value, str):
            return value
        values = {"workdir": str(self.workdir).replace("\\", "/"), **self.vars}
        try:
            return value.format(**values)
        except KeyError as exc:
            raise ValueError(f"unresolved placeholder {exc} in {value!r}") from exc

    def opt(self, source: SourceCfg, name: str, default=None):
        return self.fmt(source.options.get(name, default))

    def source(self, name: str) -> SourceCfg:
        for s in self.sources:
            if s.name == name:
                return s
        raise KeyError(name)


def default_config_text() -> str:
    return resources.files("pipelineco").joinpath("default.yml").read_text(encoding="utf-8")


def load_config(path: str | Path | None, workdir: str | Path, engine: str | None = None, **vars) -> Config:
    text = Path(path).read_text(encoding="utf-8") if path else default_config_text()
    raw = yaml.safe_load(text)
    sources = []
    for s in raw["sources"]:
        for col, typ in s["columns"].items():
            if typ not in TYPES:
                raise ValueError(f"source {s['name']}: column {col} has unknown type {typ}")
        missing = [k for k in s["key"] if k not in s["columns"]]
        if missing:
            raise ValueError(f"source {s['name']}: key columns {missing} are not declared in columns")
        sources.append(
            SourceCfg(
                name=s["name"],
                type=s["type"],
                options=s.get("options", {}),
                key=list(s["key"]),
                columns=dict(s["columns"]),
                watermark=s.get("watermark"),
                on_new_column=s.get("on_new_column", "add"),
            )
        )
    r = raw.get("retry", {})
    return Config(
        workdir=Path(workdir),
        warehouse=raw.get("warehouse", {"type": "duckdb", "database": "{workdir}/warehouse.duckdb"}),
        engine=engine or raw.get("engine", "python"),
        retry=RetryCfg(r.get("attempts", 3), r.get("delay_s", 0.2), r.get("backoff", 2.0)),
        sla_s=float(raw.get("sla_s", 120)),
        dq=raw.get("dq", {}),
        sources=sources,
        vars=vars,
    )

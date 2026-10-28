"""Extractors. Each reads one source from a watermark and writes raw JSON lines to a stage file.

Watermark semantics:
  sqlite / api  inclusive timestamp (`>=`). Boundary rows are re-read on purpose; the
                idempotent MERGE makes that harmless and it never loses a tied timestamp.
  jsonl         line offset, like a Kafka offset. Late events (old event time, new offset)
                are still picked up because the cursor is the position, not the clock.
  csv           name of the last file processed; each drop is loaded exactly once.
"""

from __future__ import annotations

import csv
import json
import os
import re
import socket
import sqlite3
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlencode

from .config import Config, SourceCfg
from .errors import PermanentError, TransientError

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass
class Extraction:
    stage_file: Path
    count: int
    new_watermark: str | None
    files: list[str] = field(default_factory=list)
    bytes: int = 0


def _ident(name: str) -> str:
    if not _IDENT.match(name):
        raise PermanentError(f"unsafe identifier in config: {name!r}")
    return name


def _write_jsonl(rows, path: Path) -> int:
    n = 0
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        for r in rows:
            f.write(json.dumps(r, default=str) + "\n")
            n += 1
    return n


def extract_sqlite(cfg: Config, src: SourceCfg, since: str | None, out: Path) -> Extraction:
    table = _ident(cfg.opt(src, "table"))
    wm = _ident(src.watermark) if src.watermark else None
    con = sqlite3.connect(cfg.opt(src, "path"))
    con.row_factory = sqlite3.Row
    try:
        sql = f"SELECT * FROM {table}"
        params: list = []
        if wm and since:
            sql += f" WHERE {wm} >= ?"
            params.append(since)
        if wm:
            sql += f" ORDER BY {wm}"
        rows = [dict(r) for r in con.execute(sql, params)]
    except sqlite3.Error as exc:
        raise TransientError(f"sqlite read failed: {exc}") from exc
    finally:
        con.close()
    n = _write_jsonl(rows, out)
    new_wm = max((r[wm] for r in rows if r.get(wm)), default=since) if wm else None
    return Extraction(out, n, new_wm)


def extract_api(cfg: Config, src: SourceCfg, since: str | None, out: Path) -> Extraction:
    base = cfg.opt(src, "url")
    size = int(src.options.get("page_size", 100))
    key = os.environ.get(src.options.get("api_key_env", "IPIPELINECO_API_KEY"), "demo-key")
    rows: list[dict] = []
    page: int | None = 1
    while page:
        q = {"page": page, "page_size": size}
        if since:
            q["updated_since"] = since
        req = urllib.request.Request(f"{base}?{urlencode(q)}", headers={"X-Api-Key": key})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                body = json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            if exc.code >= 500 or exc.code == 429:
                raise TransientError(f"API returned HTTP {exc.code}") from exc
            raise PermanentError(f"API returned HTTP {exc.code}") from exc
        except (urllib.error.URLError, socket.timeout, ConnectionError) as exc:
            raise TransientError(f"API unreachable: {exc}") from exc
        rows.extend(body["data"])
        page = body.get("next_page")
    n = _write_jsonl(rows, out)
    wm = src.watermark
    new_wm = max((r[wm] for r in rows if r.get(wm)), default=since) if wm else None
    return Extraction(out, n, new_wm)


def extract_jsonl(cfg: Config, src: SourceCfg, since: str | None, out: Path) -> Extraction:
    offset = int(since or 0)
    path = Path(cfg.opt(src, "path"))
    n = total = 0
    with open(path, encoding="utf-8") as f, open(out, "w", encoding="utf-8", newline="\n") as o:
        for i, line in enumerate(f):
            total = i + 1
            if i < offset:
                continue
            line = line.rstrip("\n").rstrip("\r")
            if line.strip():
                o.write(line + "\n")
                n += 1
    return Extraction(out, n, str(total))


def extract_csv(cfg: Config, src: SourceCfg, since: str | None, out: Path) -> Extraction:
    folder = Path(cfg.opt(src, "directory"))
    files = sorted(p for p in folder.glob(src.options.get("pattern", "*.csv")) if not since or p.name > since)

    def rows():
        for p in files:
            with open(p, encoding="utf-8", newline="") as f:
                for row in csv.DictReader(f):
                    row.pop(None, None)
                    yield row

    n = _write_jsonl(rows(), out)
    return Extraction(out, n, files[-1].name if files else since, [p.name for p in files])


EXTRACTORS = {"sqlite": extract_sqlite, "api": extract_api, "jsonl": extract_jsonl, "csv": extract_csv}


def extract(cfg: Config, src: SourceCfg, since: str | None, out: Path) -> Extraction:
    try:
        fn = EXTRACTORS[src.type]
    except KeyError as exc:
        raise PermanentError(f"unknown source type {src.type!r}") from exc
    out.parent.mkdir(parents=True, exist_ok=True)
    result = fn(cfg, src, since, out)
    result.bytes = out.stat().st_size if out.exists() else 0
    return result

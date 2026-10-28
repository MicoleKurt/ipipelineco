"""Column-of-truth lineage from dbt's manifest plus the declared source systems."""

from __future__ import annotations

import json
from pathlib import Path

from .config import Config
from .viz import Node

SYSTEM_LABEL = {"sqlite": "OLTP database", "api": "REST API", "jsonl": "Event stream", "csv": "CSV drops"}


def build_lineage(cfg: Config, manifest_path: str | Path | None) -> tuple[list[Node], list[tuple[str, str]]]:
    nodes: dict[str, Node] = {}
    edges: list[tuple[str, str]] = []
    for s in cfg.sources:
        sid = f"sys.{s.name}"
        nodes[sid] = Node(sid, f"{s.type}: {s.name}", "source", SYSTEM_LABEL.get(s.type, s.type))
        nodes[s.target] = Node(s.target, s.target, "raw", "raw")
        edges.append((sid, s.target))
    if manifest_path and Path(manifest_path).exists():
        manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
        for uid, node in manifest.get("nodes", {}).items():
            if node["resource_type"] != "model":
                continue
            kind = "mart" if "marts" in node["fqn"] else "staging"
            nodes[uid] = Node(uid, node["name"], kind, kind)
        for uid, parents in manifest.get("parent_map", {}).items():
            if uid not in nodes:
                continue
            for p in parents:
                if p.startswith("source."):
                    src = manifest["sources"][p]
                    p = f"raw.{src['name']}"
                if p in nodes:
                    edges.append((p, uid))
    return list(nodes.values()), edges


def mermaid(nodes: list[Node], edges: list[tuple[str, str]]) -> str:
    def nid(x: str) -> str:
        return "n_" + "".join(ch if ch.isalnum() else "_" for ch in x)

    lines = ["flowchart LR"]
    for n in nodes:
        lines.append(f'  {nid(n.id)}["{n.label}"]')
    for a, b in edges:
        lines.append(f"  {nid(a)} --> {nid(b)}")
    return "\n".join(lines)

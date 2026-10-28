"""Layered graph rendering to inline SVG (task DAG and lineage). No JavaScript, no CDN."""

from __future__ import annotations

from dataclasses import dataclass
from html import escape

PALETTE = {
    "success": ("#10281d", "#3fb97d"),
    "failed": ("#2d1517", "#f0636b"),
    "skipped": ("#23252b", "#6d7280"),
    "running": ("#1d2740", "#6ea8ff"),
    "source": ("#2a2236", "#b48cff"),
    "raw": ("#1d2740", "#6ea8ff"),
    "staging": ("#10281d", "#3fb97d"),
    "mart": ("#33260f", "#f2b04b"),
    "default": ("#1b1f27", "#8b93a5"),
}


@dataclass
class Node:
    id: str
    label: str
    kind: str = "default"
    sub: str = ""


def layout(nodes: list[Node], edges: list[tuple[str, str]]) -> dict[str, int]:
    depth = {n.id: 0 for n in nodes}
    for _ in range(len(nodes)):
        changed = False
        for a, b in edges:
            if a in depth and b in depth and depth[b] < depth[a] + 1:
                depth[b] = depth[a] + 1
                changed = True
        if not changed:
            break
    return depth


def render_svg(nodes: list[Node], edges: list[tuple[str, str]], node_w: int = 168, node_h: int = 46, gap_x: int = 56, gap_y: int = 14, text: str = "#e6e8ee") -> str:
    depth = layout(nodes, edges)
    cols: dict[int, list[Node]] = {}
    for n in nodes:
        cols.setdefault(depth[n.id], []).append(n)
    pos: dict[str, tuple[int, int]] = {}
    max_rows = max(len(v) for v in cols.values())
    height = max_rows * (node_h + gap_y) + gap_y
    for d, col in cols.items():
        offset = (height - len(col) * (node_h + gap_y) + gap_y) / 2
        for i, n in enumerate(col):
            pos[n.id] = (gap_x // 2 + d * (node_w + gap_x), int(offset + i * (node_h + gap_y)))
    width = gap_x // 2 * 2 + (max(cols) + 1) * node_w + max(cols) * gap_x
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="100%" role="img" font-family="ui-sans-serif,Segoe UI,Roboto,sans-serif">']
    out.append('<defs><marker id="arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0L8 4L0 8z" fill="#5b6275"/></marker></defs>')
    for a, b in edges:
        if a not in pos or b not in pos:
            continue
        x1, y1 = pos[a][0] + node_w, pos[a][1] + node_h / 2
        x2, y2 = pos[b][0], pos[b][1] + node_h / 2
        mx = (x1 + x2) / 2
        out.append(f'<path d="M{x1:.0f} {y1:.0f} C{mx:.0f} {y1:.0f} {mx:.0f} {y2:.0f} {x2 - 2:.0f} {y2:.0f}" fill="none" stroke="#5b6275" stroke-width="1.4" marker-end="url(#arr)"/>')
    for n in nodes:
        x, y = pos[n.id]
        fill, stroke = PALETTE.get(n.kind, PALETTE["default"])
        out.append(f'<rect x="{x}" y="{y}" width="{node_w}" height="{node_h}" rx="9" fill="{fill}" stroke="{stroke}" stroke-width="1.4"/>')
        label_y = y + (19 if n.sub else 28)
        out.append(f'<text x="{x + 12}" y="{label_y}" fill="{text}" font-size="12.5" font-weight="600">{escape(n.label[:24])}</text>')
        if n.sub:
            out.append(f'<text x="{x + 12}" y="{y + 36}" fill="#9aa2b5" font-size="11">{escape(n.sub[:28])}</text>')
    out.append("</svg>")
    return "".join(out)

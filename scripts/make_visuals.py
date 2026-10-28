"""Regenerates docs/img/hero.svg and docs/img/architecture.svg.

Geometry is computed, not eyeballed: every edge starts and ends on a node anchor, so
nothing floats. Run:  python scripts/make_visuals.py
"""

from __future__ import annotations

from html import escape
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "docs" / "img"
FONT = "Inter,ui-sans-serif,Segoe UI,Roboto,Helvetica,Arial,sans-serif"
MONO = "ui-monospace,SFMono-Regular,Consolas,monospace"

BG, PANEL, LINE, TEXT, MUTE, DIM = "#0b0d12", "#12151c", "#272c3a", "#eceef4", "#9097aa", "#646b7e"
VIOLET, BLUE, GREEN, AMBER, ROSE = "#a78bfa", "#6ea8ff", "#4ade80", "#fbbf24", "#fb7185"
EDGE = "#505872"


class Node:
    def __init__(self, x, y, w, h, title, sub="", dot=BLUE):
        self.x, self.y, self.w, self.h, self.title, self.sub, self.dot = x, y, w, h, title, sub, dot

    @property
    def left(self):
        return (self.x, self.y + self.h / 2)

    @property
    def right(self):
        return (self.x + self.w, self.y + self.h / 2)

    @property
    def top(self):
        return (self.x + self.w / 2, self.y)

    @property
    def bottom(self):
        return (self.x + self.w / 2, self.y + self.h)

    def svg(self) -> str:
        ty = self.y + (self.h / 2 + 4.5 if not self.sub else self.h / 2 - 2)
        parts = [
            f'<rect x="{self.x}" y="{self.y}" width="{self.w}" height="{self.h}" rx="10" fill="{PANEL}" stroke="{LINE}"/>',
            f'<circle cx="{self.x + 15}" cy="{self.y + self.h / 2}" r="3.5" fill="{self.dot}"/>',
            f'<text x="{self.x + 28}" y="{ty}" font-size="13" font-weight="600" fill="{TEXT}">{escape(self.title)}</text>',
        ]
        if self.sub:
            parts.append(f'<text x="{self.x + 28}" y="{ty + 16}" font-size="11" fill="{MUTE}">{escape(self.sub)}</text>')
        return "".join(parts)


def edge(a, b, dashed=False, curve=True) -> str:
    (x1, y1), (x2, y2) = a, b
    dash = ' stroke-dasharray="4 4"' if dashed else ""
    if abs(y1 - y2) < 0.5 or abs(x1 - x2) < 0.5 or not curve:
        d = f"M{x1:.1f} {y1:.1f} L{x2:.1f} {y2:.1f}"
    else:
        mx = (x1 + x2) / 2
        d = f"M{x1:.1f} {y1:.1f} C{mx:.1f} {y1:.1f} {mx:.1f} {y2:.1f} {x2:.1f} {y2:.1f}"
    return f'<path d="{d}" fill="none" stroke="{EDGE}" stroke-width="1.4"{dash} marker-end="url(#a)"/>'


DEFS = f"""<defs>
<marker id="a" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1.5L8.5 5L1 8.5" fill="none" stroke="{EDGE}" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/></marker>
<linearGradient id="wm" x1="0" x2="1"><stop offset="0" stop-color="#c4b5fd"/><stop offset="1" stop-color="#7cb2ff"/></linearGradient>
<radialGradient id="gl" cx="0.9" cy="0" r="0.8"><stop offset="0" stop-color="#7c6cff" stop-opacity="0.16"/><stop offset="1" stop-color="#7c6cff" stop-opacity="0"/></radialGradient>
</defs>"""


def frame(w, h, body, label) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" role="img" aria-label="{escape(label)}" font-family="{FONT}">'
        f"{DEFS}<rect width='{w}' height='{h}' rx='16' fill='{BG}'/><rect width='{w}' height='{h}' rx='16' fill='url(#gl)'/>"
        f'<rect x="0.5" y="0.5" width="{w - 1}" height="{h - 1}" rx="16" fill="none" stroke="{LINE}"/>{body}</svg>'
    )


def hero() -> str:
    W, H = 1000, 340
    src = [Node(560, 78, 104, 40, "API", dot=VIOLET), Node(560, 150, 104, 40, "Database", dot=VIOLET), Node(560, 222, 104, 40, "Files", dot=VIOLET)]
    ing = Node(708, 142, 112, 56, "Ingest", "retry, MERGE", BLUE)
    dwh = Node(868, 142, 108, 56, "Warehouse", "DuckDB, Postgres", GREEN)
    body = []
    for s in src:
        body.append(edge(s.right, (ing.x, ing.left[1])))
    body.append(edge(ing.right, dwh.left))
    body += [n.svg() for n in src + [ing, dwh]]
    labels = [(560, "SOURCES"), (708, "PIPELINE"), (868, "DESTINATION")]
    for x, t in labels:
        body.append(f'<text x="{x}" y="296" font-size="10" font-weight="600" letter-spacing="1.6" fill="{DIM}">{t}</text>')
    left = f"""
<text x="56" y="112" font-size="60" font-weight="800" letter-spacing="-2.2" fill="url(#wm)">ipipelineco</text>
<text x="58" y="158" font-size="22" font-weight="600" fill="{TEXT}">Pipelines that survive a bad Tuesday.</text>
<text x="58" y="190" font-size="14" fill="{MUTE}">Ingest to warehouse to dbt to one report.</text>
<text x="58" y="210" font-size="14" fill="{MUTE}">Idempotent, retried, observable.</text>
<line x1="58" y1="248" x2="470" y2="248" stroke="{LINE}"/>
<text x="58" y="276" font-size="12" fill="{DIM}" font-family="{MONO}">Python · PySpark · Snowflake · Postgres/Supabase · dbt · Airflow</text>
<text x="58" y="300" font-size="12" fill="{DIM}" font-family="{MONO}">$ ipipelineco demo   <tspan fill="{GREEN}">free, offline, 20 seconds</tspan></text>
"""
    return frame(W, H, left + "".join(body), "ipipelineco: pipelines that survive a bad Tuesday. Sources flow through ingest into the warehouse.")


def architecture() -> str:
    W, H = 1000, 420
    cy = 150
    mh = 48
    src_y = [40, 100, 160, 220]
    srcs = [Node(30, y, 124, 44, t, s, VIOLET) for y, (t, s) in zip(src_y, [("OLTP database", "watermark"), ("REST API", "paginated"), ("Event stream", "offset cursor"), ("File drops", "once each")])]
    extract = Node(196, cy - mh / 2, 112, mh, "Extract", "from watermark", BLUE)
    validate = Node(346, cy - mh / 2, 142, mh, "Validate + dedupe", "Python | PySpark", BLUE)
    merge = Node(526, cy - mh / 2, 116, mh, "MERGE", "one transaction", BLUE)
    wh = Node(680, cy - mh / 2, 124, mh, "Warehouse", "raw.*", GREEN)
    dbt = Node(842, cy - mh / 2, 128, mh, "dbt build", "staging, marts", GREEN)
    row2 = 258
    dlq = Node(validate.x, row2, validate.w, 44, "Dead letters", "reason + payload", ROSE)
    wm = Node(merge.x, row2, merge.w, 44, "Watermark", "same commit", AMBER)
    dq = Node(wh.x, row2, wh.w, 44, "Quality gates", "reconcile, fresh", AMBER)
    rep = Node(dbt.x, row2, dbt.w, 44, "Run report", "HTML, lineage", VIOLET)
    orch = (30, 346, 940, 48)

    body = []
    for s in srcs:
        body.append(edge(s.right, extract.left))
    for a, b in [(extract, validate), (validate, merge), (merge, wh), (wh, dbt)]:
        body.append(edge(a.right, b.left))
    for a, b in [(validate, dlq), (merge, wm), (wh, dq), (dbt, rep)]:
        body.append(edge(a.bottom, b.top, dashed=True))
    body += [n.svg() for n in srcs + [extract, validate, merge, wh, dbt, dlq, wm, dq, rep]]
    x, y, w, h = orch
    body.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12" fill="{PANEL}" stroke="{LINE}"/>')
    body.append(f'<circle cx="{x + 18}" cy="{y + h / 2}" r="3.5" fill="{BLUE}"/>')
    body.append(f'<text x="{x + 32}" y="{y + 21}" font-size="13" font-weight="600" fill="{TEXT}">Orchestrator</text>')
    body.append(f'<text x="{x + 32}" y="{y + 37}" font-size="11" fill="{MUTE}">retries with exponential backoff · task and pipeline SLAs · alerts to console, table and webhook · skip downstream on upstream failure</text>')
    # the orchestrator governs the ingest stages
    body.append(f'<text x="30" y="30" font-size="10" font-weight="600" letter-spacing="1.6" fill="{DIM}">SOURCES</text>')
    body.append(f'<text x="196" y="{cy - 44}" font-size="10" font-weight="600" letter-spacing="1.6" fill="{DIM}">INGEST, PER SOURCE</text>')
    body.append(f'<text x="680" y="{cy - 44}" font-size="10" font-weight="600" letter-spacing="1.6" fill="{DIM}">WAREHOUSE</text>')
    body.append(f'<text x="30" y="{row2 + 22}" font-size="11" fill="{DIM}">dashed = side effect, solid = data flow</text>')
    return frame(W, H, "".join(body), "ipipelineco architecture: sources, extract, validate, merge, warehouse, dbt, with dead letters, watermark, quality gates and a run report, governed by an orchestrator.")


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "hero.svg").write_text(hero(), encoding="utf-8")
    (OUT / "architecture.svg").write_text(architecture(), encoding="utf-8")
    print("wrote hero.svg, architecture.svg")

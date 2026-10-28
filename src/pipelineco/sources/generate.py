"""Deterministic demo sources: an OLTP database, a payments API dataset, an event stream
and CSV file drops. `tick=0` is the starting state, `tick=1` is what changed upstream
since: new and updated rows, late and replayed events, and a CSV drop with a new column.
"""

from __future__ import annotations

import csv
import json
import random
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

COUNTRIES = ["PH", "US", "SG", "AU", "GB", "CA"]
STATUSES = ["placed", "paid", "shipped", "cancelled"]
METHODS = ["card", "gcash", "bank"]
EVENT_TYPES = ["page_view", "add_to_cart", "checkout", "purchase"]
CATEGORIES = ["audio", "office", "kitchen", "outdoor"]


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def paths(workdir: Path) -> dict[str, Path]:
    base = Path(workdir) / "sources"
    return {
        "base": base,
        "db": base / "oltp.db",
        "payments": base / "payments.json",
        "events": base / "events.jsonl",
        "drops": base / "drops",
    }


def generate(workdir: Path, tick: int = 0, seed: int = 7) -> dict:
    """Create (tick=0) or advance (tick=1) the demo sources. Returns a summary of what was written."""
    p = paths(workdir)
    p["drops"].mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed + tick)
    now = _now()
    summary: dict = {}

    con = sqlite3.connect(p["db"])
    if tick == 0:
        con.executescript(
            """
            DROP TABLE IF EXISTS customers; DROP TABLE IF EXISTS orders;
            CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, email TEXT, country TEXT, updated_at TEXT);
            CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, status TEXT, amount REAL,
                                 currency TEXT, created_at TEXT, updated_at TEXT);
            """
        )
        n_cust, n_ord, n_pay, n_evt = 300, 1200, 1500, 2000
        c0, o0, p0 = 0, 0, 0
    else:
        n_cust, n_ord, n_pay, n_evt = 40, 150, 120, 300
        c0 = con.execute("SELECT max(id) FROM customers").fetchone()[0]
        o0 = con.execute("SELECT max(id) FROM orders").fetchone()[0]
        p0 = max(x["payment_id"] for x in json.loads(p["payments"].read_text()))

    # Tick 0 ends two hours ago so every tick-1 row is newer than the stored watermark,
    # the way a real OLTP table's updated_at only moves forward.
    if tick == 0:
        start, span = now - timedelta(days=14), timedelta(days=14) - timedelta(hours=2)
    else:
        start, span = now - timedelta(minutes=50), timedelta(minutes=50)

    def stamp() -> str:
        return _iso(start + timedelta(seconds=rng.randint(0, int(span.total_seconds()))))

    for i in range(1, n_cust + 1):
        cid = c0 + i
        con.execute(
            "INSERT INTO customers VALUES (?,?,?,?,?)",
            (cid, f"Customer {cid}", f"User{cid}@Example.com", rng.choice(COUNTRIES), stamp()),
        )
    for i in range(1, n_ord + 1):
        oid = o0 + i
        ts = stamp()
        con.execute(
            "INSERT INTO orders VALUES (?,?,?,?,?,?,?)",
            (oid, rng.randint(1, c0 + n_cust), rng.choice(STATUSES), round(rng.uniform(8, 480), 2), "USD", ts, ts),
        )
    if tick == 1:
        for cid in rng.sample(range(1, c0 + 1), 25):
            con.execute(
                "UPDATE customers SET email = ?, updated_at = ? WHERE id = ?",
                (f"moved{cid}@example.com", _iso(now - timedelta(minutes=rng.randint(1, 40))), cid),
            )
        for oid in rng.sample(range(1, o0 + 1), 30):
            con.execute(
                "UPDATE orders SET status = 'shipped', updated_at = ? WHERE id = ?",
                (_iso(now - timedelta(minutes=rng.randint(1, 40))), oid),
            )
    con.commit()
    con.close()
    summary["oltp"] = {"customers": n_cust, "orders": n_ord}

    # payments (served by the mock API)
    pays = [] if tick == 0 else json.loads(p["payments"].read_text())
    max_order = c0 and o0 + n_ord or n_ord
    for i in range(1, n_pay + 1):
        pays.append(
            {
                "payment_id": p0 + i,
                "order_id": rng.randint(1, max_order),
                "method": rng.choice(METHODS),
                "amount": round(rng.uniform(8, 480), 2),
                "status": rng.choice(["settled", "settled", "settled", "failed", "refunded"]),
                "updated_at": stamp(),
            }
        )
    if tick == 1:
        for pay in rng.sample(pays[:p0], 20):
            pay["status"] = "refunded"
            pay["updated_at"] = _iso(now - timedelta(minutes=rng.randint(1, 40)))
    p["payments"].write_text(json.dumps(pays), encoding="utf-8")
    summary["payments"] = n_pay

    # event stream (append-only, like a topic)
    lines: list[str] = []
    existing = p["events"].read_text(encoding="utf-8").splitlines() if tick == 1 else []
    e0 = len(existing)
    for i in range(n_evt):
        lines.append(
            json.dumps(
                {
                    "event_id": f"e{tick}-{i:05d}",
                    "user_id": rng.randint(1, c0 + n_cust),
                    "type": rng.choice(EVENT_TYPES),
                    "ts": stamp(),
                }
            )
        )
    if tick == 0:
        lines[100:100] = [lines[50], lines[51], lines[52]] * 8  # at-least-once delivery: duplicates
        for pos in (200, 700, 1500):
            lines.insert(pos, '{"event_id": "broken", "user_id": ')  # truncated writes
        for pos in (300, 900, 1800):
            lines.insert(pos, json.dumps({"event_id": f"bad-{pos}", "user_id": "not-a-number", "type": "page_view", "ts": stamp()}))
    else:
        for i in range(5):  # late: old event time, new offset
            lines.append(
                json.dumps({"event_id": f"late-{i}", "user_id": rng.randint(1, 50), "type": "purchase", "ts": _iso(now - timedelta(days=3, hours=i))})
            )
        lines.extend(existing[10:13])  # replayed from the broker
    with open(p["events"], "a" if tick == 1 else "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines) + "\n")
    summary["events"] = len(lines)
    summary["events_offset_before"] = e0

    # CSV drops
    day = (now - timedelta(days=1 if tick == 0 else 0)).strftime("%Y-%m-%d")
    drop = p["drops"] / f"products_{day}.csv"
    fields = ["sku", "name", "category", "price", "stock"] + (["supplier"] if tick == 1 else [])
    n_prod = 120 if tick == 0 else 40
    bad = 6 if tick == 0 else 3
    with open(drop, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for i in range(n_prod):
            row = {
                "sku": f"SKU-{tick}{i:04d}",
                "name": f"Product {tick}{i}",
                "category": rng.choice(CATEGORIES),
                "price": f"{rng.uniform(3, 220):.2f}",
                "stock": rng.randint(0, 900),
            }
            if tick == 1:
                row["supplier"] = rng.choice(["Acme", "Globex", "Initech"])
            if i < bad:
                row["price"] = "N/A" if i % 2 else "12,50 EUR"
            if i == bad:
                row["sku"] = ""
            w.writerow(row)
    summary["products_file"] = drop.name
    return summary

"""Processing engines. Both read a raw JSONL stage file and produce the same contract:

    typed, de-duplicated parquet  +  a list of rejected rows (reason + raw payload)

`python` is the default and needs nothing else. `spark` does the same work with PySpark
(PERMISSIVE JSON read, try_cast, window de-duplication) and is the path to use when a
single batch no longer fits comfortably in one process.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .errors import PermanentError
from .types import ARROW, SPARK_SQL, CastError, cast_value


@dataclass
class EngineResult:
    parquet_glob: str | None
    read: int = 0
    kept: int = 0
    duplicates: int = 0
    rejected: list[dict] = field(default_factory=list)
    watermark: str | None = None


def scan_columns(stage_file: Path) -> set[str]:
    """Union of keys seen across the stage file (used for schema-drift detection)."""
    seen: set[str] = set()
    with open(stage_file, encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                seen.update(obj)
    return seen


def process_python(stage_file: Path, columns: dict[str, str], key: list[str], order_by: str | None, out_dir: Path) -> EngineResult:
    out_dir.mkdir(parents=True, exist_ok=True)
    res = EngineResult(parquet_glob=None)
    best: dict[tuple, tuple[int, dict, tuple]] = {}
    valid = 0
    with open(stage_file, encoding="utf-8") as f:
        for seq, line in enumerate(f):
            if not line.strip():
                continue
            res.read += 1
            try:
                obj = json.loads(line)
                if not isinstance(obj, dict):
                    raise ValueError
            except (json.JSONDecodeError, ValueError):
                res.rejected.append({"reason": "malformed_json", "payload": line.rstrip("\n")})
                continue
            row: dict = {}
            reason = None
            for col, typ in columns.items():
                try:
                    row[col] = cast_value(obj.get(col), typ)
                except CastError:
                    reason = f"cast_error:{col}:{typ}"
                    break
            if reason is None:
                for k in key:
                    if row[k] is None:
                        reason = f"null_key:{k}"
                        break
            if reason:
                res.rejected.append({"reason": reason, "payload": line.rstrip("\n")})
                continue
            valid += 1
            ident = tuple(row[k] for k in key)
            rank = (True, row[order_by]) if order_by and row[order_by] is not None else (False, None)
            prev = best.get(ident)
            if prev is None or _better(rank, seq, prev[2], prev[0]):
                best[ident] = (seq, row, rank)
    rows = [v[1] for v in best.values()]
    res.kept = len(rows)
    res.duplicates = valid - len(rows)
    if rows:
        table = pa.table({c: pa.array([r[c] for r in rows], type=ARROW[t]) for c, t in columns.items()})
        pq.write_table(table, out_dir / "part-0.parquet")
        res.parquet_glob = str(out_dir / "*.parquet").replace("\\", "/")
        if order_by:
            vals = [r[order_by] for r in rows if r[order_by] is not None]
            res.watermark = max(vals).isoformat(timespec="seconds") if vals else None
    return res


def _better(new_rank, new_seq, old_rank, old_seq) -> bool:
    if new_rank[0] != old_rank[0]:
        return new_rank[0]
    if new_rank[0] and new_rank[1] != old_rank[1]:
        return new_rank[1] > old_rank[1]
    return new_seq > old_seq


def spark_available() -> bool:
    if shutil.which("java") is None:
        return False
    try:
        import pyspark  # noqa: F401
    except ImportError:
        return False
    return True


_SPARK = None


def get_spark():
    global _SPARK
    if _SPARK is None:
        from pyspark.sql import SparkSession

        _SPARK = (
            SparkSession.builder.master("local[2]")
            .appName("ipipelineco")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
            .config("spark.sql.ansi.enabled", "false")
            .getOrCreate()
        )
        _SPARK.sparkContext.setLogLevel("ERROR")
    return _SPARK


def process_spark(stage_file: Path, columns: dict[str, str], key: list[str], order_by: str | None, out_dir: Path) -> EngineResult:
    if not spark_available():
        raise PermanentError("engine 'spark' needs Java on PATH and `pip install pyspark`")
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    spark = get_spark()
    schema = StructType([StructField(c, StringType()) for c in columns] + [StructField("_corrupt", StringType())])
    df = (
        spark.read.schema(schema)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt")
        .json(str(stage_file).replace("\\", "/"))
        .withColumn("__seq", F.monotonically_increasing_id())
        .cache()
    )
    res = EngineResult(parquet_glob=None)
    res.read = df.count()

    typed = df
    for c, t in columns.items():
        typed = typed.withColumn(f"__t_{c}", F.expr(f"try_cast(nullif(trim(`{c}`), '') AS {SPARK_SQL[t]})"))
    reason = F.when(F.col("_corrupt").isNotNull(), F.lit("malformed_json"))
    for c, t in columns.items():
        has_raw = F.expr(f"nullif(trim(`{c}`), '') IS NOT NULL")
        reason = reason.when(has_raw & F.col(f"__t_{c}").isNull(), F.lit(f"cast_error:{c}:{t}"))
    for k in key:
        reason = reason.when(F.col(f"__t_{k}").isNull(), F.lit(f"null_key:{k}"))
    typed = typed.withColumn("__reason", reason).cache()

    bad = typed.filter(F.col("__reason").isNotNull()).select("__reason", *[F.col(c) for c in columns], "_corrupt").collect()
    for r in bad:
        payload = r["_corrupt"] if r["_corrupt"] is not None else json.dumps({c: r[c] for c in columns})
        res.rejected.append({"reason": r["__reason"], "payload": payload})

    good = typed.filter(F.col("__reason").isNull())
    valid = good.count()
    order_cols = []
    if order_by:
        order_cols.append(F.col(f"__t_{order_by}").desc_nulls_last())
    order_cols.append(F.col("__seq").desc())
    w = Window.partitionBy(*[F.col(f"__t_{k}") for k in key]).orderBy(*order_cols)
    dedup = good.withColumn("__rn", F.row_number().over(w)).filter("__rn = 1").select(*[F.col(f"__t_{c}").alias(c) for c in columns])
    dedup = dedup.cache()
    res.kept = dedup.count()
    res.duplicates = valid - res.kept
    if res.kept:
        if out_dir.exists():
            shutil.rmtree(out_dir)
        dedup.coalesce(1).write.mode("overwrite").parquet(str(out_dir).replace("\\", "/"))
        res.parquet_glob = str(out_dir / "*.parquet").replace("\\", "/")
        if order_by:
            m = dedup.agg(F.max(order_by)).collect()[0][0]
            res.watermark = m.isoformat(timespec="seconds") if m is not None else None
    df.unpersist()
    typed.unpersist()
    dedup.unpersist()
    return res


ENGINES = {"python": process_python, "spark": process_spark}


def process(engine: str, stage_file: Path, columns, key, order_by, out_dir: Path) -> EngineResult:
    try:
        fn = ENGINES[engine]
    except KeyError as exc:
        raise PermanentError(f"unknown engine {engine!r}") from exc
    return fn(stage_file, columns, key, order_by, out_dir)

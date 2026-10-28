from ipipelineco.warehouse import snowflake_sql as s

COLS = {"id": "BIGINT", "name": "VARCHAR", "updated_at": "TIMESTAMP"}


def test_create_table_maps_types_and_clusters():
    sql = s.create_table_sql("raw.orders", COLS, ["id"], cluster_by=["updated_at"])
    assert '"id" NUMBER(38,0)' in sql and '"updated_at" TIMESTAMP_NTZ' in sql
    assert 'PRIMARY KEY ("id")' in sql and sql.endswith('CLUSTER BY ("updated_at")')
    assert "_LOADED_AT TIMESTAMP_NTZ" in sql


def test_merge_updates_non_keys_and_inserts_all():
    sql = s.merge_sql("raw.orders", "raw.tmp", COLS, ["id"])
    assert sql.startswith('MERGE INTO raw.orders t USING raw.tmp s ON t."id" = s."id"')
    update_part = sql.split("UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
    assert 't."name" = s."name"' in update_part
    assert 't."id" = s."id"' not in update_part  # never rewrite the key
    assert "WHEN NOT MATCHED THEN INSERT" in sql and "s._RUN_ID" in sql


def test_composite_key_and_stage_flow():
    sql = s.merge_sql("raw.x", "raw.t", {"a": "BIGINT", "b": "BIGINT", "c": "VARCHAR"}, ["a", "b"])
    assert 't."a" = s."a" AND t."b" = s."b"' in sql
    assert (
        s.put_sql("C:\\tmp\\p\\*.parquet", "ops.stg", "r1")
        == "PUT 'file://C:/tmp/p/*.parquet' @ops.stg/r1/ AUTO_COMPRESS = FALSE OVERWRITE = TRUE"
    )
    assert "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE" in s.copy_sql("raw.t", "ops.stg", "r1")
    assert s.qident('we"ird') == '"we""ird"'

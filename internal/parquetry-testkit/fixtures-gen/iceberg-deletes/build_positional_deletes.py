"""Build an Iceberg v2 merge-on-read positional-delete table with Spark.

Runs inside apache/sedona:1.6.1 (Spark 3.4.1) via run.sh; the Iceberg runtime
is pulled by --packages. Writes the raw warehouse (absolute paths) under
./work/warehouse; normalize_and_vendor.py rewrites paths and vendors it.

Layout produced for db.events (format-version=2, merge-on-read):
  - one data file, 1000 rows id 0..999, value = id * 1.5, sorted by id, with
    ten 100-row Parquet row groups (positions = id);
  - one DELETE producing a single position-delete file covering row group 3
    entirely (positions 300..399) and spanning the row-group-5/6 boundary
    (positions 595..604), 110 deletions, leaving 890 live rows.

The row-group boundaries let the reader demonstrate delete-aware pruning:
row group 3 is fully deleted (eliminated), several groups have no deletes
(pass-all), and the 595..604 span crosses a boundary.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession

HERE = Path(__file__).resolve().parent
WAREHOUSE = HERE / "work" / "warehouse"

ROW_COUNT = 1000
DELETED_POSITIONS = sorted(set(range(300, 400)) | set(range(595, 605)))
EXPECTED_LIVE = ROW_COUNT - len(DELETED_POSITIONS)


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("iceberg-positional-deletes")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", str(WAREHOUSE))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def create_table(spark: SparkSession) -> None:
    spark.sql(
        """
        CREATE TABLE local.db.events (
            id bigint,
            category string,
            value double
        ) USING iceberg
        TBLPROPERTIES (
            'format-version'='2',
            'write.delete.mode'='merge-on-read',
            'write.update.mode'='merge-on-read',
            'write.merge.mode'='merge-on-read',
            'write.parquet.row-group-size-bytes'='256',
            'write.parquet.page-size-bytes'='128',
            'write.parquet.compression-codec'='uncompressed',
            'write.metadata.compression-codec'='none'
        )
        """
    )


def append_rows(spark: SparkSession) -> None:
    rows = spark.range(0, ROW_COUNT).selectExpr(
        "id",
        "case when id % 3 = 0 then 'a' when id % 3 = 1 then 'b' else 'c' end as category",
        "cast(id as double) * 1.5 as value",
    )
    rows.repartition(1).sortWithinPartitions("id").writeTo("local.db.events").append()


def delete_rows(spark: SparkSession) -> None:
    spark.sql(
        "DELETE FROM local.db.events "
        "WHERE (id >= 300 AND id < 400) OR (id >= 595 AND id < 605)"
    )


def verify(spark: SparkSession) -> None:
    data_files = spark.sql("SELECT file_path FROM local.db.events.data_files").collect()
    if len(data_files) != 1:
        raise SystemExit(f"expected one data file, got {len(data_files)}")

    delete_files = spark.sql(
        "SELECT file_path FROM local.db.events.all_delete_files"
    ).collect()
    if len(delete_files) != 1:
        raise SystemExit(f"expected one delete file, got {len(delete_files)}")

    positions = [
        row["pos"]
        for row in spark.sql(
            "SELECT pos FROM local.db.events.position_deletes ORDER BY pos"
        ).collect()
    ]
    if positions != DELETED_POSITIONS:
        raise SystemExit("deleted positions do not match the intended set")

    live = spark.sql("SELECT count(*) AS c FROM local.db.events").collect()[0]["c"]
    if live != EXPECTED_LIVE:
        raise SystemExit(f"expected {EXPECTED_LIVE} live rows, got {live}")

    print(f"OK: 1 data file, 1 delete file, {len(positions)} deletes, {live} live", flush=True)


def main() -> int:
    if WAREHOUSE.exists():
        shutil.rmtree(WAREHOUSE)
    WAREHOUSE.mkdir(parents=True, exist_ok=True)

    spark = build_spark()
    create_table(spark)
    append_rows(spark)
    delete_rows(spark)
    verify(spark)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

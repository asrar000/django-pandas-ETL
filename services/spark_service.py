"""Spark session factory for the PySpark analytics branch."""
import os

from pyspark.sql import SparkSession

from config.loader import get
from utils.paths import ICEBERG_WAREHOUSE_DIR


def build_spark_session() -> SparkSession:
    """Build a local Spark session configured for the Iceberg-backed Spark branch."""
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

    catalog = get("iceberg.catalog", "local")
    iceberg_package = get(
        "iceberg.package",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1",
    )
    warehouse_uri = ICEBERG_WAREHOUSE_DIR.resolve().as_uri()

    spark = (
        SparkSession.builder
        .appName(get("spark.app_name", "ecommerce_etl_iceberg"))
        .master(get("spark.master", "local[*]"))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config(
            "spark.sql.shuffle.partitions",
            str(get("spark.shuffle_partitions", 8)),
        )
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.jars.packages", iceberg_package)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{catalog}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse_uri)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

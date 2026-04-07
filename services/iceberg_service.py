"""
Iceberg write helpers for the Spark analytics path.
"""
import uuid

from pyspark.sql import DataFrame

from config.loader import get
from utils.logger import get_logger

logger = get_logger(__name__)


def _write_df_to_iceberg(df: DataFrame, table_name: str) -> None:
    """
    Create or replace an Iceberg table from a Spark DataFrame.
    """
    spark = df.sparkSession
    temp_view = f"tmp_{uuid.uuid4().hex}"
    try:
        df.createOrReplaceTempView(temp_view)
        statement = "REPLACE TABLE" if spark.catalog.tableExists(table_name) else "CREATE TABLE"
        spark.sql(
            f"""
            {statement} {table_name}
            USING iceberg
            TBLPROPERTIES ('format-version' = '2')
            AS SELECT * FROM {temp_view}
            """
        )
    finally:
        try:
            spark.catalog.dropTempView(temp_view)
        except Exception:
            pass


def write_analytics_to_iceberg(customer_df: DataFrame, order_df: DataFrame) -> None:
    """
    Write both analytics tables to Iceberg.
    """
    spark = customer_df.sparkSession
    catalog = get("iceberg.catalog", "local")
    database = get("iceberg.database", "analytics")
    customer_table = f"{catalog}.{database}.customer_analytics"
    order_table = f"{catalog}.{database}.order_analytics"

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{database}")

    logger.info(f"Writing Iceberg table → {customer_table}")
    _write_df_to_iceberg(customer_df, customer_table)
    logger.info(f"Writing Iceberg table → {order_table}")
    _write_df_to_iceberg(order_df, order_table)

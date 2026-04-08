"""
Script to view Iceberg tables after ETL pipeline execution.
"""
from config.loader import get
from services.spark_service import build_spark_session


def view_iceberg_tables():
    """
    Query and display sample data from the Iceberg analytics tables.
    """
    spark = build_spark_session()
    catalog = get("iceberg.catalog", "local")
    database = get("iceberg.database", "analytics")
    customer_table = f"{catalog}.{database}.customer_analytics"
    order_table = f"{catalog}.{database}.order_analytics"

    print("Viewing Iceberg Analytics Tables")
    print("=" * 40)

    # Check if tables exist
    if not spark.catalog.tableExists(customer_table):
        print(f"Table {customer_table} does not exist. Run the ETL pipeline first.")
        return
    if not spark.catalog.tableExists(order_table):
        print(f"Table {order_table} does not exist. Run the ETL pipeline first.")
        return

    # Describe tables
    print(f"\nCustomer Analytics Table: {customer_table}")
    spark.sql(f"DESCRIBE {customer_table}").show()

    print(f"\nOrder Analytics Table: {order_table}")
    spark.sql(f"DESCRIBE {order_table}").show()

    # Sample data
    print(f"\nSample Customer Analytics (first 5 rows):")
    spark.sql(f"SELECT * FROM {customer_table} LIMIT 5").show()

    print(f"\nSample Order Analytics (first 5 rows):")
    spark.sql(f"SELECT * FROM {order_table} LIMIT 5").show()

    # Row counts
    customer_count = spark.sql(f"SELECT COUNT(*) as count FROM {customer_table}").collect()[0]["count"]
    order_count = spark.sql(f"SELECT COUNT(*) as count FROM {order_table}").collect()[0]["count"]
    print(f"\nTotal rows - Customer Analytics: {customer_count}, Order Analytics: {order_count}")

    spark.stop()


if __name__ == "__main__":
    view_iceberg_tables()</content>
<parameter name="filePath">/home/w3e21/assingments/django-pandas-ETL/utils/view_iceberg_tables.py
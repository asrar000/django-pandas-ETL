from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("pipeline", "0005_restore_orderanalytics_order_timestamp"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[
                migrations.RunSQL(
                    sql=(
                        "ALTER TABLE customer_analytics "
                        "ADD COLUMN IF NOT EXISTS email_domain varchar(100) "
                        "NOT NULL DEFAULT '';"
                    ),
                    reverse_sql=(
                        "ALTER TABLE customer_analytics "
                        "DROP COLUMN IF EXISTS email_domain CASCADE;"
                    ),
                ),
                migrations.RunSQL(
                    sql=(
                        "ALTER TABLE order_analytics "
                        "ADD COLUMN IF NOT EXISTS order_complexity_score integer "
                        "NOT NULL DEFAULT 0;"
                    ),
                    reverse_sql=(
                        "ALTER TABLE order_analytics "
                        "DROP COLUMN IF EXISTS order_complexity_score CASCADE;"
                    ),
                ),
                migrations.RunSQL(
                    sql=(
                        "ALTER TABLE order_analytics "
                        "ADD COLUMN IF NOT EXISTS dominant_category varchar(100) "
                        "NOT NULL DEFAULT '';"
                    ),
                    reverse_sql=(
                        "ALTER TABLE order_analytics "
                        "DROP COLUMN IF EXISTS dominant_category CASCADE;"
                    ),
                ),
            ],
            state_operations=[
                migrations.AddField(
                    model_name="customeranalytics",
                    name="email_domain",
                    field=models.CharField(default="", max_length=100),
                ),
                migrations.AddField(
                    model_name="orderanalytics",
                    name="dominant_category",
                    field=models.CharField(default="", max_length=100),
                ),
                migrations.AddField(
                    model_name="orderanalytics",
                    name="order_complexity_score",
                    field=models.IntegerField(default=0),
                ),
            ],
        ),
    ]

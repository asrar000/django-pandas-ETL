from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("pipeline", "0004_restore_orderanalytics_shipping_provider"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[
                migrations.RunSQL(
                    sql=(
                        "ALTER TABLE order_analytics "
                        "ADD COLUMN IF NOT EXISTS order_timestamp "
                        "timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP;"
                    ),
                    reverse_sql=(
                        "ALTER TABLE order_analytics "
                        "DROP COLUMN IF EXISTS order_timestamp CASCADE;"
                    ),
                ),
            ],
            state_operations=[
                migrations.AddField(
                    model_name="orderanalytics",
                    name="order_timestamp",
                    field=models.DateTimeField(),
                ),
            ],
        ),
    ]

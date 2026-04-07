from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("pipeline", "0003_remove_customeranalytics_email_domain_and_more"),
    ]

    operations = [
        migrations.RunSQL(
            sql=(
                "ALTER TABLE order_analytics "
                "ADD COLUMN IF NOT EXISTS shipping_provider varchar(100) "
                "NOT NULL DEFAULT '';"
            ),
            reverse_sql=(
                "ALTER TABLE order_analytics "
                "DROP COLUMN IF EXISTS shipping_provider CASCADE;"
            ),
        ),
    ]

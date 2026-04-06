# This migration is auto-generated. Re-generate with:
#   python manage.py makemigrations
from django.db import migrations, models

class Migration(migrations.Migration):
    initial = True
    dependencies = []

    operations = [
        migrations.CreateModel(
            name="CustomerAnalytics",
            fields=[
                ("id",                   models.BigAutoField(auto_created=True, primary_key=True, serialize=False)),
                ("customer_id",          models.IntegerField(unique=True, db_index=True)),
                ("full_name",            models.CharField(max_length=255)),
                ("email",                models.EmailField(max_length=255)),
                ("email_domain",         models.CharField(max_length=100)),
                ("city",                 models.CharField(max_length=100)),
                ("customer_tenure_days", models.IntegerField(default=0)),
                ("total_orders",         models.IntegerField(default=0)),
                ("total_spent",          models.DecimalField(decimal_places=2, default=0, max_digits=14)),
                ("avg_order_value",      models.DecimalField(decimal_places=2, default=0, max_digits=14)),
                ("lifetime_value_score", models.DecimalField(decimal_places=2, default=0, max_digits=8)),
                ("customer_segment",     models.CharField(max_length=20)),
                ("created_at",           models.DateTimeField(auto_now_add=True)),
                ("updated_at",           models.DateTimeField(auto_now=True)),
            ],
            options={"db_table": "customer_analytics", "ordering": ["-lifetime_value_score"]},
        ),
        migrations.CreateModel(
            name="OrderAnalytics",
            fields=[
                ("id",                    models.BigAutoField(auto_created=True, primary_key=True, serialize=False)),
                ("order_id",              models.CharField(max_length=100, unique=True, db_index=True)),
                ("customer_id",           models.IntegerField(db_index=True)),
                ("order_date",            models.DateField()),
                ("order_hour",            models.IntegerField()),
                ("total_items",           models.IntegerField(default=0)),
                ("gross_amount",          models.DecimalField(decimal_places=2, default=0, max_digits=14)),
                ("total_discount_amount", models.DecimalField(decimal_places=2, default=0, max_digits=14)),
                ("net_amount",            models.DecimalField(decimal_places=2, default=0, max_digits=14)),
                ("shipping_cost",         models.DecimalField(decimal_places=2, default=0, max_digits=10)),
                ("final_amount",          models.DecimalField(decimal_places=2, default=0, max_digits=14)),
                ("discount_ratio",        models.DecimalField(decimal_places=4, default=0, max_digits=6)),
                ("order_complexity_score",models.IntegerField(default=0)),
                ("dominant_category",     models.CharField(max_length=100)),
                ("payment_method",        models.CharField(max_length=50)),
                ("shipping_provider",     models.CharField(max_length=100)),
                ("currency",              models.CharField(default="BDT", max_length=10)),
                ("created_at",            models.DateTimeField(auto_now_add=True)),
                ("updated_at",            models.DateTimeField(auto_now=True)),
            ],
            options={"db_table": "order_analytics", "ordering": ["-order_date", "order_hour"]},
        ),
    ]

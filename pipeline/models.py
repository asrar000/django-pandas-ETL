"""
Django ORM models for the two analytics tables.
Table names are explicit so they match the ETL output.
"""
from django.db import models
from config.loader import get

_CURRENCY_SYMBOL = get("processing.currency_symbol", "$")


class CustomerAnalytics(models.Model):
    customer_id          = models.IntegerField(unique=True, db_index=True)
    full_name            = models.CharField(max_length=255)
    email                = models.EmailField(max_length=255)
    email_domain         = models.CharField(max_length=100, default="")
    city                 = models.CharField(max_length=100)
    customer_tenure_days = models.IntegerField(default=0)
    total_orders         = models.IntegerField(default=0)
    total_spent          = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    avg_order_value      = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    lifetime_value_score = models.DecimalField(max_digits=8,  decimal_places=2, default=0)
    customer_segment     = models.CharField(max_length=20)
    created_at           = models.DateTimeField(auto_now_add=True)
    updated_at           = models.DateTimeField(auto_now=True)

    class Meta:
        db_table         = "customer_analytics"
        ordering         = ["-lifetime_value_score"]
        verbose_name     = "Customer Analytics"
        verbose_name_plural = "Customer Analytics"

    def __str__(self) -> str:
        """Return a concise label for display in the Django admin."""
        return f"{self.full_name} | {self.customer_segment} | {_CURRENCY_SYMBOL}{self.total_spent}"


class OrderAnalytics(models.Model):
    order_id               = models.CharField(max_length=100, unique=True, db_index=True)
    customer_id            = models.IntegerField(db_index=True)
    order_timestamp        = models.DateTimeField()
    order_date             = models.DateField()
    order_hour             = models.IntegerField()
    gross_amount           = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    total_discount_amount  = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    net_amount             = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    shipping_cost          = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    final_amount           = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    total_items            = models.IntegerField(default=0)
    discount_ratio         = models.DecimalField(max_digits=6,  decimal_places=4, default=0)
    order_complexity_score = models.IntegerField(default=0)
    dominant_category      = models.CharField(max_length=100, default="")
    payment_method         = models.CharField(max_length=50)
    shipping_provider      = models.CharField(max_length=100)
    created_at             = models.DateTimeField(auto_now_add=True)
    updated_at             = models.DateTimeField(auto_now=True)

    class Meta:
        db_table         = "order_analytics"
        ordering         = ["-order_date", "order_hour"]
        verbose_name     = "Order Analytics"
        verbose_name_plural = "Order Analytics"

    def __str__(self) -> str:
        """Return a concise label for display in the Django admin."""
        return f"Order {self.order_id[:8]}… | {_CURRENCY_SYMBOL}{self.final_amount} | {self.payment_method}"

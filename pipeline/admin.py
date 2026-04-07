from django.contrib import admin
from .models import CustomerAnalytics, OrderAnalytics


@admin.register(CustomerAnalytics)
class CustomerAnalyticsAdmin(admin.ModelAdmin):
    list_display  = ("customer_id", "full_name", "city", "total_orders",
                     "total_spent", "customer_segment", "updated_at")
    list_filter   = ("customer_segment", "city")
    search_fields = ("full_name", "email", "city")
    ordering      = ("-lifetime_value_score",)


@admin.register(OrderAnalytics)
class OrderAnalyticsAdmin(admin.ModelAdmin):
    list_display  = ("order_id", "customer_id", "order_timestamp", "order_date", "final_amount",
                     "payment_method", "shipping_provider")
    list_filter   = ("payment_method", "shipping_provider")
    search_fields = ("order_id",)
    ordering      = ("-order_timestamp",)

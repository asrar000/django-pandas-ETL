"""OpenSearch index mappings for analytics datasets."""

_TEXT_WITH_KEYWORD = {
    "type": "text",
    "fields": {
        "keyword": {
            "type": "keyword",
            "ignore_above": 256,
        }
    },
}

CUSTOMER_INDEX_MAPPING = {
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "customer_id": {"type": "integer"},
            "full_name": _TEXT_WITH_KEYWORD,
            "email": {"type": "keyword"},
            "email_domain": {"type": "keyword"},
            "city": {"type": "keyword"},
            "customer_tenure_days": {"type": "integer"},
            "total_orders": {"type": "integer"},
            "total_spent": {"type": "double"},
            "avg_order_value": {"type": "double"},
            "lifetime_value_score": {"type": "double"},
            "customer_segment": {"type": "keyword"},
        },
    },
}

ORDER_INDEX_MAPPING = {
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "order_id": {"type": "keyword"},
            "customer_id": {"type": "integer"},
            "order_timestamp": {
                "type": "date",
                "format": "strict_date_optional_time||epoch_millis",
            },
            "order_date": {
                "type": "date",
                "format": "strict_date_optional_time||epoch_millis",
            },
            "order_hour": {"type": "integer"},
            "gross_amount": {"type": "double"},
            "total_discount_amount": {"type": "double"},
            "net_amount": {"type": "double"},
            "shipping_cost": {"type": "double"},
            "final_amount": {"type": "double"},
            "total_items": {"type": "integer"},
            "discount_ratio": {"type": "double"},
            "order_complexity_score": {"type": "integer"},
            "dominant_category": {"type": "keyword"},
            "payment_method": {"type": "keyword"},
            "shipping_provider": {"type": "keyword"},
        },
    },
}

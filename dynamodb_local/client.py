"""Local DynamoDB resource builder."""

from __future__ import annotations

import boto3

from config.loader import get


def build_dynamodb_resource():
    """Build a boto3 DynamoDB resource pointed at DynamoDB Local."""
    return boto3.resource(
        "dynamodb",
        endpoint_url=get("dynamodb.endpoint_url", "http://localhost:8000"),
        region_name=get("dynamodb.region_name", "us-east-1"),
        aws_access_key_id=get("dynamodb.access_key_id", "dummy"),
        aws_secret_access_key=get("dynamodb.secret_access_key", "dummy"),
    )
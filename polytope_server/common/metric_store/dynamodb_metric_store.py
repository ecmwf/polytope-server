#
# Copyright 2024 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
#

import logging
import operator
import warnings
from decimal import Decimal
from enum import Enum
from functools import reduce

import boto3
import botocore
import botocore.exceptions
from boto3.dynamodb.conditions import Attr, Key

from ..metric import Metric, MetricType, RequestStatusChange
from . import MetricStore

logger = logging.getLogger(__name__)


METRIC_TYPE_CLASS_MAP = {
    MetricType.REQUEST_STATUS_CHANGE: RequestStatusChange,
}


def _iter_items(fn, **params):
    while True:
        response = fn(**params)
        for item in response["Items"]:
            yield item
        if "LastEvaluatedKey" not in response:
            break
        params["ExclusiveStartKey"] = response["LastEvaluatedKey"]


def _make_query(**kwargs):
    return {
        key: value.value if isinstance(value, Enum) else value for key, value in kwargs.items() if value is not None
    }


def _visit(obj, fn):
    if isinstance(obj, dict):
        return {key: _visit(value, fn) for key, value in obj.items()}
    if isinstance(obj, list):
        return [_visit(value, fn) for value in obj]
    return fn(obj)


def _convert_numbers(obj, reverse=False):
    def fn(item):
        if not reverse and isinstance(item, float):
            return Decimal(str(item))
        elif reverse and isinstance(item, Decimal):
            return float(item)
        return item

    return _visit(obj, fn)


def _load(item, exclude_fields=None):
    metric_type = Metric.deserialize_slot("type", item["type"])
    cls = METRIC_TYPE_CLASS_MAP[metric_type]
    if exclude_fields is not None:
        item = {key: value for key, value in item.items() if key not in exclude_fields}
    return cls(from_dict=_convert_numbers(item, reverse=True))


def _dump(metric):
    item = _convert_numbers(metric.serialize())
    if "request_id" in item and item["request_id"] is None:
        del item["request_id"]  # index hash keys are not nullable
    return item


def _create_table(dynamodb, table_name):
    try:
        kwargs = {
            "AttributeDefinitions": [
                {"AttributeName": "uuid", "AttributeType": "S"},
                {"AttributeName": "request_id", "AttributeType": "S"},
                {"AttributeName": "type", "AttributeType": "S"},
                {"AttributeName": "timestamp", "AttributeType": "N"},
            ],
            "TableName": table_name,
            "KeySchema": [{"AttributeName": "uuid", "KeyType": "HASH"}],
            "GlobalSecondaryIndexes": [
                {
                    "IndexName": "request-index",
                    "KeySchema": [{"AttributeName": "request_id", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                },
                # GSI for type-based queries with timestamp sorting
                {
                    "IndexName": "type-timestamp-index",
                    "KeySchema": [
                        {"AttributeName": "type", "KeyType": "HASH"},
                        {"AttributeName": "timestamp", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
            ],
            "BillingMode": "PAY_PER_REQUEST",
        }
        table = dynamodb.create_table(**kwargs)
        table.wait_until_exists()
    except dynamodb.meta.client.exceptions.ResourceInUseException:
        pass


class DynamoDBMetricStore(MetricStore):
    def __init__(self, config=None):
        if config is None:
            config = {}

        endpoint_url = config.get("endpoint_url")
        region = config.get("region")
        table_name = config.get("table_name", "metrics")

        dynamodb = boto3.resource("dynamodb", region_name=region, endpoint_url=endpoint_url)
        client = dynamodb.meta.client
        self.table = dynamodb.Table(table_name)

        try:
            response = client.describe_table(TableName=table_name)
            if response["Table"]["TableStatus"] != "ACTIVE":
                raise RuntimeError(f"DynamoDB table {table_name} is not active.")
        except client.exceptions.ResourceNotFoundException:
            _create_table(dynamodb, table_name)

    def get_type(self):
        return "dynamodb"

    def add_metric(self, metric):
        try:
            self.table.put_item(Item=_dump(metric), ConditionExpression=Attr("uuid").not_exists())
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                raise ValueError("Request already exists in request store") from e
            raise

    def remove_metric(self, uuid):
        try:
            self.table.delete_item(Key={"uuid": str(uuid)}, ConditionExpression=Attr("uuid").exists())
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                raise KeyError("Request does not exist in request store") from e
            raise

    def get_metric(self, uuid):
        response = self.table.get_item(Key={"uuid": str(uuid)})
        if "Item" in response:
            return _load(response["Item"])

    def get_metrics(self, ascending=None, descending=None, limit=None, request_id=None, exclude_fields=None, **kwargs):
        if ascending is not None and descending is not None:
            raise ValueError("Cannot sort by ascending and descending at the same time.")

        if request_id is not None:
            fn = self.table.query
            params = {
                "IndexName": "request-index",
                "KeyConditionExpression": Key("request_id").eq(request_id),
            }
        else:
            fn = self.table.scan
            params = {}

        if limit is not None:
            params["Limit"] = limit

        if query := _make_query(**kwargs):
            params["FilterExpression"] = reduce(operator.__and__, (Attr(key).eq(value) for key, value in query.items()))

        items = (_load(item, exclude_fields) for item in _iter_items(fn, **params))
        if ascending is not None:
            return sorted(items, key=lambda item: getattr(item, ascending))
        if descending is not None:
            return sorted(items, key=lambda item: getattr(item, descending), reverse=True)
        return list(items)

    def update_metric(self, metric):
        self.table.put_item(Item=_dump(metric))

    def wipe(self):
        warnings.warn("wipe is not implemented for DynamoDBMetricStore")

    def remove_old_metrics(self, cutoff):
        cutoff_timestamp = cutoff.timestamp()
        response = self.table.scan(
            FilterExpression=Attr("timestamp").lt(_convert_numbers(cutoff_timestamp)),
            ProjectionExpression="#u",
            ExpressionAttributeNames={"#u": "uuid"},
        )
        items_to_delete = [item["uuid"] for item in response.get("Items", [])]

        if not items_to_delete:
            return 0

        with self.table.batch_writer() as batch:
            for uuid in items_to_delete:
                batch.delete_item(Key={"uuid": str(uuid)})

        return len(items_to_delete)

    def get_usage_metrics_aggregated(self, cutoff_timestamps):
        """
        Fetch and aggregate usage metrics from DynamoDB.

        Unlike MongoDB, DynamoDB cannot perform server-side aggregation,
        so this method minimizes data transfer by:
        1. Using a GSI to filter by type (avoids full table scan)
        2. Projecting only necessary fields (userid, timestamp, status)
        3. Paginating efficiently through results
        4. Performing aggregation in Python

        Args:
            cutoff_timestamps: Dict mapping timeframe names to Unix timestamps

        Returns:
            Dict with total_requests, unique_users, and time_frame_metrics
        """

        # The GSI (type-timestamp-index) allows us to efficiently query
        # only request_status_change metrics without scanning the entire table.
        # ProjectionExpression reduces data transfer by fetching only needed fields.

        try:
            # Items we'll collect from DynamoDB
            # We only need: user_id, timestamp, status
            items = []

            # Use the type-timestamp-index GSI for efficient querying
            # This avoids a full table scan by filtering on partition key (type)
            query_params = {
                "IndexName": "type-timestamp-index",
                # Query only request_status_change type metrics
                "KeyConditionExpression": Key("type").eq("request_status_change"),
                # Additional filter: only processed status
                # Note: FilterExpression is applied AFTER query
                "FilterExpression": Attr("status").eq("processed"),
                # Projection: only fetch fields we need
                "ProjectionExpression": "user_id, #ts, #st",
                # ExpressionAttributeNames required because 'status' and 'timestamp'
                # are reserved keywords in DynamoDB
                "ExpressionAttributeNames": {
                    "#ts": "timestamp",
                    "#st": "status",
                },
            }

            # DynamoDB returns max 1MB per call, so we must paginate.
            # The query automatically handles this via LastEvaluatedKey.

            while True:
                response = self.table.query(**query_params)

                # Add items from this page to our collection
                items.extend(response.get("Items", []))

                # Check if there are more pages
                if "LastEvaluatedKey" not in response:
                    break  # No more results

                # Set the starting point for the next page
                query_params["ExclusiveStartKey"] = response["LastEvaluatedKey"]

            # Since DynamoDB can't do server-side aggregation, we must
            # count and deduplicate in application code.

            # Convert Decimal timestamps back to float for comparison
            # DynamoDB stores numbers as Decimal type
            items_with_float_timestamps = []
            for item in items:
                items_with_float_timestamps.append(
                    {
                        "user_id": item.get("user_id"),
                        "timestamp": float(item["timestamp"]),  # Decimal -> float
                        "status": item.get("status"),
                    }
                )

            # Overall metrics across all time
            total_requests = len(items_with_float_timestamps)
            unique_users = set()  # Set automatically handles deduplication

            # Initialize per-timeframe metrics storage
            time_frame_metrics = {}
            for frame_name in cutoff_timestamps.keys():
                time_frame_metrics[frame_name] = {
                    "requests": 0,
                    "unique_users": set(),
                }

            # Single pass through data to calculate:
            # - Total unique users
            # - Per-timeframe request counts
            # - Per-timeframe unique users

            for item in items_with_float_timestamps:
                user_id = item.get("user_id")
                timestamp = item["timestamp"]

                # Add to overall unique users count
                if user_id:
                    unique_users.add(user_id)

                # Check which timeframes this request falls into
                for frame_name, cutoff in cutoff_timestamps.items():
                    # If this request is newer than the cutoff
                    if timestamp >= cutoff:
                        # Increment request count for this timeframe
                        time_frame_metrics[frame_name]["requests"] += 1

                        # Add user to this timeframe's unique users set
                        if user_id:
                            time_frame_metrics[frame_name]["unique_users"].add(user_id)

            for frame_name in time_frame_metrics.keys():
                # Replace the set with its size (count of unique items)
                time_frame_metrics[frame_name]["unique_users"] = len(time_frame_metrics[frame_name]["unique_users"])

            # Return the aggregated results in the expected format
            return {
                "total_requests": total_requests,
                "unique_users": len(unique_users),
                "time_frame_metrics": time_frame_metrics,
            }

        except Exception as e:
            logger.error(f"Error fetching metrics from DynamoDB: {e}")
            raise

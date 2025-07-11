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

from ..metric import (
    CacheInfo,
    Metric,
    MetricType,
    QueueInfo,
    RequestStatusChange,
    StorageInfo,
    WorkerInfo,
    WorkerStatusChange,
)
from . import MetricStore

logger = logging.getLogger(__name__)


METRIC_TYPE_CLASS_MAP = {
    MetricType.WORKER_STATUS_CHANGE: WorkerStatusChange,
    MetricType.WORKER_INFO: WorkerInfo,
    MetricType.REQUEST_STATUS_CHANGE: RequestStatusChange,
    MetricType.STORAGE_INFO: StorageInfo,
    MetricType.CACHE_INFO: CacheInfo,
    MetricType.QUEUE_INFO: QueueInfo,
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
            ],
            "TableName": table_name,
            "KeySchema": [{"AttributeName": "uuid", "KeyType": "HASH"}],
            "GlobalSecondaryIndexes": [
                {
                    "IndexName": "request-index",
                    "KeySchema": [{"AttributeName": "request_id", "KeyType": "HASH"}],
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

    def collect_metric_info(self):
        return {}

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

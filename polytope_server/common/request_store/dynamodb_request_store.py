#
# Copyright 2022 European Centre for Medium-Range Weather Forecasts (ECMWF)
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

import datetime as dt
import logging
import operator
import warnings
from decimal import Decimal
from functools import reduce

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Attr, Key

from .. import metric_store
from ..exceptions import ForbiddenRequest, NotFound, UnauthorizedRequest
from ..metric import RequestStatusChange
from ..request import Request, Status
from ..user import User
from . import request_store

logger = logging.getLogger(__name__)


def _iter_items(fn, **params):
    while True:
        response = fn(**params)
        for item in response["Items"]:
            yield item
        if "LastEvaluatedKey" not in response:
            break
        params["ExclusiveStartKey"] = response["LastEvaluatedKey"]


def _make_query(**kwargs):
    query = {}
    for key, value in kwargs.items():
        if key not in Request.__slots__:
            raise KeyError("Request has no key {}".format(key))

        if value is None:
            continue

        query[key] = Request.serialize_slot(key, value)

    return query


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


def _load(item):
    return Request(
        from_dict={key: _convert_numbers(value, reverse=True) for key, value in item.items() if key != "user_id"}
    )


def _dump(request):
    item = _convert_numbers(request.serialize())
    if request.user is not None:
        return item | {"user_id": str(request.user.id)}
    return item


def _create_table(dynamodb, table_name):
    try:
        kwargs = {
            "AttributeDefinitions": [
                {"AttributeName": "id", "AttributeType": "S"},
                {"AttributeName": "status", "AttributeType": "S"},
                {"AttributeName": "user_id", "AttributeType": "S"},
            ],
            "TableName": table_name,
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "GlobalSecondaryIndexes": [
                {
                    "IndexName": "status-index",
                    "KeySchema": [{"AttributeName": "status", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                },
                {
                    "IndexName": "user-index",
                    "KeySchema": [{"AttributeName": "user_id", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                },
            ],
            "BillingMode": "PAY_PER_REQUEST",
        }
        table = dynamodb.create_table(**kwargs)
        table.wait_until_exists()
    except dynamodb.meta.client.exceptions.ResourceInUseException:
        pass


class DynamoDBRequestStore(request_store.RequestStore):

    def __init__(self, config=None, metric_store_config=None):
        if config is None:
            config = {}

        endpoint_url = config.get("endpoint_url")
        region = config.get("region")
        table_name = config.get("table_name", "requests")

        dynamodb = boto3.resource("dynamodb", region_name=region, endpoint_url=endpoint_url)
        client = dynamodb.meta.client
        self.table = dynamodb.Table(table_name)

        try:
            response = client.describe_table(TableName=table_name)
            if response["Table"]["TableStatus"] != "ACTIVE":
                raise RuntimeError(f"DynamoDB table {table_name} is not active.")
        except client.exceptions.ResourceNotFoundException:
            _create_table(dynamodb, table_name)

        self.metric_store = None
        if metric_store_config is not None:
            self.metric_store = metric_store.create_metric_store(metric_store_config)

        logger.info("DynamoDB request store configured for table name %s.", table_name)

    def get_type(self):
        return "dynamodb"

    def add_request(self, request):
        try:
            self.table.put_item(Item=_dump(request), ConditionExpression=Attr("id").not_exists())
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                raise ValueError("Request already exists in request store") from e
            raise

        if self.metric_store:
            self.metric_store.add_metric(RequestStatusChange(request_id=request.id, status=request.status))

        logger.info("Request ID %s status set to %s.", request.id, request.status)

    def remove_request(self, id):
        try:
            self.table.delete_item(Key={"id": id}, ConditionExpression=Attr("id").exists())
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                raise KeyError("Request does not exist in request store") from e
            raise

        if self.metric_store:
            items = self.metric_store.get_metrics(request_id=id)
            for item in items:
                self.metric_store.remove_metric(item.uuid)

        logger.info("Request ID %s removed.", id)

    def revoke_request(self, user: User, id: str):
        if id == "all":
            # Query the status index for WAITING and QUEUED requests for this user
            deleted = 0
            items_to_delete = []
            for status in [Status.WAITING.value, Status.QUEUED.value]:
                items = _iter_items(
                    self.table.query,
                    IndexName="status-index",
                    KeyConditionExpression=Key("status").eq(status),
                    FilterExpression=Attr("user_id").eq(str(user.id)),
                )
                for item in items:
                    items_to_delete.append(item["id"])
            # Use batch_writer for efficient deletion
            with self.table.batch_writer() as batch:
                for req_id in items_to_delete:
                    try:
                        # Try to delete using the same logic as _revoke_single_request
                        batch.delete_item(Key={"id": req_id})
                        deleted += 1
                    except Exception as e:
                        logger.error("Failed to revoke request %s: %s", req_id, e)
                        continue
            return deleted
        else:
            # Revoke a single request by ID
            return self._revoke_single_request(user, id)

    def _revoke_single_request(self, user, id):
        # Revoke a single request by ID
        try:
            self.table.delete_item(
                Key={"id": id},
                ConditionExpression=Attr("id").exists()
                & Attr("status").exists()
                & Attr("status").is_in([Status.WAITING.value, Status.QUEUED.value])
                & Attr("user_id").eq(str(user.id)),
            )
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                # Check if the request exists to distinguish error cause
                request = self.get_request(id)
                if request is None:
                    raise NotFound("Request does not exist in request store")
                elif request.user != user:
                    raise UnauthorizedRequest("Only the user who created the request can revoke it")
                elif request.status not in [Status.WAITING, Status.QUEUED]:
                    raise ForbiddenRequest("Request can only be revoked before it starts processing.")
                else:
                    raise
            raise

        logger.info("Request ID %s revoked.", id)
        return 1  # Successfully revoked one request

    def get_request(self, id):
        response = self.table.get_item(Key={"id": id})
        if "Item" in response:
            return _load(response["Item"])
        return None

    def get_requests(self, ascending=None, descending=None, limit=None, status=None, user=None, **kwargs):
        if ascending is not None and descending is not None:
            raise ValueError("Cannot sort by ascending and descending at the same time.")

        query = _make_query(**kwargs)
        if user is not None:
            key_cond_expr = Key("user_id").eq(str(user.id))
            fn = self.table.query
            params = {
                "IndexName": "user-index",
                "KeyConditionExpression": key_cond_expr,
            }
            if status is not None:
                query["status"] = status.value
        elif status is not None:
            key_cond_expr = Key("status").eq(status.value)
            fn = self.table.query
            params = {
                "IndexName": "status-index",
                "KeyConditionExpression": key_cond_expr,
            }
        else:
            fn = self.table.scan
            params = {}

        if query:
            filter_expr = reduce(operator.__and__, (Attr(key).eq(value) for key, value in query.items()))
            params["FilterExpression"] = filter_expr

        if limit is not None:
            params["Limit"] = limit

        reqs = (_load(item) for item in _iter_items(fn, **params))
        if ascending:
            return sorted(reqs, key=lambda req: getattr(req, ascending))
        if descending:
            return sorted(reqs, key=lambda req: getattr(req, descending), reverse=True)
        return list(reqs)

    def update_request(self, request):
        now = dt.datetime.now(dt.timezone.utc)
        request.last_modified = now.timestamp()
        try:
            self.table.put_item(Item=_dump(request), ConditionExpression=Attr("id").eq(request.id))
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                raise NotFound("Request {} not found in request store".format(request.id)) from e
            raise

        if self.metric_store:
            self.metric_store.add_metric(RequestStatusChange(request_id=request.id, status=request.status))

        logger.info("Request ID %s status set to %s.", request.id, request.status)

    def wipe(self):
        warnings.warn("wipe is not implemented for DynamoDBRequestStore")

    def collect_metric_info(self):
        return {}

    def remove_old_requests(self, cutoff: dt.datetime):
        cutoff_timestamp = cutoff.timestamp()

        to_delete = _iter_items(
            self.table.scan,
            FilterExpression=Attr("status").is_in([Status.FAILED.value, Status.PROCESSED.value])
            & Attr("last_modified").lt(_convert_numbers(cutoff_timestamp)),
        )
        items_to_delete = [item["id"] for item in to_delete]

        if not items_to_delete:
            logger.info("No requests older than cutoff found.")
            return 0

        with self.table.batch_writer() as batch:
            for id in items_to_delete:
                batch.delete_item(Key={"id": id})
                logger.info("Deleting request %s because it is older than cutoff.", id)
        return len(items_to_delete)

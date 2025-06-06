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

from . import dynamodb_request_store, mongodb_request_store, request_store

type_to_class_map: dict[str : type[request_store.RequestStore]] = {  # noqa: E203
    "mongodb": mongodb_request_store.MongoRequestStore,
    "dynamodb": dynamodb_request_store.DynamoDBRequestStore,
}


def create_request_store(request_store_config=None, metric_store_config=None) -> type[request_store.RequestStore]:

    if request_store_config is None:
        request_store_config = {"mongodb": {}}

    db_type = next(iter(request_store_config.keys()))

    assert db_type in type_to_class_map.keys()

    return type_to_class_map[db_type](request_store_config.get(db_type), metric_store_config)

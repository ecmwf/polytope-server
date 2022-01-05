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

import importlib
from abc import ABC, abstractmethod
from typing import Dict, Union

from ..metric import MetricType
from ..request import Status


class Message:
    def __init__(self, body, context=None):
        self.body = body
        self.context = context


class Queue(ABC):
    def __init__(self, config):
        """Connect to a queue"""

    @abstractmethod
    def enqueue(self, message: Message) -> None:
        """Enqueue a message"""

    @abstractmethod
    def dequeue(self) -> Message:
        """Get one message from the queue, if possible"""
        """ Returns a Message object or None """

    @abstractmethod
    def ack(self, message: Message) -> None:
        """Ack a message which has been dequeued"""

    @abstractmethod
    def nack(self, message: Message) -> None:
        """Nack a message which has been dequeued"""

    @abstractmethod
    def keep_alive(self) -> bool:
        """Sends a heartbeat to the queue server, returns connection status"""

    @abstractmethod
    def check_connection(self) -> bool:
        """Check the queue connection"""

    @abstractmethod
    def close_connection(self) -> None:
        """Close the queue connection"""

    @abstractmethod
    def count(self) -> int:
        """Count the number of messages in the queue"""

    @abstractmethod
    def get_type(self) -> str:
        """Get the implementation type"""

    @abstractmethod
    def collect_metric_info(
        self,
    ) -> Dict[str, Union[None, int, float, str, Status, MetricType]]:
        """Collect dictionary of metrics"""


queue_dict = {"rabbitmq": "RabbitmqQueue"}


def create_queue(queue_config):

    queue_type = next(iter(queue_config.keys()), "rabbitmq")

    QueueClass = importlib.import_module("polytope_server.common.queue." + queue_type + "_queue")
    return getattr(QueueClass, queue_dict[queue_type])(queue_config[queue_type])

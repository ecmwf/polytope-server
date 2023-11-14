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

import pika

from ..metric import QueueInfo
from . import MetricCollector


class QueueMetricCollector(MetricCollector):
    def __init__(self):
        self.host = "none"

    def collect(self):
        return QueueInfo(queue_host=self.host, total_queued=self.total_queued())

    def total_queued(self):
        return None


class RabbitmqQueueMetricCollector(QueueMetricCollector):
    def __init__(self, host, parameters, queue_name):
        self.host = host
        self.parameters = parameters
        self.queue_name = queue_name

    def total_queued(self):
        connection = pika.BlockingConnection(self.parameters)
        channel = connection.channel()
        q = channel.queue_declare(queue=self.queue_name, durable=True, passive=True)
        return q.method.message_count


class SQSQueueMetricCollector(QueueMetricCollector):
    def __init__(self, host):
        self.host = host
        self.message_counts = None

    def total_queued(self):
        num_messages = 0
        for key in self.message_counts:
            num_messages += self.message_counts[key]
        return num_messages

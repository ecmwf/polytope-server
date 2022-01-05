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

import json
import logging

import pika

from ..metric_collector import RabbitmqQueueMetricCollector
from . import queue


class RabbitmqQueue(queue.Queue):
    def __init__(self, config):

        self.host = config.get("host", "localhost")
        self.port = config.get("port", "5672")
        endpoint = "{}:{}".format(self.host, self.port)
        self.queue_name = config.get("name", "default")
        self.username = config.get("user", "guest")
        self.password = config.get("password", "guest")
        self.keep_alive_interval = config.get("keep_alive_interval", 30)

        self.credentials = pika.PlainCredentials(self.username, self.password)

        logging.getLogger("pika").setLevel("WARNING")

        self.parameters = pika.ConnectionParameters(
            self.host,
            self.port,
            "/",
            credentials=self.credentials,
            heartbeat=self.keep_alive_interval,
            connection_attempts=5,
            retry_delay=5,
        )
        self.connection = pika.BlockingConnection(self.parameters)
        self.channel = self.connection.channel(channel_number=1)  # always reconnect to the same channel number
        self.queue = self.channel.queue_declare(queue=self.queue_name, durable=True)
        self.channel.confirm_delivery()
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_recover(requeue=True)

        self.queue_metric_collector = RabbitmqQueueMetricCollector(endpoint, self.parameters, self.queue_name)

    def enqueue(self, message):
        self.channel.basic_publish(
            exchange="",
            routing_key=self.queue_name,
            body=json.dumps(message.body).encode("utf-8"),
            mandatory=True,
            properties=pika.BasicProperties(delivery_mode=2),
        )

    def dequeue(self):
        method, header, body = self.channel.basic_get(queue=self.queue_name)
        if None not in (method, header, body):
            return queue.Message(json.loads(body.decode("utf-8")), context=method)
        else:
            return None

    def ack(self, message):
        method_frame = message.context
        self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    def nack(self, message):
        method_frame = message.context
        self.channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)

    def keep_alive(self):
        self.connection.process_data_events()  # Sends heartbeat
        return self.check_connection()

    def check_connection(self):
        return self.connection.is_open

    def count(self):
        q = self.channel.queue_declare(queue=self.queue_name, durable=True, passive=True)
        return q.method.message_count

    def close_connection(self):
        self.connection.close()

    def get_type(self):
        return "rabbitmq"

    def collect_metric_info(self):
        return self.queue_metric_collector.collect().serialize()

import json
import logging
from . import queue
import boto3
from ..metric_collector import SQSQueueMetricCollector


class SQSQueue(queue.Queue):
    def __init__(self, config):
        queue_name = config.get("queue_name")
        region = config.get("region")
        self.keep_alive_interval = config.get("keep_alive_interval", 60)
        self.visibility_timeout = config.get("visibility_timeout", 120)

        logging.getLogger("sqs").setLevel("WARNING")

        self.client = boto3.client("sqs", region_name=region)
        self.queue_url = self.client.get_queue_url(QueueName=queue_name).get("QueueUrl")
        self.check_connection()
        self.queue_metric_collector = SQSQueueMetricCollector(self.queue_url)

    def enqueue(self, message):
        self.client.send_message(QueueUrl=self.queue_url, MessageBody=json.dumps(message.body))

    def dequeue(self):
        response = self.client.receive_message(
            QueueUrl=self.queue_url,
            VisibilityTimeout=self.visibility_timeout,  # If processing takes more seconds, message will be read twice
            MaxNumberOfMessages=1,
        )
        if not response["Messages"]:
            return None

        msg, *remainder = response["Messages"]
        for item in remainder:
            self.client.change_message_visibility(
                QueueUrl=self.queue_url, ReceiptHandle=item["ReceiptHandle"], VisibilityTimeout=0
            )
        body = msg["Body"]
        receipt_handle = msg["ReceiptHandle"]

        return queue.Message(json.loads(body), context=receipt_handle)

    def ack(self, message):
        self.client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=message.context)

    def nack(self, message):
        self.client.change_message_visibility(
            QueueUrl=self.queue_url, ReceiptHandle=message.context, VisibilityTimeout=0
        )

    def keep_alive(self):
        # Implemented for compatibility, disabled because each request to SQS is billed
        pass
        # return self.check_connection()

    def check_connection(self):
        response = self.client.get_queue_attributes(QueueUrl=self.queue_url, AttributeNames=["CreatedTimestamp"])
        # Tries to parse response
        return "Attributes" in response and "CreatedTimestamp" in response["Attributes"]

    def close_connection(self):
        self.client.close()

    def count(self):
        response = self.client.get_queue_attributes(
            QueueUrl=self.queue_url, AttributeNames=["ApproximateNumberOfMessages"]
        )
        num_messages = response["Attributes"]["ApproximateNumberOfMessages"]

        return int(num_messages)

    def get_type(self):
        return "sqs"

    def collect_metric_info(self):
        response = self.client.get_queue_attributes(
            QueueUrl=self.queue_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesDelayed",
                "ApproximateNumberOfMessagesNotVisible",
            ],
        )
        self.queue_metric_collector.message_counts = response["Attributes"]
        return self.queue_metric_collector.collect().serialize()

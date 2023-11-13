import json
import logging
from . import queue
import boto3
import os


class SQSQueue(queue.Queue):
    def __init__(self, config):
        host = config.get("host", "localhost")
        queue_name = config.get("name", "default")
        region = config.get("region", "eu-central-2")
        self.keep_alive_interval = config.get("keep_alive_interval", 60)
        self.visibility_timeout = config.get("visibility_timeout", 120)
        self.queue_url = "{}/{}".format(host, queue_name)

        logging.getLogger("sqs").setLevel("WARNING")
        session = boto3.Session(
            aws_access_key_id=os.getenv("POLYTOPE_S3_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("POLYTOPE_S3_SECRET_KEY"),
            region_name=region,
        )
        self.client = session.client("sqs")
        self.check_connection()

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
        if len(response["Messages"]) > 1:
            raise ValueError("Received {} messages, should have received 1".format(len(response["Messages"])))

        body = response["Messages"][0]["Body"]
        receipt_handle = response["Messages"][0]["ReceiptHandle"]

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
        return response["Attributes"]

"""SQS long-poll consumer.

Bypassed when USE_AWS_SQS=false; the local-dev path uses direct API calls.
"""
import asyncio
import json
import logging
import os
from typing import Awaitable, Callable

logger = logging.getLogger(__name__)


class SQSConsumer:
    def __init__(self, queue_url: str, handler: Callable[[dict], Awaitable[dict]]):
        self.queue_url = queue_url
        self.handler = handler
        self.use_aws = os.getenv("USE_AWS_SQS", "false").lower() == "true"
        self.region = os.getenv("AWS_REGION", "us-east-1")
        self._client = None
        self._running = False

    @property
    def client(self):
        if not self._client and self.use_aws:
            import boto3
            self._client = boto3.client("sqs", region_name=self.region)
        return self._client

    async def run(self, poll_seconds: int = 20):
        if not self.use_aws:
            logger.info("sqs_consumer_disabled_use_aws_false")
            return
        self._running = True
        logger.info("sqs_consumer_started", extra={"queue": self.queue_url})
        loop = asyncio.get_event_loop()
        while self._running:
            try:
                resp = await loop.run_in_executor(
                    None,
                    lambda: self.client.receive_message(
                        QueueUrl=self.queue_url,
                        MaxNumberOfMessages=10,
                        WaitTimeSeconds=poll_seconds,
                    ),
                )
                for msg in resp.get("Messages", []):
                    body = json.loads(msg["Body"])
                    try:
                        await self.handler(body)
                        await loop.run_in_executor(
                            None,
                            lambda: self.client.delete_message(
                                QueueUrl=self.queue_url,
                                ReceiptHandle=msg["ReceiptHandle"],
                            ),
                        )
                    except Exception as e:
                        logger.exception(
                            "sqs_handler_failed", extra={"error": str(e)}
                        )
            except Exception as e:
                logger.exception(
                    "sqs_poll_failed", extra={"error": str(e)}
                )
                await asyncio.sleep(2)

    def stop(self):
        self._running = False

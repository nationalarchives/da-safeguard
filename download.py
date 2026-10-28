import json
import sys

from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

from pathlib import Path
import logging
import boto3
import os
import signal
from urllib.parse import unquote_plus, quote
from threading import Lock, Event, Semaphore

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)

stop_requested = Event()

counter_lock = Lock()

sqs = boto3.client("sqs")

s3 = boto3.client("s3")


def get_semaphore(concurrency: int):
    return Semaphore(concurrency)


def request_shutdown(_, __):
    stop_requested.set()


def object_info(record):
    key = record["s3"]["object"]["key"]
    size = record["s3"]["object"]["size"]
    bucket = record["s3"]["bucket"]["name"]
    version_id = record["s3"]["object"].get("versionId")
    return key, bucket, size, version_id


def process_message(message, queue_url, work_dir):
    try:
        receipt_handle = message["receipt_handle"]
        records = message["body"]["Records"]
        for record in records:
            key, bucket, size, version_id = object_info(record)
            decoded_key = "/".join([unquote_plus(x) for x in key.split("/")])
            output_file = Path(work_dir) / Path(bucket) / Path(decoded_key)
            if size > 0 and  not output_file.exists():
                output_file.parent.mkdir(parents=True, exist_ok=True)
                download_args = {
                    "Bucket": bucket,
                    "Key": decoded_key,
                    "Filename": output_file,
                }
                if version_id:
                    download_args["ExtraArgs"] = {"VersionId": version_id}
                s3.download_file(**download_args)
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

    except Exception as e:
        logger.error("Error processing messages", e)


def run():
    concurrency = int(os.environ.get("CONCURRENCY", 50))
    queue_url = os.environ["QUEUE_URL"]
    work_dir = os.environ["WORK_DIR"]
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        pending = set()

        while not stop_requested.is_set():
            pending = {future for future in pending if not future.done()}
            messages = []

            while len(messages) < concurrency:
                response = sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=10,
                )
                response_messages = response.get("Messages", [])
                if not response_messages:
                    continue
                messages.extend(response_messages)

            if not messages:
                stop_requested.wait(timeout=300)
                continue

            for msg in messages:
                message = {
                    "receipt_handle": msg["ReceiptHandle"],
                    "body": json.loads(msg["Body"]),
                }
                pending.add(
                    pool.submit(process_message, message, queue_url, work_dir)
                )
            wait(pending, timeout=60 * 60 * 3)


def main():
    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)
    run()


if __name__ == "__main__":
    main()

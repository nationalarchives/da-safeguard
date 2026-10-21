import datetime
import json
import sys

from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

from pathlib import Path
import logging
import psycopg2
import boto3
import os
import signal
from urllib.parse import unquote_plus, quote
from threading import Lock, Event, Semaphore

DB = dict(
    host=os.environ.get("DBHOST", "pg"),
    port=5432,
    dbname="postgres",
    user="postgres",
    password="password",
)

conn = psycopg2.connect(**DB)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)

stop_requested = Event()

counter_lock = Lock()

processed_message_count = 0

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
    return key, bucket, size


def increment_count():
    global processed_message_count
    with counter_lock:
        processed_message_count += 1
        if processed_message_count % 10 == 0:
            logger.info("Processed %d messages", processed_message_count)


def process_message(message, queue_url, work_dir):
    try:
        receipt_handle = message["receipt_handle"]
        records = message["body"]["Records"]
        for record in records:
            key, bucket, size = object_info(record)
            decoded_key = "/".join([unquote_plus(x) for x in key.split("/")])
            output_file = Path(work_dir) / Path(bucket) / Path(decoded_key)
            if size > 0 and  not output_file.exists():
                output_file.parent.mkdir(parents=True, exist_ok=True)
                s3.download_file(Bucket=bucket, Key=decoded_key, Filename=output_file)
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        mark_files_downloaded(records)
        increment_count()

    except Exception as e:
        logger.error("Error processing messages", e)


def create_db_string(record):
    key, bucket, _ = object_info(record)
    return f"{bucket}/{quote(unquote_plus(key))}"


def mark_files_downloaded(records):
    with conn.cursor() as cur:
        now = datetime.datetime.now()

        updates = [
            (
                now,
                create_db_string(record)
            )
            for record in records
        ]
        sql = "UPDATE safeguard_keys SET downloaded_at = %s WHERE s3_key = %s"
        cur.executemany(sql, updates)
        conn.commit()


def run():
    concurrency = int(os.environ.get("CONCURRENCY", 50))
    queue_url = os.environ["QUEUE_URL"]
    work_dir = os.environ["WORK_DIR"]
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        pending = set()

        while not stop_requested.is_set():
            pending = {future for future in pending if not future.done()}

            if len(pending) >= concurrency:
                wait(pending, timeout=1, return_when=FIRST_COMPLETED)
                continue

            free_slots = concurrency - len(pending)
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=min(10, free_slots),
            )
            messages = response.get("Messages", [])

            if not messages:
                stop_requested.wait(timeout=5)
                continue

            for msg in messages:
                message = {
                    "receipt_handle": msg["ReceiptHandle"],
                    "body": json.loads(msg["Body"]),
                }
                pending.add(
                    pool.submit(process_message, message, queue_url, work_dir)
                )


def main():
    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)
    run()


if __name__ == "__main__":
    main()

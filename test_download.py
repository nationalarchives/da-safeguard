import json
import os
import signal
import tempfile
import unittest
from concurrent.futures import Future
from pathlib import Path
from unittest.mock import MagicMock, call, patch


_sqs = MagicMock()
_s3 = MagicMock()
_database_connection = MagicMock()

with (
    patch("boto3.client", side_effect={"sqs": _sqs, "s3": _s3}.get),
):
    import download


class DownloadTests(unittest.TestCase):
    def setUp(self):
        _sqs.reset_mock()
        _s3.reset_mock()
        _database_connection.reset_mock()
        download.stop_requested.clear()
        download.processed_message_count = 0

    def test_request_shutdown_sets_stop_event(self):
        download.request_shutdown(signal.SIGTERM, None)

        self.assertTrue(download.stop_requested.is_set())

    def test_object_info_returns_key_bucket_size_and_no_version_id(self):
        record = {
            "s3": {
                "object": {"key": "nested/file.txt", "size": 12},
                "bucket": {"name": "archive"},
            }
        }

        self.assertEqual(
            download.object_info(record), ("nested/file.txt", "archive", 12, None)
        )

    def test_object_info_returns_version_id(self):
        record = {
            "s3": {
                "object": {
                    "key": "nested/file.txt",
                    "size": 12,
                    "versionId": "version-123",
                },
                "bucket": {"name": "archive"},
            }
        }

        self.assertEqual(
            download.object_info(record),
            ("nested/file.txt", "archive", 12, "version-123"),
        )

    def test_process_message_acknowledges_and_marks_all_valid_records(self):
        record = {
            "s3": {
                "object": {"key": "folder/object+%C2%A3.txt", "size": 1},
                "bucket": {"name": "archive"},
            }
        }
        message = {"receipt_handle": "receipt", "body": {"Records": [record]}}

        with (
            tempfile.TemporaryDirectory() as directory
        ):
            download.process_message(message, "queue-url", directory)

        _s3.download_file.assert_called_once_with(
            Bucket="archive",
            Key="folder/object £.txt",
            Filename=Path(directory) / "archive/folder/object £.txt",
        )
        _sqs.delete_message.assert_called_once_with(
            QueueUrl="queue-url", ReceiptHandle="receipt"
        )

    def test_process_message_downloads_specified_version_for_deleted_file(self):
        record = {
            "s3": {
                "object": {
                    "key": "folder/deleted-file.txt",
                    "size": 1,
                    "versionId": "version-to-restore",
                },
                "bucket": {"name": "archive"},
            }
        }
        message = {"receipt_handle": "receipt", "body": {"Records": [record]}}

        with tempfile.TemporaryDirectory() as directory:
            download.process_message(message, "queue-url", directory)

            _s3.download_file.assert_called_once_with(
                Bucket="archive",
                Key="folder/deleted-file.txt",
                Filename=Path(directory) / "archive/folder/deleted-file.txt",
                ExtraArgs={"VersionId": "version-to-restore"},
            )

        _sqs.delete_message.assert_called_once_with(
            QueueUrl="queue-url", ReceiptHandle="receipt"
        )

    def test_process_message_skips_empty_and_already_downloaded_objects(self):
        records = [
            {
                "s3": {
                    "object": {"key": "empty.txt", "size": 0},
                    "bucket": {"name": "archive"},
                }
            },
            {
                "s3": {
                    "object": {"key": "already-there.txt", "size": 10},
                    "bucket": {"name": "archive"},
                }
            },
        ]
        message = {"receipt_handle": "receipt", "body": {"Records": records}}

        with tempfile.TemporaryDirectory() as directory:
            existing_file = Path(directory) / "archive/already-there.txt"
            existing_file.parent.mkdir(parents=True)
            existing_file.touch()

            download.process_message(message, "queue-url", directory)


        _s3.download_file.assert_not_called()
        _sqs.delete_message.assert_called_once_with(
            QueueUrl="queue-url", ReceiptHandle="receipt"
        )


    def test_run_uses_environment_configuration_and_submits_messages(self):
        message = {"receipt_handle": "receipt", "body": {"Records": []}}

        def receive(**kwargs):
            if receive_message.call_count == 2:
                download.stop_requested.set()
                return {}
            return {
                "Messages": [
                    {"ReceiptHandle": "receipt", "Body": json.dumps(message["body"])}
                ]
            }

        with (
            patch.dict(
                os.environ,
                {"CONCURRENCY": "3", "QUEUE_URL": "queue-url", "WORK_DIR": "/work"},
            ),
            patch.object(
                download.sqs, "receive_message", side_effect=receive
            ) as receive_message,
            patch.object(download, "ThreadPoolExecutor") as executor,
        ):
            pool = executor.return_value.__enter__.return_value
            download.run()

        executor.assert_called_once_with(max_workers=3)
        self.assertEqual(
            receive_message.call_args_list,
            [
                call(QueueUrl="queue-url", MaxNumberOfMessages=3, WaitTimeSeconds=20),
                call(QueueUrl="queue-url", MaxNumberOfMessages=2, WaitTimeSeconds=20),
            ],
        )
        pool.submit.assert_called_once_with(
            download.process_message, message, "queue-url", "/work"
        )

    def test_run_waits_at_capacity_and_only_refills_completed_slots(self):
        futures = [Future() for _ in range(4)]
        messages = [
            {"ReceiptHandle": str(index), "Body": json.dumps({"Records": []})}
            for index in range(4)
        ]

        def receive(**kwargs):
            if receive_message.call_count == 1:
                return {"Messages": messages[:3]}
            self.assertEqual(wait_for_workers.call_count, 2)
            self.assertTrue(futures[0].done())
            download.stop_requested.set()
            return {"Messages": messages[3:]}

        def wait_at_capacity(pending, **kwargs):
            self.assertEqual(pending, set(futures[:3]))
            self.assertEqual(receive_message.call_count, 1)
            self.assertEqual(pool.submit.call_count, 3)
            if wait_for_workers.call_count == 2:
                futures[0].set_result(None)
                return {futures[0]}, set(futures[1:3])
            return set(), set(pending)

        with (
            patch.dict(
                os.environ,
                {"CONCURRENCY": "3", "QUEUE_URL": "queue-url", "WORK_DIR": "/work"},
            ),
            patch.object(
                download.sqs, "receive_message", side_effect=receive
            ) as receive_message,
            patch.object(
                download, "wait", side_effect=wait_at_capacity
            ) as wait_for_workers,
            patch.object(download, "ThreadPoolExecutor") as executor,
        ):
            pool = executor.return_value.__enter__.return_value
            pool.submit.side_effect = futures
            download.run()

        self.assertEqual(
            receive_message.call_args_list,
            [
                call(QueueUrl="queue-url", MaxNumberOfMessages=3, WaitTimeSeconds=20),
                call(QueueUrl="queue-url", MaxNumberOfMessages=1, WaitTimeSeconds=20),
            ],
        )
        self.assertEqual(wait_for_workers.call_count, 2)
        for wait_call in wait_for_workers.call_args_list:
            self.assertEqual(
                wait_call.kwargs,
                {"timeout": 1, "return_when": download.FIRST_COMPLETED},
            )
        self.assertEqual(pool.submit.call_count, 4)

    def test_run_exits_when_shutdown_during_capacity_wait(self):
        messages = [
            {"ReceiptHandle": str(index), "Body": json.dumps({"Records": []})}
            for index in range(10)
        ]
        futures = [Future() for _ in messages]

        def shutdown(pending, **kwargs):
            self.assertEqual(pending, set(futures))
            download.stop_requested.set()
            return set(), set(pending)

        with (
            patch.dict(
                os.environ,
                {"CONCURRENCY": "10", "QUEUE_URL": "queue-url", "WORK_DIR": "/work"},
            ),
            patch.object(
                download.sqs, "receive_message", return_value={"Messages": messages}
            ) as receive_message,
            patch.object(download, "wait", side_effect=shutdown) as wait_for_workers,
            patch.object(download, "ThreadPoolExecutor") as executor,
        ):
            pool = executor.return_value.__enter__.return_value
            pool.submit.side_effect = futures
            download.run()

        receive_message.assert_called_once_with(
            QueueUrl="queue-url", MaxNumberOfMessages=10, WaitTimeSeconds=20
        )
        wait_for_workers.assert_called_once()
        self.assertEqual(pool.submit.call_count, 10)

    def test_run_caps_receive_batch_at_ten_and_waits_when_queue_is_empty(self):
        def shutdown(**kwargs):
            download.stop_requested.set()
            return {}

        with (
            patch.dict(
                os.environ,
                {"CONCURRENCY": "50", "QUEUE_URL": "queue-url", "WORK_DIR": "/work"},
            ),
            patch.object(
                download.sqs, "receive_message", side_effect=shutdown
            ) as receive_message,
            patch.object(download, "ThreadPoolExecutor") as executor,
        ):
            download.run()

        receive_message.assert_called_once_with(
            QueueUrl="queue-url", MaxNumberOfMessages=10, WaitTimeSeconds=20
        )
        executor.return_value.__enter__.return_value.submit.assert_not_called()

    def test_main_registers_shutdown_signals_and_runs(self):
        with (
            patch.object(download.signal, "signal") as register_signal,
            patch.object(download, "run") as run,
        ):
            download.main()

        self.assertEqual(
            register_signal.call_args_list,
            [
                call(signal.SIGTERM, download.request_shutdown),
                call(signal.SIGINT, download.request_shutdown),
            ],
        )
        run.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()

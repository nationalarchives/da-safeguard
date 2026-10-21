import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch


_s3 = MagicMock()

with patch("boto3.client", return_value=_s3):
    import restore


class RestoreTests(unittest.TestCase):
    def setUp(self):
        _s3.reset_mock()
        _s3.head_object.side_effect = None
        _s3.head_object.return_value = {}

    def test_process_result_decodes_key_and_requests_restore(self):
        _s3.head_object.return_value = {}
        result = ("archive", "folder%20name%2Ffile%2Bname")

        response = restore.process_result(result)

        _s3.head_object.assert_called_once_with(
            Bucket="archive", Key="folder name/file+name"
        )
        _s3.restore_object.assert_called_once_with(
            Bucket="archive",
            Key="folder name/file+name",
            RestoreRequest={
                "Days": 1,
                "GlacierJobParameters": {"Tier": "Bulk"},
            },
        )
        self.assertEqual(response, {"errors": 0, "succeeded": 1, "skipped": 0})

    def test_process_result_skips_restore_already_in_progress(self):
        _s3.head_object.return_value = {"Restore": 'ongoing-request="true"'}

        response = restore.process_result(("archive", "object"))

        _s3.restore_object.assert_not_called()
        self.assertEqual(response, {"errors": 0, "succeeded": 0, "skipped": 1})

    def test_process_result_reports_head_object_errors(self):
        _s3.head_object.side_effect = RuntimeError("request failed")
        output = io.StringIO()

        with redirect_stdout(output):
            response = restore.process_result(("archive", "object"))

        _s3.restore_object.assert_not_called()
        self.assertEqual(response, {"errors": 1, "succeeded": 0, "skipped": 0})
        self.assertIn("('archive', 'object') request failed", output.getvalue())

    def test_run_queries_inventory_maps_results_and_reports_totals(self):
        results = [("archive", "one"), ("archive", "two"), ("archive", "three")]
        outcomes = [
            {"errors": 0, "succeeded": 1, "skipped": 0},
            {"errors": 0, "succeeded": 0, "skipped": 1},
            {"errors": 1, "succeeded": 0, "skipped": 0},
        ]
        connection = MagicMock()
        cursor = (
            connection.__enter__.return_value.cursor.return_value.__enter__.return_value
        )
        cursor.fetchall.return_value = results
        pool = MagicMock()
        pool.__enter__.return_value.map.return_value = outcomes
        output = io.StringIO()

        with (
            patch.object(restore.psycopg2, "connect", return_value=connection) as connect,
            patch.object(restore, "Pool", return_value=pool) as pool_factory,
            redirect_stdout(output),
        ):
            restore.run("archive")

        connect.assert_called_once_with(**restore.DB)
        cursor.execute.assert_called_once_with(
            "select bucket, s3_key from s3_inventory bi where bucket = %s and obj_size != 0 and not exists (select 1 from s3_inventory bii where bii.s3_key = bi.s3_key and obj_size is null) group by 1,2;",
            ("archive",),
        )
        self.assertEqual(cursor.fetchall.call_count, 1)
        pool_factory.assert_called_once_with(100)
        pool.__enter__.return_value.map.assert_called_once_with(
            restore.process_result, results
        )
        self.assertEqual(
            output.getvalue(),
            "Succeeded 1\nSkipped 1\nErrors 1\n",
        )


if __name__ == "__main__":
    unittest.main()

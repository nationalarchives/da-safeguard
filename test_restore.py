import io
import importlib
import os
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
        result = ("archive", "folder%20name%2Ffile%2Bname", None)

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

    def test_process_result_restores_specific_version(self):
        result = ("archive", "folder%20name%2Ffile.txt", "version-123")

        response = restore.process_result(result)

        _s3.head_object.assert_called_once_with(
            Bucket="archive",
            Key="folder name/file.txt",
            VersionId="version-123",
        )
        _s3.restore_object.assert_called_once_with(
            Bucket="archive",
            Key="folder name/file.txt",
            RestoreRequest={
                "Days": 1,
                "GlacierJobParameters": {"Tier": "Bulk"},
            },
            VersionId="version-123",
        )
        self.assertEqual(response, {"errors": 0, "succeeded": 1, "skipped": 0})

    def test_process_result_skips_restore_already_in_progress(self):
        _s3.head_object.return_value = {"Restore": 'ongoing-request="true"'}

        response = restore.process_result(("archive", "object", None))

        _s3.restore_object.assert_not_called()
        self.assertEqual(response, {"errors": 0, "succeeded": 0, "skipped": 1})

    def test_process_result_reports_head_object_errors(self):
        _s3.head_object.side_effect = RuntimeError("request failed")
        output = io.StringIO()

        with redirect_stdout(output):
            response = restore.process_result(("archive", "object", None))

        _s3.restore_object.assert_not_called()
        self.assertEqual(response, {"errors": 1, "succeeded": 0, "skipped": 0})
        self.assertIn(
            "('archive', 'object', None) request failed", output.getvalue()
        )

    def test_run_queries_inventory_maps_results_and_reports_totals(self):
        results = [
            ("archive", "one", None),
            ("archive", "two", None),
            ("archive", "three", None),
        ]
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
            "select vsi.bucket, vsi.s3_key, null from versioned_s3_inventory vsi where obj_size != 0 and vsi.is_latest and not vsi.is_delete_marker and bucket = %s and starts_with(vsi.s3_key, %s);",
            ("archive", ""),
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

    def test_run_deleted_queries_latest_non_delete_versions(self):
        results = [("archive", "deleted%20file.txt", "version-456")]
        connection = MagicMock()
        cursor = (
            connection.__enter__.return_value.cursor.return_value.__enter__.return_value
        )
        cursor.fetchall.return_value = results
        pool = MagicMock()
        pool.__enter__.return_value.map.return_value = [
            {"errors": 0, "succeeded": 1, "skipped": 0}
        ]

        with (
            patch.object(restore.psycopg2, "connect", return_value=connection),
            patch.object(restore, "Pool", return_value=pool),
            redirect_stdout(io.StringIO()),
        ):
            restore.run("archive", prefix="folder/", deleted=True)

        cursor.execute.assert_called_once_with(
            "select distinct on (vsi.s3_key) vsi.bucket, vsi.s3_key, vsi.version_id from versioned_s3_inventory vsi join versioned_s3_inventory marker on marker.bucket = vsi.bucket and marker.s3_key = vsi.s3_key and marker.is_latest and marker.is_delete_marker where vsi.obj_size != 0 and not vsi.is_delete_marker and vsi.bucket = %s and starts_with(vsi.s3_key, %s) order by vsi.s3_key, vsi.last_modified_date desc;",
            ("archive", "folder/"),
        )
        cursor.fetchall.assert_called_once_with()
        pool.__enter__.return_value.map.assert_called_once_with(
            restore.process_result, results
        )

    def test_deleted_environment_variable_is_parsed_as_boolean(self):
        original_deleted = restore.DELETED

        try:
            with (
                patch.dict(os.environ, {"DELETED": "true"}),
                patch("boto3.client", return_value=_s3),
            ):
                importlib.reload(restore)
                self.assertTrue(restore.DELETED)

            with (
                patch.dict(os.environ, {"DELETED": "false"}),
                patch("boto3.client", return_value=_s3),
            ):
                importlib.reload(restore)
                self.assertFalse(restore.DELETED)
        finally:
            restore.DELETED = original_deleted
            
    def test_split_list_handles_commas_spaces_and_blanks(self):
        self.assertEqual(
            restore.split_list("sftp_user_29/, sftp_user_30/"),
            ["sftp_user_29/", "sftp_user_30/"],
        )
        self.assertEqual(restore.split_list("source-das-6"), ["source-das-6"])
        self.assertEqual(restore.split_list(""), [""])
        self.assertEqual(restore.split_list("sftp_user_29/,"), ["sftp_user_29/"])

                

if __name__ == "__main__":
    unittest.main()

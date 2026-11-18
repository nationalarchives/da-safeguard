import io
import unittest
from contextlib import redirect_stdout

import psycopg2

import make_chunks

TB = 1_000_000_000_000


class FakeCursor:
    """Answers the sizes query from a dictionary of {prefix: [(item, bytes), ...]}."""

    def __init__(self, answers):
        self.answers = answers

    def execute(self, sql, params):
        self.rows = self.answers[params["prefix"]]

    def fetchall(self):
        return self.rows


class TestChunking(unittest.TestCase):

    def test_new_chunk_starts_when_the_next_item_would_not_fit(self):
        items = [("a/", 2 * TB), ("b/", 1 * TB), ("c/", 2 * TB)]

        chunks, too_big = make_chunks.make_chunks(items, limit=4 * TB)

        self.assertEqual(chunks, [(["a/", "b/"], 3 * TB), (["c/"], 2 * TB)])
        self.assertEqual(too_big, [])

    def test_folder_over_the_limit_is_split_into_its_subfolders(self):
        cur = FakeCursor({
            "": [("small/", 1 * TB), ("big/", 6 * TB)],
            "big/": [("big/x/", 3 * TB), ("big/loose.txt", 1 * TB), ("big/y/", 2 * TB)],
        })

        with redirect_stdout(io.StringIO()):
            items = make_chunks.items_under(cur, "", limit=4 * TB)

        self.assertEqual(items, [("small/", 1 * TB), ("big/x/", 3 * TB), ("big/loose.txt", 1 * TB), ("big/y/", 2 * TB)])

    def test_single_file_over_the_limit_is_reported_not_chunked(self):
        chunks, too_big = make_chunks.make_chunks([("a/", 1 * TB), ("huge.tif", 5 * TB)], limit=4 * TB)

        self.assertEqual(chunks, [(["a/"], 1 * TB)])
        self.assertEqual(too_big, [("huge.tif", 5 * TB)])


class TestRealData(unittest.TestCase):
    """Uses the SFTP inventory loaded into your local database. Skipped if it isn't there."""

    @classmethod
    def setUpClass(cls):
        try:
            cls.conn = psycopg2.connect(**make_chunks.DB)
        except psycopg2.OperationalError:
            raise unittest.SkipTest("Local database not reachable")
        with cls.conn.cursor() as cur:
            cur.execute(f"select coalesce(sum(obj_size), 0) from {make_chunks.INVENTORY_TABLE} "
                        "where bucket = %s and is_latest and not is_delete_marker and obj_size != 0",
                        (make_chunks.BUCKET,))
            cls.bucket_total = cur.fetchone()[0]
            if cls.bucket_total == 0:
                raise unittest.SkipTest("No inventory rows for the bucket")
            with redirect_stdout(io.StringIO()):
                cls.items = make_chunks.items_under(cur, "")
        cls.chunks, cls.too_big = make_chunks.make_chunks(cls.items)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_every_chunk_fits_the_limit(self):
        for names, size in self.chunks:
            self.assertLessEqual(size, make_chunks.LIMIT, f"Chunk with {names[:3]} is too big")

    def test_sizes_add_up_to_the_whole_bucket(self):
        # nothing missed and nothing counted twice
        chunked = sum(size for _, size in self.chunks)
        left_out = sum(size for _, size in self.too_big)
        self.assertEqual(chunked + left_out, self.bucket_total)


if __name__ == "__main__":
    unittest.main()
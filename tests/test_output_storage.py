"""Filesystem failures, exact replays, and cross-batch sample safety."""

from __future__ import annotations

import csv
import io
import json
import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from parsedmarc.output_storage import (
    append_csv_file,
    append_json_file,
    write_sample_file,
)


class TestOutputStorage(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.json_path = str(self.root / "aggregate.json")
        self.csv_path = str(self.root / "aggregate.csv")

    def test_json_append_retains_public_duplicate_semantics_by_default(self):
        append_json_file(self.json_path, [{"id": 1}])
        append_json_file(self.json_path, [{"id": 1}])
        self.assertEqual(json.loads(Path(self.json_path).read_text()), [{"id": 1}] * 2)

    def test_exact_json_retry_preserves_batch_multiplicity(self):
        batch = [{"id": 1}, {"id": 1}, {"id": 2}]
        for _ in range(3):
            append_json_file(self.json_path, batch, deduplicate=True)
        self.assertEqual(json.loads(Path(self.json_path).read_text()), batch)

    def test_json_corruption_is_not_overwritten(self):
        for invalid in (b" ", b"[", b'{"not": "an array"}', b"\xff"):
            with self.subTest(invalid=invalid):
                Path(self.json_path).write_bytes(invalid)
                with self.assertRaises(ValueError):
                    append_json_file(self.json_path, [{"id": 1}])
                self.assertEqual(Path(self.json_path).read_bytes(), invalid)

    def test_zero_byte_bootstrap_retains_existing_api_semantics(self):
        Path(self.json_path).write_bytes(b"")
        append_json_file(self.json_path, [{"id": 1}])
        self.assertEqual(json.loads(Path(self.json_path).read_text()), [{"id": 1}])

    def test_json_serialization_failure_preserves_history(self):
        append_json_file(self.json_path, [{"id": 1}])
        original = Path(self.json_path).read_bytes()
        with self.assertRaises(TypeError):
            append_json_file(self.json_path, [{"bad": object()}])
        self.assertEqual(Path(self.json_path).read_bytes(), original)
        self.assertEqual(list(self.root.glob(".parsedmarc-*.tmp")), [])

    def test_json_fsync_failure_preserves_history(self):
        append_json_file(self.json_path, [{"id": 1}])
        original = Path(self.json_path).read_bytes()
        with patch("os.fsync", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                append_json_file(self.json_path, [{"id": 2}])
        self.assertEqual(Path(self.json_path).read_bytes(), original)
        self.assertEqual(list(self.root.glob(".parsedmarc-*.tmp")), [])

    def test_json_replace_failure_preserves_history(self):
        append_json_file(self.json_path, [{"id": 1}])
        original = Path(self.json_path).read_bytes()
        with patch("os.replace", side_effect=PermissionError("replace denied")):
            with self.assertRaises(PermissionError):
                append_json_file(self.json_path, [{"id": 2}])
        self.assertEqual(Path(self.json_path).read_bytes(), original)

    def test_json_read_error_is_not_discarded(self):
        append_json_file(self.json_path, [{"id": 1}])
        original = Path(self.json_path).read_bytes()
        actual_open = open

        def failing_open(path, mode="r", *args, **kwargs):
            if str(path) == self.json_path and mode == "r":
                raise PermissionError("read denied")
            return actual_open(path, mode, *args, **kwargs)

        with patch("builtins.open", side_effect=failing_open):
            with self.assertRaises(PermissionError):
                append_json_file(self.json_path, [{"id": 2}])
        self.assertEqual(Path(self.json_path).read_bytes(), original)

    def test_json_concurrent_read_merge_write_does_not_lose_rows(self):
        with ThreadPoolExecutor(max_workers=6) as pool:
            list(
                pool.map(
                    lambda i: append_json_file(self.json_path, [{"id": i}]), range(30)
                )
            )
        self.assertEqual(
            sorted(x["id"] for x in json.loads(Path(self.json_path).read_text())),
            list(range(30)),
        )

    def test_csv_retry_and_partial_prior_batch(self):
        append_csv_file(self.csv_path, "id,count\nA,1\n", deduplicate=True)
        full = "id,count\nA,1\nB,2\nB,2\n"
        append_csv_file(self.csv_path, full, deduplicate=True)
        append_csv_file(self.csv_path, full, deduplicate=True)
        with open(self.csv_path, newline="") as result:
            self.assertEqual(
                list(csv.reader(result)),
                [["id", "count"], ["A", "1"], ["B", "2"], ["B", "2"]],
            )

    def test_csv_schema_mismatch_retains_history(self):
        Path(self.csv_path).write_text("old,header\n1,2\n")
        original = Path(self.csv_path).read_bytes()
        with self.assertRaises(ValueError):
            append_csv_file(self.csv_path, "new,header\n3,4\n", deduplicate=True)
        self.assertEqual(Path(self.csv_path).read_bytes(), original)

    def test_csv_truncated_quoted_field_is_not_overwritten(self):
        Path(self.csv_path).write_text('id,count\n"unfinished')
        original = Path(self.csv_path).read_bytes()
        with self.assertRaises(ValueError):
            append_csv_file(self.csv_path, "id,count\nA,1\n", deduplicate=True)
        self.assertEqual(Path(self.csv_path).read_bytes(), original)

    def test_csv_handles_large_quoted_unicode_and_multiline_fields(self):
        text = io.StringIO(newline="")
        writer = csv.writer(text)
        row = ["A", 'café, "quoted"\n' + "x" * 140000]
        writer.writerows([["id", "data"], row])
        append_csv_file(self.csv_path, text.getvalue(), deduplicate=True)
        append_csv_file(self.csv_path, text.getvalue(), deduplicate=True)
        with open(self.csv_path, newline="") as result:
            self.assertEqual(list(csv.reader(result)), [["id", "data"], row])

    def test_csv_replace_failure_preserves_history(self):
        append_csv_file(self.csv_path, "id,count\nA,1\n", deduplicate=True)
        before = Path(self.csv_path).read_bytes()
        with patch("os.replace", side_effect=OSError("disk unavailable")):
            with self.assertRaises(OSError):
                append_csv_file(self.csv_path, "id,count\nB,2\n", deduplicate=True)
        self.assertEqual(Path(self.csv_path).read_bytes(), before)

    def test_different_samples_same_subject_across_batches(self):
        first = write_sample_file(str(self.root), "Same subject", "first sample")
        second = write_sample_file(str(self.root), "Same subject", "second sample")
        self.assertNotEqual(first, second)
        self.assertEqual(Path(first).read_text(), "first sample")
        self.assertEqual(Path(second).read_text(), "second sample")

    def test_exact_sample_retry_reuses_existing_file(self):
        first = write_sample_file(str(self.root), "Sample", "café\nbody")
        self.assertEqual(
            write_sample_file(str(self.root), "Sample", "café\nbody"), first
        )
        self.assertEqual(len(list(self.root.glob("*.eml"))), 1)

    def test_sample_fsync_failure_does_not_publish_a_partial_file(self):
        with patch("os.fsync", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                write_sample_file(str(self.root), "Sample", "body")
        self.assertEqual(list(self.root.glob("*.eml")), [])
        self.assertEqual(list(self.root.glob("*.tmp")), [])

    def test_sample_link_failure_cleans_temporary_file(self):
        with patch("os.link", side_effect=OSError("filesystem unavailable")):
            with self.assertRaises(OSError):
                write_sample_file(str(self.root), "Sample", "body")
        self.assertEqual(list(self.root.glob("*.eml")), [])
        self.assertEqual(list(self.root.glob("*.tmp")), [])

    @unittest.skipIf(
        os.name == "nt", "Creating symlinks may require a Windows privilege"
    )
    def test_sample_does_not_follow_existing_symlink(self):
        outside = self.root / "outside.txt"
        outside.write_text("untouched")
        (self.root / "Sample.eml").symlink_to(outside)
        created = write_sample_file(str(self.root), "Sample", "new sample")
        self.assertEqual(outside.read_text(), "untouched")
        self.assertNotEqual(created, str(self.root / "Sample.eml"))

    def test_concurrent_samples_do_not_overwrite_each_other(self):
        with ThreadPoolExecutor(max_workers=4) as pool:
            paths = list(
                pool.map(
                    lambda i: write_sample_file(
                        str(self.root), "Subject", f"sample-{i}"
                    ),
                    range(12),
                )
            )
        self.assertEqual(len(set(paths)), 12)
        self.assertEqual(
            {Path(p).read_text() for p in paths}, {f"sample-{i}" for i in range(12)}
        )

    def test_empty_json_batch_does_not_create_output(self):
        append_json_file(self.json_path, [])
        self.assertFalse(Path(self.json_path).exists())


class TestJsonCsvPair(unittest.TestCase):
    def setUp(self):
        import tempfile
        from pathlib import Path

        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.json_file = str(self.root / "report.json")
        self.csv_file = str(self.root / "report.csv")

    @staticmethod
    def render(reports):
        import csv
        import io

        buffer = io.StringIO(newline="")
        writer = csv.writer(buffer)
        writer.writerow(["subject"])
        writer.writerows([[r["subject"]] for r in reports])
        return buffer.getvalue()

    def save(self, reports, *, deduplicate=True):
        from parsedmarc.output_storage import save_json_csv_pair

        save_json_csv_pair(
            self.json_file, self.csv_file, reports, self.render, deduplicate=deduplicate
        )

    def read(self):
        import csv
        import json

        with open(self.json_file) as source:
            reports = json.load(source)
        with open(self.csv_file, newline="") as source:
            rows = list(csv.DictReader(source))
        return reports, rows

    def test_distinct_reports_with_identical_csv_rows_are_preserved(self):
        self.save([{"subject": "same", "identity": "one"}])
        self.save([{"subject": "same", "identity": "two"}])
        reports, rows = self.read()
        self.assertEqual(len(reports), 2)
        self.assertEqual(len(rows), 2)

    def test_exact_report_retry_does_not_add_csv_rows(self):
        self.save([{"subject": "report", "id": "one"}])
        self.save([{"subject": "report", "id": "one"}])
        reports, rows = self.read()
        self.assertEqual((len(reports), len(rows)), (1, 1))

    def test_failure_events_keep_at_least_once_multiplicity(self):
        for _ in range(2):
            self.save([{"subject": "failure"}], deduplicate=False)
        reports, rows = self.read()
        self.assertEqual((len(reports), len(rows)), (2, 2))

    def test_retry_repairs_csv_after_second_replace_failed(self):
        from unittest.mock import patch
        import os

        replace = os.replace

        def fail_csv(source, destination):
            if str(destination) == self.csv_file:
                raise OSError("CSV destination temporarily unavailable")
            return replace(source, destination)

        with patch("parsedmarc.output_storage.os.replace", side_effect=fail_csv):
            with self.assertRaises(OSError):
                self.save([{"subject": "new report"}])
        self.save([{"subject": "new report"}])
        reports, rows = self.read()
        self.assertEqual((len(reports), len(rows)), (1, 1))

    def test_orphaned_csv_is_not_overwritten(self):
        from pathlib import Path

        original = b"subject\r\nhistorical\r\n"
        Path(self.csv_file).write_bytes(original)
        with self.assertRaisesRegex(ValueError, "no JSON source"):
            self.save([{"subject": "new"}])
        self.assertEqual(Path(self.csv_file).read_bytes(), original)
        self.assertFalse(Path(self.json_file).exists())

    def test_renderer_failure_happens_before_committing_json(self):
        from parsedmarc.output_storage import save_json_csv_pair
        from pathlib import Path

        self.save([{"subject": "old"}])
        original = Path(self.json_file).read_bytes()

        def broken_renderer(reports):
            raise ValueError("invalid output shape")

        with self.assertRaises(ValueError):
            save_json_csv_pair(
                self.json_file,
                self.csv_file,
                [{"subject": "new"}],
                broken_renderer,
                deduplicate=True,
            )
        self.assertEqual(Path(self.json_file).read_bytes(), original)

    def test_empty_json_cannot_authorize_discarding_nonempty_csv(self):
        Path(self.json_file).write_bytes(b"")
        original = b"subject\r\nhistorical\r\n"
        Path(self.csv_file).write_bytes(original)
        with self.assertRaises(ValueError):
            self.save([{"subject": "new"}])
        self.assertEqual(Path(self.csv_file).read_bytes(), original)
        self.assertEqual(Path(self.json_file).read_bytes(), b"")

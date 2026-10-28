import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from pipeline import check_quality, retry, run_pipeline, transform


class PipelineTests(unittest.TestCase):
    def test_transform_trims_values(self):
        self.assertEqual(transform([[" 42 ", " Ada "]]), [["42", "Ada"]])

    def test_retry_retries_io_errors(self):
        operation = Mock(side_effect=[OSError("temporary failure"), "done"])

        result = retry(operation, attempts=2, delay=0, description="test operation")

        self.assertEqual(result, "done")
        self.assertEqual(operation.call_count, 2)

    def test_quality_checks_detect_nulls_duplicates_and_row_count_changes(self):
        checks = check_quality(
            [["a", "b"], ["a", "b"], ["c", ""], ["d", "e"]],
            [["a", "b"], ["a", "b"], ["c", ""]],
        )
        self.assertFalse(checks["row_count"]["passed"])
        self.assertFalse(checks["null_values"]["passed"])
        self.assertEqual(checks["null_values"]["count"], 1)
        self.assertFalse(checks["duplicate_rows"]["passed"])
        self.assertEqual(checks["duplicate_rows"]["count"], 1)

    def test_run_writes_transformed_csv_and_failed_quality_status(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            source = root / "input.csv"
            output = root / "output" / "cleaned.csv"
            status_path = root / "status.json"
            source.write_text("id,name\n1, Ada \n2,\n", encoding="utf-8")

            status = run_pipeline(source, output, status_path, retry_delay=0)

            self.assertEqual(status["status"], "FAILED")
            self.assertEqual(status["source_row_count"], 2)
            self.assertEqual(status["quality_checks"]["null_values"]["count"], 1)
            with output.open(encoding="utf-8", newline="") as output_file:
                self.assertEqual(list(csv.reader(output_file)), [["id", "name"], ["1", "Ada"], ["2", ""]])
            self.assertEqual(json.loads(status_path.read_text(encoding="utf-8"))["status"], "FAILED")

    def test_invalid_csv_is_recorded_as_failed(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            source = root / "input.csv"
            source.write_text("id,name\n1\n", encoding="utf-8")
            status_path = root / "status.json"

            status = run_pipeline(source, root / "output.csv", status_path, retry_delay=0)

            self.assertEqual(status["status"], "FAILED")
            self.assertIn("expected 2", status["error"])
            self.assertEqual(json.loads(status_path.read_text(encoding="utf-8"))["status"], "FAILED")


if __name__ == "__main__":
    unittest.main()

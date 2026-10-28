"""A small CSV ETL pipeline with logging, retries, and data quality checks."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, TypeVar

T = TypeVar("T")
LOGGER = logging.getLogger("pipeline")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def retry(operation: Callable[[], T], attempts: int, delay: float, description: str) -> T:
    """Retry an I/O operation a bounded number of times, then report its error."""
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except OSError:
            if attempt == attempts:
                LOGGER.exception("%s failed after %d attempts", description, attempts)
                raise
            LOGGER.warning(
                "%s failed (attempt %d/%d); retrying in %.2f seconds",
                description,
                attempt,
                attempts,
                delay,
            )
            time.sleep(delay)
    raise RuntimeError("Retry operation ended unexpectedly")


def read_csv(path: Path) -> tuple[list[str], list[list[str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source)
        headers = next(reader, None)
        if not headers or any(not header.strip() for header in headers):
            raise ValueError("The input CSV must have a non-empty header row")
        if len(set(headers)) != len(headers):
            raise ValueError("The input CSV contains duplicate column names")

        rows = list(reader)
    for line_number, row in enumerate(rows, start=2):
        if len(row) != len(headers):
            raise ValueError(
                f"Row {line_number} has {len(row)} values; expected {len(headers)}"
            )
    return headers, rows


def transform(rows: list[list[str]]) -> list[list[str]]:
    """Trim surrounding whitespace without changing the CSV schema."""
    return [[value.strip() for value in row] for row in rows]


def check_quality(
    source_rows: list[list[str]], output_rows: list[list[str]]
) -> dict[str, dict[str, Any]]:
    null_cells = sum(value == "" for row in output_rows for value in row)
    duplicate_rows = len(output_rows) - len({tuple(row) for row in output_rows})
    row_counts_match = len(source_rows) == len(output_rows)
    return {
        "row_count": {
            "source": len(source_rows),
            "output": len(output_rows),
            "passed": row_counts_match,
        },
        "null_values": {
            "count": null_cells,
            "passed": null_cells == 0,
        },
        "duplicate_rows": {
            "count": duplicate_rows,
            "passed": duplicate_rows == 0,
        },
    }


def write_status(path: Path, status: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(status, indent=2) + "\n", encoding="utf-8")


def run_pipeline(
    input_path: Path,
    output_path: Path,
    status_path: Path,
    attempts: int = 3,
    retry_delay: float = 1.0,
) -> dict[str, Any]:
    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    if retry_delay < 0:
        raise ValueError("retry_delay cannot be negative")

    status: dict[str, Any] = {
        "run_id": str(uuid.uuid4()),
        "started_at": utc_now(),
        "status": "RUNNING",
        "input": str(input_path),
        "output": str(output_path),
    }
    try:
        headers, source_rows = retry(
            lambda: read_csv(input_path), attempts, retry_delay, "Read input CSV"
        )
        output_rows = transform(source_rows)

        def write_output() -> None:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with output_path.open("w", encoding="utf-8", newline="") as destination:
                writer = csv.writer(destination)
                writer.writerow(headers)
                writer.writerows(output_rows)

        retry(write_output, attempts, retry_delay, "Write output CSV")
        quality_checks = check_quality(source_rows, output_rows)
        passed = all(check["passed"] for check in quality_checks.values())
        status.update(
            {
                "status": "SUCCESS" if passed else "FAILED",
                "source_row_count": len(source_rows),
                "output_row_count": len(output_rows),
                "quality_checks": quality_checks,
            }
        )
        if not passed:
            LOGGER.error("Data quality checks failed")
        else:
            LOGGER.info("Pipeline completed; all data quality checks passed")
    except Exception as error:
        status.update({"status": "FAILED", "error": str(error)})
        LOGGER.exception("Pipeline run failed")
    finally:
        status["finished_at"] = utc_now()
        write_status(status_path, status)
    return status


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform a CSV file and record pipeline and data quality status."
    )
    parser.add_argument("--input", type=Path, required=True, help="Source CSV path")
    parser.add_argument("--output", type=Path, required=True, help="Output CSV path")
    parser.add_argument(
        "--status",
        type=Path,
        default=Path("run_status.json"),
        help="Run status JSON path (default: run_status.json)",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=Path("pipeline.log"),
        help="Log file path (default: pipeline.log)",
    )
    parser.add_argument(
        "--retries", type=int, default=3, help="Maximum attempts for file I/O"
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=1.0,
        help="Seconds between retry attempts",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(args.log_file, encoding="utf-8"),
        ],
    )
    status = run_pipeline(
        args.input,
        args.output,
        args.status,
        attempts=args.retries,
        retry_delay=args.retry_delay,
    )
    return 0 if status["status"] == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

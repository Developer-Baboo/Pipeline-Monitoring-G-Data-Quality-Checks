# Pipeline Monitoring & Data Quality Checks

A lightweight, dependency-free Python ETL project for transforming CSV files while monitoring run status and checking basic data quality.

## What it does

- Reads a source CSV and trims surrounding whitespace from each value.
- Retries CSV file reads and writes when an operating-system I/O error occurs.
- Logs progress and errors to the console and a log file.
- Checks that source and output row counts match.
- Counts empty values and duplicate output rows.
- Writes a JSON run-status report, including timestamps and quality-check results.
- Exits with code `0` when all checks pass and `1` when the run or a check fails.

## Requirements

- Python 3.9 or newer
- No third-party packages

## Quick start

From the project directory, run:

```powershell
python pipeline.py --input sample_data\input.csv --output output\cleaned.csv
```

By default, the pipeline writes `run_status.json` and `pipeline.log` in the current directory. Output folders are created automatically.

## Options

```text
--input       Required source CSV file
--output      Required transformed CSV file
--status      Run-status JSON path (default: run_status.json)
--log-file    Log file path (default: pipeline.log)
--retries     Maximum attempts for file I/O (default: 3)
--retry-delay Seconds between retry attempts (default: 1)
```

Example with custom monitoring files and retry settings:

```powershell
python pipeline.py --input sample_data\input.csv --output output\cleaned.csv `
  --status logs\latest-run.json --log-file logs\pipeline.log --retries 4 --retry-delay 0.5
```

## Quality-check behavior

The pipeline keeps all input rows and columns; it does not remove invalid or duplicate records. It trims cell values, then reports:

- **Row count:** source and output data-row counts should match.
- **Null values:** empty cells in the transformed output are counted; the check passes only when the count is zero.
- **Duplicate rows:** fully identical output data rows are counted; the check passes only when the count is zero.

Any failed quality check marks the run as `FAILED` but still writes the transformed CSV and status report. Invalid CSV structure and I/O errors also mark the run as failed. The JSON status report contains the run ID, timestamps, file paths, counts, checks, and any error message.

## Project layout

```text
.
├── pipeline.py
├── sample_data/
│   └── input.csv
└── tests/
    └── test_pipeline.py
```

## Run tests

```powershell
python -m unittest discover -s tests -v
```

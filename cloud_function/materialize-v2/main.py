## main.py
# Build a single, ever-growing CSV from all structured JSONL files.
# Reads:  gs://<bucket>/<STRUCTURED_PREFIX>/run_id=*/jsonl/*.jsonl
# Writes: gs://<bucket>/<STRUCTURED_PREFIX>/datasets/listings_master_v2.csv

import csv
import json
import os
import re
from datetime import datetime, timezone
from typing import Dict, Iterable, Optional

from flask import Request, jsonify
from google.cloud import storage

# -------------------- ENV --------------------
BUCKET_NAME = os.getenv("GCS_BUCKET")
STRUCTURED_PREFIX = os.getenv("STRUCTURED_PREFIX", "structured")
MIN_RUN_ID = os.getenv("MIN_RUN_ID", "").strip()

storage_client = storage.Client()

RUN_ID_ISO_RE = re.compile(r"^\d{8}T\d{6}Z$")
RUN_ID_PLAIN_RE = re.compile(r"^\d{14}$")

CSV_COLUMNS = [
    "post_id",
    "run_id",
    "scraped_at",
    "price",
    "year",
    "make",
    "model",
    "mileage",
    "condition",
    "cylinders",
    "drive_type",
    "fuel_type",
    "paint_color",
    "title_status",
    "transmission",
    "vehicle_type",
    "listing_post_id",
    "listing_posted_at",
    "source_txt",
]


def _is_valid_run_id(run_id: str) -> bool:
    return bool(RUN_ID_ISO_RE.match(run_id) or RUN_ID_PLAIN_RE.match(run_id))


def _run_id_to_dt(run_id: str) -> datetime:
    if RUN_ID_ISO_RE.match(run_id):
        return datetime.strptime(run_id, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    if RUN_ID_PLAIN_RE.match(run_id):
        return datetime.strptime(run_id, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    return datetime.min.replace(tzinfo=timezone.utc)


def _list_run_ids(bucket: str, structured_prefix: str) -> list[str]:
    it = storage_client.list_blobs(bucket, prefix=f"{structured_prefix}/", delimiter="/")
    for _ in it:
        pass

    run_ids = []
    for prefix in getattr(it, "prefixes", []):
        tail = prefix.rstrip("/").split("/")[-1]
        if not tail.startswith("run_id="):
            continue
        run_id = tail.split("run_id=", 1)[1]
        if _is_valid_run_id(run_id):
            run_ids.append(run_id)
    return sorted(run_ids)


def _record_to_row(record: Dict) -> list[str]:
    row = []
    for column in CSV_COLUMNS:
        value = record.get(column, "")
        row.append("" if value is None else value)
    return row


def _open_gcs_text_writer(bucket: str, key: str):
    blob = storage_client.bucket(bucket).blob(key)
    return blob.open("w")


def _write_csv(records: Iterable[Dict], dest_key: str) -> int:
    rows_written = 0
    with _open_gcs_text_writer(BUCKET_NAME, dest_key) as out:
        writer = csv.writer(out)
        writer.writerow(CSV_COLUMNS)
        for record in records:
            writer.writerow(_record_to_row(record))
            rows_written += 1
    return rows_written


def _get_min_run_id(request: Request) -> Optional[str]:
    body = request.get_json(silent=True)
    min_run_id = ""
    if isinstance(body, dict):
        min_run_id = str(body.get("min_run_id", "")).strip()
    if not min_run_id:
        min_run_id = MIN_RUN_ID
    if not min_run_id:
        return None
    if not _is_valid_run_id(min_run_id):
        raise ValueError(f"invalid min_run_id: {min_run_id}")
    return min_run_id


def _jsonl_records_for_run(bucket: str, structured_prefix: str, run_id: str, stats: Dict[str, int]):
    prefix = f"{structured_prefix}/run_id={run_id}/jsonl/"

    for blob in storage_client.bucket(bucket).list_blobs(prefix=prefix):
        if not blob.name.endswith(".jsonl"):
            continue

        stats["files_scanned"] += 1
        try:
            data = blob.download_as_text()
        except Exception:
            stats["invalid_records"] += 1
            continue

        for line in data.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except Exception:
                stats["invalid_records"] += 1
                continue

            if not isinstance(record, dict):
                stats["invalid_records"] += 1
                continue

            record_run_id = str(record.get("run_id", "")).strip()
            if not _is_valid_run_id(record_run_id):
                record["run_id"] = run_id
            yield record


def materialize_http(request: Request):
    try:
        if not BUCKET_NAME:
            return jsonify({"ok": False, "error": "missing GCS_BUCKET env"}), 500

        min_run_id = _get_min_run_id(request)
        run_ids = _list_run_ids(BUCKET_NAME, STRUCTURED_PREFIX)
        if min_run_id:
            min_run_dt = _run_id_to_dt(min_run_id)
            run_ids = [run_id for run_id in run_ids if _run_id_to_dt(run_id) >= min_run_dt]

        if not run_ids:
            return jsonify({
                "ok": False,
                "error": f"no runs found under {STRUCTURED_PREFIX}/",
                "min_run_id": min_run_id,
            }), 200

        latest_by_post: Dict[str, Dict] = {}
        stats = {"files_scanned": 0, "invalid_records": 0}

        for run_id in run_ids:
            for record in _jsonl_records_for_run(BUCKET_NAME, STRUCTURED_PREFIX, run_id, stats):
                post_id = record.get("post_id")
                if not post_id:
                    continue

                previous = latest_by_post.get(post_id)
                record_run_id = str(record.get("run_id", run_id))
                if previous is None or _run_id_to_dt(record_run_id) > _run_id_to_dt(str(previous.get("run_id", ""))):
                    latest_by_post[post_id] = record

        final_key = f"{STRUCTURED_PREFIX}/datasets/listings_master_v2.csv"
        ordered_records = [latest_by_post[post_id] for post_id in sorted(latest_by_post)]
        rows_written = _write_csv(ordered_records, final_key)

        return jsonify({
            "ok": True,
            "runs_scanned": len(run_ids),
            "files_scanned": stats["files_scanned"],
            "invalid_records_skipped": stats["invalid_records"],
            "unique_listings": len(latest_by_post),
            "rows_written": rows_written,
            "output_csv": f"gs://{BUCKET_NAME}/{final_key}",
            "min_run_id": min_run_id,
        }), 200
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"ok": False, "error": f"{type(exc).__name__}: {exc}"}), 500

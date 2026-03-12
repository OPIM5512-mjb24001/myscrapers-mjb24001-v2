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
LATEST_ONLY_COLUMNS = {"post_id", "run_id", "scraped_at", "listing_posted_at", "source_txt"}
BACKFILL_COLUMNS = [column for column in CSV_COLUMNS if column not in LATEST_ONLY_COLUMNS]
MAKE_ALIASES = {
    "acura": "Acura",
    "audi": "Audi",
    "bmw": "BMW",
    "benz": "Mercedes-Benz",
    "buick": "Buick",
    "cadillac": "Cadillac",
    "chevrolet": "Chevrolet",
    "chevy": "Chevrolet",
    "chrysler": "Chrysler",
    "dodge": "Dodge",
    "ford": "Ford",
    "gmc": "GMC",
    "honda": "Honda",
    "hyundai": "Hyundai",
    "infiniti": "Infiniti",
    "infinity": "Infiniti",
    "jaguar": "Jaguar",
    "jeep": "Jeep",
    "kia": "Kia",
    "lexus": "Lexus",
    "lincoln": "Lincoln",
    "mazda": "Mazda",
    "mercedes": "Mercedes-Benz",
    "mercedes benz": "Mercedes-Benz",
    "mercedes-benz": "Mercedes-Benz",
    "mini": "Mini",
    "mitsubishi": "Mitsubishi",
    "nissan": "Nissan",
    "porsche": "Porsche",
    "ram": "Ram",
    "subaru": "Subaru",
    "toyota": "Toyota",
    "volkswagen": "Volkswagen",
    "volkswagon": "Volkswagen",
    "volvo": "Volvo",
    "vw": "Volkswagen",
}
MAKE_LOOKUP = {
    re.sub(r"\s+", " ", key.replace("-", " ")).strip().lower(): value
    for key, value in MAKE_ALIASES.items()
}
BLOCKED_MAKE_KEYS = {"accord", "civic", "e350", "f 150", "f150", "limited", "super", "whitney", "xl", "xlt"}


def _is_valid_run_id(run_id: str) -> bool:
    return bool(RUN_ID_ISO_RE.match(run_id) or RUN_ID_PLAIN_RE.match(run_id))


def _run_id_to_dt(run_id: str) -> datetime:
    if RUN_ID_ISO_RE.match(run_id):
        return datetime.strptime(run_id, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    if RUN_ID_PLAIN_RE.match(run_id):
        return datetime.strptime(run_id, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    return datetime.min.replace(tzinfo=timezone.utc)


def _parse_scraped_at(value: str) -> datetime:
    value = str(value or "").strip()
    if not value:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


def _is_empty_value(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return True
        return cleaned.lower() in {"nan", "none", "null"}
    return False


def _normalize_make_key(value: str) -> str:
    value = re.sub(r"[^a-z0-9]+", " ", str(value or "").lower())
    return re.sub(r"\s+", " ", value).strip()


def _normalize_make(value):
    key = _normalize_make_key(value)
    if not key:
        return ""
    if key in BLOCKED_MAKE_KEYS:
        return ""
    if re.fullmatch(r"[a-z]?\d[\w-]*", key.replace(" ", "")):
        return ""
    return MAKE_LOOKUP.get(key, "")


def _canon_drive(value):
    cleaned = re.sub(r"\s+", " ", str(value or "").strip().lower().replace("-", " "))
    mapping = {
        "4x4": "4wd",
        "4wd": "4wd",
        "four wheel drive": "4wd",
        "awd": "awd",
        "all wheel drive": "awd",
        "fwd": "fwd",
        "front wheel drive": "fwd",
        "rwd": "rwd",
        "rear wheel drive": "rwd",
    }
    return mapping.get(cleaned, value)


def _normalize_record(record: Dict) -> Dict:
    normalized = dict(record)
    if not _is_empty_value(normalized.get("make")):
        normalized["make"] = _normalize_make(normalized.get("make"))
    if not _is_empty_value(normalized.get("drive_type")):
        normalized["drive_type"] = _canon_drive(normalized.get("drive_type"))
    return normalized


def _record_sort_key(record: Dict):
    run_id_dt = _run_id_to_dt(str(record.get("run_id", "")))
    scraped_at_dt = _parse_scraped_at(record.get("scraped_at", ""))
    return (run_id_dt, scraped_at_dt)


def _merge_post_records(records: list[Dict]) -> Dict:
    ordered = sorted((_normalize_record(record) for record in records), key=_record_sort_key, reverse=True)
    merged = dict(ordered[0])
    for column in BACKFILL_COLUMNS:
        if not _is_empty_value(merged.get(column)):
            continue
        for older in ordered[1:]:
            value = older.get(column)
            if _is_empty_value(value):
                continue
            merged[column] = value
            break
    return _normalize_record(merged)


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

        records_by_post: Dict[str, list[Dict]] = {}
        stats = {"files_scanned": 0, "invalid_records": 0}

        for run_id in run_ids:
            for record in _jsonl_records_for_run(BUCKET_NAME, STRUCTURED_PREFIX, run_id, stats):
                post_id = record.get("post_id")
                if not post_id:
                    continue

                records_by_post.setdefault(post_id, []).append(record)

        final_key = f"{STRUCTURED_PREFIX}/datasets/listings_master_v2.csv"
        ordered_records = [_merge_post_records(records_by_post[post_id]) for post_id in sorted(records_by_post)]
        rows_written = _write_csv(ordered_records, final_key)

        return jsonify({
            "ok": True,
            "runs_scanned": len(run_ids),
            "files_scanned": stats["files_scanned"],
            "invalid_records_skipped": stats["invalid_records"],
            "unique_listings": len(records_by_post),
            "rows_written": rows_written,
            "output_csv": f"gs://{BUCKET_NAME}/{final_key}",
            "min_run_id": min_run_id,
        }), 200
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"ok": False, "error": f"{type(exc).__name__}: {exc}"}), 500

# main.py
# Purpose: Convert raw TXT -> one-line JSON records (.jsonl) in GCS.
# Compatible input layouts:
#   gs://<bucket>/<SCRAPES_PREFIX>/<RUN>/*.txt
#   gs://<bucket>/<SCRAPES_PREFIX>/<RUN>/txt/*.txt
# where <RUN> is either 20251026T170002Z or 20251026170002.
# Output:
#   gs://<bucket>/<STRUCTURED_PREFIX>/run_id=<RUN>/jsonl/<post_id>.jsonl

import os
import re
import json
import logging
import traceback
from datetime import datetime, timezone

from flask import Request, jsonify
from google.api_core import retry as gax_retry
from google.cloud import storage

# -------------------- ENV --------------------
PROJECT_ID         = os.getenv("PROJECT_ID")
BUCKET_NAME        = os.getenv("GCS_BUCKET")                        # REQUIRED
SCRAPES_PREFIX     = os.getenv("SCRAPES_PREFIX", "scrapes")         # input
STRUCTURED_PREFIX  = os.getenv("STRUCTURED_PREFIX", "structured")   # output

# Accept BOTH run id styles:
RUN_ID_ISO_RE   = re.compile(r"^\d{8}T\d{6}Z$")  # 20251026T170002Z
RUN_ID_PLAIN_RE = re.compile(r"^\d{14}$")        # 20251026170002

READ_RETRY = gax_retry.Retry(
    predicate=gax_retry.if_transient_error,
    initial=1.0, maximum=10.0, multiplier=2.0, deadline=120.0
)

storage_client = storage.Client()

# -------------------- SIMPLE REGEX EXTRACTORS --------------------
PRICE_RE         = re.compile(r"\$\s?([0-9,]+)")
YEAR_RE          = re.compile(r"\b(?:19|20)\d{2}\b")
MAKE_MODEL_RE    = re.compile(r"\b([A-Z][a-z]+)\s+([A-Z][A-Za-z0-9]+)")
MILEAGE_RE       = re.compile(r"(?:mileage|odometer)\s*[:\-]?\s*([\d,]+)", re.I)
MILES_K_RE       = re.compile(r"(\d+(?:\.\d+)?)\s*k\s*(?:mi|mile|miles)\b", re.I)
MILES_RE         = re.compile(r"(\d{1,3}(?:[,\d]{3})*)\s*(?:mi|mile|miles)\b", re.I)
FIELD_LINE_RE    = re.compile(r"(?im)^\s*([a-z][a-z ]*[a-z])\s*:\s*(.+?)\s*$")
POST_ID_RE       = re.compile(r"(?im)^\s*post(?:ing)? id\s*:\s*(\d+)\s*$")
POSTED_AT_RE     = re.compile(r"(?im)^\s*posted\s*:\s*([^\n]+?)\s*$")
TITLE_NOISE_RE   = re.compile(r"^(?:qr code link to this post|craigslist(?:\s*-\s*.*)?)$", re.I)
TITLE_TOKEN_RE   = re.compile(r"[A-Za-z0-9][A-Za-z0-9&'/.-]*")
NON_WORD_RE      = re.compile(r"[^\w\s/-]+")

# -------------------- HELPERS --------------------
def _normalize_text(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\xa0", " ")
    return "\n".join(re.sub(r"[ \t]+", " ", line).strip() for line in text.splitlines())

def _to_int(value: str):
    try:
        return int(re.sub(r"[^\d]", "", value))
    except (TypeError, ValueError):
        return None

def _clean_label_value(value: str) -> str:
    value = NON_WORD_RE.sub(" ", value).strip().lower()
    return re.sub(r"\s+", " ", value)

def _extract_labeled_value(text: str, *labels: str):
    for label in labels:
        pattern = re.compile(rf"(?im)^\s*{re.escape(label)}\s*:\s*([^\n]+?)\s*$")
        m = pattern.search(text)
        if m:
            value = m.group(1).strip().strip("|").strip()
            if value:
                return value
    return None

def _format_title_token(token: str) -> str:
    if not token:
        return token
    if any(ch.isdigit() for ch in token):
        return token.upper() if token.isalpha() else token.title()
    return token[:1].upper() + token[1:].lower()

def _extract_title_line(text: str) -> str:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for line in lines[:10]:
        lower = line.lower()
        if TITLE_NOISE_RE.match(lower):
            continue
        if FIELD_LINE_RE.match(line):
            continue
        candidate = PRICE_RE.sub(" ", line)
        candidate = re.sub(r"\s+", " ", candidate).strip(" -|")
        if len(candidate) < 4:
            continue
        if YEAR_RE.search(candidate) or MAKE_MODEL_RE.search(candidate):
            return candidate
    for line in lines[:5]:
        if not FIELD_LINE_RE.match(line) and not TITLE_NOISE_RE.match(line.lower()):
            return re.sub(r"\s+", " ", PRICE_RE.sub(" ", line)).strip(" -|")
    return ""

def _extract_year_make_model(text: str) -> dict:
    title = _extract_title_line(text)
    parsed = {}

    if title:
        year_match = YEAR_RE.search(title)
        if year_match:
            year = _to_int(year_match.group(0))
            if year:
                parsed["year"] = year
            title = title[year_match.end():].strip(" -|")

        tokens = TITLE_TOKEN_RE.findall(title)
        tokens = [t for t in tokens if t.lower() not in {"for", "sale", "by", "owner", "dealer"}]
        if tokens:
            parsed["make"] = _format_title_token(tokens[0])
        if len(tokens) > 1:
            parsed["model"] = _format_title_token(tokens[1])

    if "year" not in parsed:
        for line in text.splitlines():
            lower = line.lower()
            if lower.startswith("posted:") or lower.startswith("post id:"):
                continue
            m = YEAR_RE.search(line)
            if m:
                year = _to_int(m.group(0))
                if year:
                    parsed["year"] = year
                    break

    if "make" not in parsed or "model" not in parsed:
        mm = MAKE_MODEL_RE.search(text)
        if mm:
            parsed.setdefault("make", mm.group(1))
            parsed.setdefault("model", mm.group(2))

    return parsed

def _canon_fuel(value: str) -> str:
    value = _clean_label_value(value)
    mapping = {
        "gas": "gasoline",
        "gasoline": "gasoline",
        "diesel": "diesel",
        "hybrid": "hybrid",
        "electric": "electric",
        "other": "other",
    }
    return mapping.get(value, value)

def _canon_drive(value: str) -> str:
    value = _clean_label_value(value).replace("-", " ")
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
    return mapping.get(value, value.replace(" ", ""))

def _canon_transmission(value: str) -> str:
    value = _clean_label_value(value)
    mapping = {
        "auto": "automatic",
        "automatic": "automatic",
        "manual": "manual",
        "stick": "manual",
        "cvt": "cvt",
        "other": "other",
    }
    return mapping.get(value, value)

def _list_run_ids(bucket: str, scrapes_prefix: str) -> list[str]:
    """
    List run folders under gs://bucket/<scrapes_prefix>/ and return normalized run_ids.
    Accept:
      - <scrapes_prefix>/run_id=20251026T170002Z/
      - <scrapes_prefix>/20251026170002/
    """
    it = storage_client.list_blobs(bucket, prefix=f"{scrapes_prefix}/", delimiter="/")
    for _ in it:
        pass  # populate it.prefixes

    run_ids: list[str] = []
    for pref in getattr(it, "prefixes", []):
        # e.g., 'scrapes/run_id=20251026T170002Z/' OR 'scrapes/20251026170002/'
        tail = pref.rstrip("/").split("/")[-1]
        cand = tail.split("run_id=", 1)[1] if tail.startswith("run_id=") else tail
        if RUN_ID_ISO_RE.match(cand) or RUN_ID_PLAIN_RE.match(cand):
            run_ids.append(cand)
    return sorted(run_ids)

def _txt_objects_for_run(run_id: str) -> list[str]:
    """
    Return .txt object names for a given run_id.
    Tries (in order) and returns the first non-empty list:
      scrapes/run_id=<run_id>/txt/
      scrapes/run_id=<run_id>/
      scrapes/<run_id>/txt/
      scrapes/<run_id>/
    """
    bucket = storage_client.bucket(BUCKET_NAME)
    candidates = [
        f"{SCRAPES_PREFIX}/run_id={run_id}/txt/",
        f"{SCRAPES_PREFIX}/run_id={run_id}/",
        f"{SCRAPES_PREFIX}/{run_id}/txt/",
        f"{SCRAPES_PREFIX}/{run_id}/",
    ]
    for pref in candidates:
        names = [b.name for b in bucket.list_blobs(prefix=pref) if b.name.endswith(".txt")]
        if names:
            return names
    return []

def _download_text(blob_name: str) -> str:
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    return blob.download_as_text(retry=READ_RETRY, timeout=120)

def _upload_jsonl_line(blob_name: str, record: dict):
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
    blob.upload_from_string(line, content_type="application/x-ndjson")

def _parse_run_id_as_iso(run_id: str) -> str:
    """Normalize either run_id style to ISO8601 Z (fallback = now UTC)."""
    try:
        if RUN_ID_ISO_RE.match(run_id):
            dt = datetime.strptime(run_id, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        elif RUN_ID_PLAIN_RE.match(run_id):
            dt = datetime.strptime(run_id, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        else:
            raise ValueError("unsupported run_id")
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

# -------------------- PARSE A LISTING --------------------
def parse_listing(text: str) -> dict:
    text = _normalize_text(text)
    d = {}

    m = PRICE_RE.search(text)
    if m:
        price = _to_int(m.group(1))
        if price is not None:
            d["price"] = price

    d.update(_extract_year_make_model(text))

    # mileage variants
    mi = None
    m1 = MILEAGE_RE.search(text)
    if m1:
        mi = _to_int(m1.group(1))
    if mi is None:
        m2 = MILES_K_RE.search(text)
        if m2:
            try:
                mi = int(float(m2.group(1)) * 1000)
            except ValueError:
                mi = None
    if mi is None:
        m3 = MILES_RE.search(text)
        if m3:
            mi = _to_int(m3.group(1))
    if mi is not None:
        d["mileage"] = mi

    condition = _extract_labeled_value(text, "condition")
    if condition:
        d["condition"] = _clean_label_value(condition)

    cylinders = _extract_labeled_value(text, "cylinders")
    if cylinders:
        cyl = _to_int(cylinders)
        if cyl is not None:
            d["cylinders"] = cyl

    drive_type = _extract_labeled_value(text, "drive")
    if drive_type:
        d["drive_type"] = _canon_drive(drive_type)

    fuel_type = _extract_labeled_value(text, "fuel")
    if fuel_type:
        d["fuel_type"] = _canon_fuel(fuel_type)

    paint_color = _extract_labeled_value(text, "paint color", "paint")
    if paint_color:
        d["paint_color"] = _clean_label_value(paint_color)

    title_status = _extract_labeled_value(text, "title status")
    if title_status:
        d["title_status"] = _clean_label_value(title_status)

    transmission = _extract_labeled_value(text, "transmission")
    if transmission:
        d["transmission"] = _canon_transmission(transmission)

    vehicle_type = _extract_labeled_value(text, "type", "vehicle type")
    if vehicle_type:
        d["vehicle_type"] = _clean_label_value(vehicle_type)

    post_id = _extract_labeled_value(text, "post id", "posting id")
    if post_id:
        post_id = _to_int(post_id)
        if post_id is not None:
            d["listing_post_id"] = str(post_id)
    else:
        m = POST_ID_RE.search(text)
        if m:
            d["listing_post_id"] = m.group(1)

    posted_at = _extract_labeled_value(text, "posted")
    if posted_at:
        d["listing_posted_at"] = posted_at.strip()
    else:
        m = POSTED_AT_RE.search(text)
        if m:
            d["listing_posted_at"] = m.group(1).strip()

    return d

# -------------------- HTTP ENTRY --------------------
def extract_http(request: Request):
    """
    Reads latest (or requested) run's TXT listings and writes ONE-LINE JSON records to:
      gs://<bucket>/<STRUCTURED_PREFIX>/run_id=<run_id>/jsonl/<post_id>.jsonl
    Request JSON (optional):
      { "run_id": "<...>", "max_files": 0, "overwrite": false }
    """
    logging.getLogger().setLevel(logging.INFO)

    if not BUCKET_NAME:
        return jsonify({"ok": False, "error": "missing GCS_BUCKET env"}), 500

    try:
        body = request.get_json(silent=True) or {}
    except Exception:
        body = {}

    run_id    = body.get("run_id")
    max_files = int(body.get("max_files") or 0)        # 0 = unlimited
    overwrite = bool(body.get("overwrite") or False)

    # Pick newest run if not provided
    if not run_id:
        runs = _list_run_ids(BUCKET_NAME, SCRAPES_PREFIX)
        if not runs:
            return jsonify({"ok": False, "error": f"no run_ids found under {SCRAPES_PREFIX}/"}), 200
        run_id = runs[-1]

    scraped_at_iso = _parse_run_id_as_iso(run_id)

    txt_blobs = _txt_objects_for_run(run_id)
    if not txt_blobs:
        return jsonify({"ok": False, "run_id": run_id, "error": "no .txt files found for run"}), 200
    if max_files > 0:
        txt_blobs = txt_blobs[:max_files]

    processed = written = skipped = errors = 0
    bucket = storage_client.bucket(BUCKET_NAME)

    for name in txt_blobs:
        try:
            text = _download_text(name)
            fields = parse_listing(text)

            post_id = os.path.splitext(os.path.basename(name))[0]
            record = {
                "post_id": post_id,
                "run_id": run_id,
                "scraped_at": scraped_at_iso,
                "source_txt": name,
                **fields,
            }

            out_key = f"{STRUCTURED_PREFIX}/run_id={run_id}/jsonl/{post_id}.jsonl"

            if not overwrite and bucket.blob(out_key).exists():
                skipped += 1
            else:
                _upload_jsonl_line(out_key, record)
                written += 1

        except Exception as e:
            errors += 1
            logging.error(f"Failed {name}: {e}\n{traceback.format_exc()}")

        processed += 1

    result = {
        "ok": True,
        "version": "extractor-v3-jsonl-flex",
        "run_id": run_id,
        "processed_txt": processed,
        "written_jsonl": written,
        "skipped_existing": skipped,
        "errors": errors
    }
    logging.info(json.dumps(result))
    return jsonify(result), 200

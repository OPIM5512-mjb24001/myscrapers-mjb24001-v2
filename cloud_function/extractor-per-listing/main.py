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
MILEAGE_RE       = re.compile(r"(?:mileage|odometer)\s*[:\-]?\s*([\d,]+)", re.I)
MILES_K_RE       = re.compile(r"(\d+(?:\.\d+)?)\s*k\s*(?:mi|mile|miles)\b", re.I)
MILES_RE         = re.compile(r"(\d{1,3}(?:[,\d]{3})*)\s*(?:mi|mile|miles)\b", re.I)
FIELD_LINE_RE    = re.compile(r"(?im)^\s*([a-z][a-z ]*[a-z])\s*:\s*(.+?)\s*$")
POST_ID_RE       = re.compile(r"(?im)^\s*post(?:ing)? id\s*:\s*(\d+)\s*$")
POSTED_AT_RE     = re.compile(r"(?im)^\s*posted\s*:\s*([^\n]+?)\s*$")
TITLE_YEAR_RE    = re.compile(r"^\s*((?:19|20)\d{2})\b\s*(.+?)\s*$", re.I)
TITLE_TOKEN_RE   = re.compile(r"[A-Za-z0-9][A-Za-z0-9&'/.-]*")
NON_WORD_RE      = re.compile(r"[^\w\s/-]+")
KNOWN_MAKES = {
    "Ford", "Chevrolet", "Chevy", "GMC", "Dodge", "RAM", "Jeep", "Toyota",
    "Honda", "Nissan", "Hyundai", "Kia", "Subaru", "Volkswagen", "VW",
    "Audi", "BMW", "Acura", "Lexus", "Mazda", "Mercedes-Benz", "Mercedes",
    "Volvo", "Jaguar", "Porsche", "Buick", "Cadillac", "Lincoln",
    "Chrysler", "Infiniti", "Mitsubishi", "Mini",
}
MAKE_ALIASES = {
    "acura": "Acura",
    "audi": "Audi",
    "bmw": "BMW",
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
    "jaguar": "Jaguar",
    "jeep": "Jeep",
    "kia": "Kia",
    "lexus": "Lexus",
    "lincoln": "Lincoln",
    "mazda": "Mazda",
    "mercedes": "Mercedes",
    "mercedes-benz": "Mercedes-Benz",
    "mini": "Mini",
    "mitsubishi": "Mitsubishi",
    "nissan": "Nissan",
    "porsche": "Porsche",
    "ram": "RAM",
    "subaru": "Subaru",
    "toyota": "Toyota",
    "volkswagen": "Volkswagen",
    "volvo": "Volvo",
    "vw": "Volkswagen",
}
KNOWN_MAKE_LOOKUP = {make.lower(): MAKE_ALIASES.get(make.lower(), make) for make in KNOWN_MAKES}
TITLE_NOISE_BITS = (
    "contact information",
    "qr code link to this post",
    "more ads by this seller",
    "more ads by this user",
    "google map",
    "reply",
    "favorite",
    "flag",
    "print",
    "craigslist",
    "dealer",
    "account",
    "hidden",
    "favorites",
    "post to classifieds",
    "showing",
    "availability",
    "delivery available",
    "save this search",
    "new search",
    "refresh the page",
    "help / faq",
    "avoid scams",
    "no image",
)
BAD_TITLE_TOKENS = {
    "account", "ads", "by", "code", "contact", "craigslist", "dealer",
    "favorite", "favorites", "flag", "google", "hidden", "information",
    "link", "map", "more", "post", "print", "qr", "reply", "seller",
    "this", "to", "user",
}
TITLE_STOP_WORDS = {
    "clean", "rebuilt", "salvage", "title", "financing", "warranty",
    "available", "today", "trade", "trades", "obo", "firm",
}
TITLE_TRIM_WORDS = {
    "base", "custom", "denali", "edition", "ex", "fx4", "lariat", "laramie",
    "le", "limited", "lt", "ltz", "lx", "platinum", "premium", "se", "sel",
    "slt", "sport", "touring", "xl", "xle", "xlt",
}
MODEL_NEEDS_PARTNER = {"grand", "super", "town"}
MODEL_PAIR_WORDS = {
    ("grand", "caravan"),
    ("grand", "cherokee"),
    ("super", "duty"),
}
MODEL_THREE_WORDS = {
    ("town", "and", "country"),
}
NUMERIC_MODEL_PARTNERS = {"crew", "cab"}

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

def _clean_title_line(line: str) -> str:
    line = PRICE_RE.sub(" ", line)
    line = re.sub(r"\s+[|/]\s+", " ", line)
    line = re.sub(r"\s+", " ", line)
    return line.strip(" -|")

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
    if token.upper() in {"GMC", "BMW", "RAM", "VW", "AWD", "FWD", "RWD", "4WD", "4X4"}:
        return token.upper()
    if token.isupper() and len(token) <= 4:
        return token
    if any(ch.isdigit() for ch in token):
        return token.upper() if token == token.upper() else token[:1].upper() + token[1:]
    return token[:1].upper() + token[1:].lower()

def _is_noise_title_line(line: str) -> bool:
    line = _clean_title_line(line)
    if not line or len(line) < 6:
        return True
    if FIELD_LINE_RE.match(line):
        return True

    lower = line.lower()
    if ":" in line and not lower.startswith(("rebuilt ", "salvage ", "clean ")):
        return True
    if any(bit in lower for bit in TITLE_NOISE_BITS):
        return True
    if lower.startswith(("http", "www.", "reply ", "favorite ", "flag ", "print ")):
        return True
    if lower.startswith(("contact ", "qr code ", "more ads ", "google map", "craigslist ")):
        return True
    if lower in {"google map", "contact information", "reply", "favorite", "flag", "print"}:
        return True
    return False

def _normalize_make(raw_make: str) -> str:
    return KNOWN_MAKE_LOOKUP.get(raw_make.strip().lower(), "")

def _extract_make_tokens(tokens: list[str]):
    for size in (2, 1):
        if len(tokens) < size:
            continue
        raw_make = " ".join(tokens[:size])
        make = _normalize_make(raw_make)
        if make:
            return make, tokens[size:]
    return "", tokens

def _format_model_tokens(tokens: list[str]) -> str:
    return " ".join(_format_title_token(token) for token in tokens)

def _extract_model_tokens(tokens: list[str]) -> list[str]:
    kept: list[str] = []
    for token in tokens:
        lower = token.lower()
        if lower in BAD_TITLE_TOKENS:
            break
        if lower in TITLE_STOP_WORDS and kept:
            break
        kept.append(token)
        if len(kept) >= 4:
            break

    if not kept:
        return []

    lower_tokens = [token.lower() for token in kept]

    if tuple(lower_tokens[:3]) in MODEL_THREE_WORDS:
        return kept[:3]
    if tuple(lower_tokens[:2]) in MODEL_PAIR_WORDS:
        return kept[:2]
    if kept[0].isdigit() and len(kept) >= 2 and lower_tokens[1] in NUMERIC_MODEL_PARTNERS:
        return kept[:2]

    while len(kept) > 1 and kept[-1].lower() in TITLE_TRIM_WORDS:
        kept.pop()

    if not kept:
        return []

    if len(kept) == 1:
        only = kept[0].lower()
        if only in MODEL_NEEDS_PARTNER or only in TITLE_TRIM_WORDS:
            return []
        if _normalize_make(kept[0]):
            return []
        return kept

    if kept[0].lower() in MODEL_NEEDS_PARTNER:
        return kept[:2]

    return kept[:3]

def _extract_year_make_model(text: str) -> dict:
    parsed = {}
    lines = [line for line in text.splitlines() if line.strip()]

    # Make/model extraction is intentionally conservative so we leave blanks
    # rather than writing obviously wrong vehicle values into the CSV.
    for line in lines[:60]:
        if _is_noise_title_line(line):
            continue

        candidate = _clean_title_line(line)
        m = TITLE_YEAR_RE.match(candidate)
        if not m:
            continue

        year = _to_int(m.group(1))
        if not year:
            continue
        remainder = m.group(2).strip()
        if not remainder:
            continue

        tokens = TITLE_TOKEN_RE.findall(remainder)
        if len(tokens) < 2 or len(tokens) > 8:
            if "year" not in parsed:
                parsed["year"] = year
            continue

        make, model_tokens = _extract_make_tokens(tokens)
        if not make:
            if "year" not in parsed:
                parsed["year"] = year
            continue

        model_tokens = _extract_model_tokens(model_tokens)
        parsed["year"] = year
        parsed["make"] = make
        if model_tokens:
            parsed["model"] = _format_model_tokens(model_tokens)
        return parsed

    for line in lines[:60]:
        if _is_noise_title_line(line):
            continue
        candidate = _clean_title_line(line)
        m = TITLE_YEAR_RE.match(candidate) or YEAR_RE.search(candidate)
        if m:
            year = _to_int(m.group(1) if m.lastindex else m.group(0))
            if year:
                parsed["year"] = year
                break

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

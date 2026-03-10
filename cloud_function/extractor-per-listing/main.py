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
MAKE_MODEL_RE    = re.compile(r"\b([A-Z][a-z]+)\s+([A-Z][A-Za-z0-9-]+)")
ODOMETER_RE      = re.compile(r"(?:mileage|odometer)\s*[:\-]?\s*([\d,]+)", re.I)
K_MILES_RE       = re.compile(r"(\d+(?:\.\d+)?)\s*k\s*(?:mi|mile|miles)\b", re.I)
MILES_RE         = re.compile(r"(\d{1,3}(?:[,\d]{3})*)\s*(?:mi|mile|miles)\b", re.I)
CONDITION_RE     = re.compile(r"\bcondition\s*:\s*([^\n\r|]+)", re.I)
CYLINDERS_RE     = re.compile(r"\bcylinders?\s*:\s*([^\n\r|]+)", re.I)
DRIVE_RE         = re.compile(r"\bdrive\s*:\s*([^\n\r|]+)", re.I)
FUEL_RE          = re.compile(r"\bfuel\s*:\s*([^\n\r|]+)", re.I)
PAINT_COLOR_RE   = re.compile(r"\bpaint\s+color\s*:\s*([^\n\r|]+)", re.I)
TITLE_STATUS_RE  = re.compile(r"\btitle\s+status\s*:\s*([^\n\r|]+)", re.I)
TRANSMISSION_RE  = re.compile(r"\btrans(?:mission)?\s*:\s*([^\n\r|]+)", re.I)
VEHICLE_TYPE_RE  = re.compile(r"\b(?:vehicle\s+)?type\s*:\s*([^\n\r|]+)", re.I)
POST_ID_RE       = re.compile(r"\bpost(?:ing)?\s+id\s*:\s*([0-9]+)\b", re.I)
POSTED_RE        = re.compile(r"\bposted\s*:\s*([^\n\r|]+)", re.I)
NEXT_LABEL_RE    = re.compile(
    r"\s+(?=(?:condition|cylinders?|drive|fuel|paint color|title status|trans(?:mission)?|"
    r"(?:vehicle\s+)?type|post(?:ing)? id|posted|odometer|mileage)\s*:)",
    re.I,
)
TITLE_JUNK_RE    = re.compile(
    r"\s+-\s+(?:cars.*|trucks.*|by owner.*|by dealer.*|vehicle.*|automotive.*|craigslist.*)$",
    re.I,
)

MULTI_WORD_MAKES = {
    ("alfa", "romeo"),
    ("aston", "martin"),
    ("land", "rover"),
    ("rolls", "royce"),
}

TITLE_STOP_WORDS = {
    "clean", "rebuilt", "salvage", "runs", "run", "drives", "excellent", "good",
    "fair", "automatic", "manual", "gas", "gasoline", "diesel", "hybrid",
    "electric", "awd", "fwd", "rwd", "4wd", "4x4", "miles", "mile", "mi",
    "title", "condition", "obo", "firm",
}

# -------------------- HELPERS --------------------
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

def _normalize_text(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\xa0", " ").replace("\u200b", "")
    return "\n".join(line.rstrip() for line in text.splitlines())

def _clean_value(value: str) -> str:
    value = NEXT_LABEL_RE.split(value, maxsplit=1)[0]
    value = re.sub(r"\s+", " ", value)
    return value.strip(" \t-:|")

def _to_int(value: str):
    if not value:
        return None
    digits = re.sub(r"[^\d]", "", value)
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None

def _title_case(value: str) -> str:
    parts = []
    for part in value.split():
        lower = part.lower()
        if lower in {"bmw", "gmc", "mg"}:
            parts.append(part.upper())
        elif any(ch.isdigit() for ch in part):
            parts.append(part.upper())
        else:
            parts.append(part.title())
    return " ".join(parts)

def _extract_label_value(pattern, text: str):
    m = pattern.search(text)
    if not m:
        return None
    value = _clean_value(m.group(1))
    return value or None

def _canon_fuel_type(value: str):
    if not value:
        return None
    value = value.lower().strip()
    if value == "gas":
        return "gasoline"
    if "gasoline" in value:
        return "gasoline"
    if "plug-in" in value or "plugin" in value:
        return "plug-in hybrid"
    if "flex" in value:
        return "flex fuel"
    if "diesel" in value:
        return "diesel"
    if "hybrid" in value:
        return "hybrid"
    if "electric" in value or value == "ev":
        return "electric"
    return value

def _canon_drive_type(value: str):
    if not value:
        return None
    value = value.lower().strip()
    if value == "4x4" or "4 wheel drive" in value or "four wheel drive" in value or value == "4wd":
        return "4wd"
    if "all wheel" in value or value == "awd":
        return "awd"
    if "front wheel" in value or value == "fwd":
        return "fwd"
    if "rear wheel" in value or value == "rwd":
        return "rwd"
    return value

def _canon_transmission(value: str):
    if not value:
        return None
    value = value.lower().strip()
    if value in {"auto", "a/t", "at"} or "automatic" in value:
        return "automatic"
    if value in {"manual", "m/t", "mt", "std"} or "manual" in value or "stick" in value or "standard" in value:
        return "manual"
    return value

def _extract_title_line(text: str):
    label_names = {
        "condition", "cylinders", "drive", "fuel", "odometer", "mileage",
        "paint color", "title status", "transmission", "trans", "type",
        "vehicle type", "post id", "posting id", "posted",
    }
    for raw_line in text.splitlines():
        line = _clean_value(raw_line)
        if not line or len(line) > 180:
            continue
        if ":" in line:
            left = line.split(":", 1)[0].strip().lower()
            if left in label_names:
                continue
        if not YEAR_RE.search(line):
            continue
        line = re.sub(r"^\$\s*[0-9,]+\s*", "", line)
        line = TITLE_JUNK_RE.sub("", line)
        line = re.split(r"\s+\|\s+", line, maxsplit=1)[0]
        line = _clean_value(line)
        if YEAR_RE.search(line):
            return line
    return None

def _extract_year_make_model(text: str) -> dict:
    line = _extract_title_line(text)
    if not line:
        return {}

    m = re.search(r"\b((?:19|20)\d{2})\b\s+(.+)$", line)
    if not m:
        return {}

    out = {}
    year = _to_int(m.group(1))
    if year is not None:
        out["year"] = year

    rest = _clean_value(m.group(2))
    tokens = re.findall(r"[A-Za-z0-9]+(?:[-/&][A-Za-z0-9]+)*", rest)
    if not tokens:
        return out

    if len(tokens) >= 2 and (tokens[0].lower(), tokens[1].lower()) in MULTI_WORD_MAKES:
        make_tokens = tokens[:2]
        model_tokens = tokens[2:]
    else:
        make_tokens = tokens[:1]
        model_tokens = tokens[1:]

    if make_tokens:
        out["make"] = _title_case(" ".join(make_tokens))

    kept = []
    for token in model_tokens:
        low = token.lower()
        if kept and low in TITLE_STOP_WORDS:
            break
        kept.append(token)
        if len(kept) >= 4:
            break
    if kept:
        out["model"] = _title_case(" ".join(kept))

    return out

# -------------------- PARSE A LISTING --------------------
def parse_listing(text: str) -> dict:
    text = _normalize_text(text)
    d = {}

    m = PRICE_RE.search(text)
    if m:
        try:
            d["price"] = int(m.group(1).replace(",", ""))
        except ValueError:
            pass

    d.update(_extract_year_make_model(text))

    if "year" not in d:
        y = YEAR_RE.search(text)
        if y:
            try:
                d["year"] = int(y.group(0))
            except ValueError:
                pass

    if "make" not in d or "model" not in d:
        mm = MAKE_MODEL_RE.search(text)
        if mm:
            d.setdefault("make", _title_case(mm.group(1)))
            d.setdefault("model", _title_case(mm.group(2)))

    # mileage variants
    mi = None
    m1 = ODOMETER_RE.search(text)
    if m1:
        try:
            mi = int(m1.group(1).replace(",", ""))
        except ValueError:
            mi = None
    if mi is None:
        m2 = K_MILES_RE.search(text)
        if m2:
            try:
                mi = int(float(m2.group(1)) * 1000)
            except ValueError:
                mi = None
    if mi is None:
        m3 = MILES_RE.search(text)
        if m3:
            try:
                mi = int(re.sub(r"[^\d]", "", m3.group(1)))
            except ValueError:
                mi = None
    if mi is not None:
        d["mileage"] = mi

    condition = _extract_label_value(CONDITION_RE, text)
    if condition:
        d["condition"] = condition.lower()

    cylinders = _extract_label_value(CYLINDERS_RE, text)
    cyl_int = _to_int(cylinders)
    if cyl_int is not None:
        d["cylinders"] = cyl_int

    drive = _canon_drive_type(_extract_label_value(DRIVE_RE, text))
    if drive:
        d["drive_type"] = drive

    fuel = _canon_fuel_type(_extract_label_value(FUEL_RE, text))
    if fuel:
        d["fuel_type"] = fuel

    paint_color = _extract_label_value(PAINT_COLOR_RE, text)
    if paint_color:
        d["paint_color"] = paint_color.lower()

    title_status = _extract_label_value(TITLE_STATUS_RE, text)
    if title_status:
        d["title_status"] = title_status.lower()

    transmission = _canon_transmission(_extract_label_value(TRANSMISSION_RE, text))
    if transmission:
        d["transmission"] = transmission

    vehicle_type = _extract_label_value(VEHICLE_TYPE_RE, text)
    if vehicle_type:
        d["vehicle_type"] = vehicle_type.lower()

    post_id = _extract_label_value(POST_ID_RE, text)
    if post_id:
        d["listing_post_id"] = post_id

    posted = _extract_label_value(POSTED_RE, text)
    if posted:
        d["listing_posted_at"] = posted

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

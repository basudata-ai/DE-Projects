"""
Batch Ingestion Script: Public API -> MinIO (Data Lake Landing Zone)
=====================================================================

Source : Open-Meteo API (free, no API key required)
Target : MinIO (S3-compatible object storage)
Pattern: Batch ingestion with date-based partitioning + idempotent writes

Author : <your name>
Purpose: Portfolio demo — data engineering ingestion layer
"""

import io
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import requests
from minio import Minio
from minio.error import S3Error

# ---------------------------------------------------------------------------
# 1. CONFIGURATION
# ---------------------------------------------------------------------------
# Keep config out of code (env vars / .env file) — never hardcode secrets.
# For local dev you can `export` these or use a .env + python-dotenv.

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE     = os.getenv("MINIO_SECURE", "false").lower() == "true"
BUCKET_NAME      = os.getenv("MINIO_BUCKET", "raw-landing-zone")

# API + ingestion parameters (Bengaluru coords as default example)
API_BASE_URL = "https://api.open-meteo.com/v1/forecast"
LATITUDE     = os.getenv("INGEST_LAT", "12.9716")
LONGITUDE    = os.getenv("INGEST_LON", "77.5946")
LOCATION_TAG = os.getenv("INGEST_LOCATION", "bengaluru")

MAX_RETRIES     = 3
RETRY_BACKOFF_S = 2  # exponential: 2s, 4s, 8s

# ---------------------------------------------------------------------------
# 2. LOGGING
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("ingestion")


# ---------------------------------------------------------------------------
# 3. EXTRACT — pull data from the public API with retry/backoff
# ---------------------------------------------------------------------------
def fetch_weather_data(lat: str, lon: str) -> dict:
    """
    Calls the Open-Meteo API and returns the raw JSON payload.
    Retries transient failures with exponential backoff — a batch job
    should never die on a single flaky network call.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "timezone": "auto",
    }

    last_exception = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"Fetching data from API (attempt {attempt}/{MAX_RETRIES})...")
            response = requests.get(API_BASE_URL, params=params, timeout=15)
            response.raise_for_status()
            logger.info("API call successful.")
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_exception = exc
            wait = RETRY_BACKOFF_S ** attempt
            logger.warning(f"Attempt {attempt} failed: {exc}. Retrying in {wait}s...")
            time.sleep(wait)

    logger.error("All retry attempts exhausted. Aborting ingestion run.")
    raise RuntimeError(f"Failed to fetch API data: {last_exception}")


# ---------------------------------------------------------------------------
# 4. TRANSFORM (light) — wrap raw payload with ingestion metadata
# ---------------------------------------------------------------------------
def enrich_with_metadata(raw_payload: dict, ingestion_ts: datetime) -> dict:
    """
    Batch ingestion best practice: never store raw source data alone.
    Attach metadata so every landed record is traceable — when it was
    pulled, from where, and by which run. This is critical for
    debugging, lineage, and reprocessing decisions later.
    """
    return {
        "metadata": {
            "source": "open-meteo-api",
            "ingestion_timestamp_utc": ingestion_ts.isoformat(),
            "location_tag": LOCATION_TAG,
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "ingestion_type": "batch",
        },
        "data": raw_payload,
    }


# ---------------------------------------------------------------------------
# 5. LOAD — land the file in MinIO with partitioned, idempotent object keys
# ---------------------------------------------------------------------------
def get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def ensure_bucket_exists(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        logger.info(f"Bucket '{bucket}' not found. Creating it.")
        client.make_bucket(bucket)
    else:
        logger.info(f"Bucket '{bucket}' already exists.")


def build_object_key(ingestion_ts: datetime) -> str:
    """
    Partitioning scheme: source/location/year/month/day/hour/filename
    This mirrors how Hive-style partitioning works in a lake, and lets
    Trino/Spark later query only the partitions they need instead of
    scanning everything — a core reason batch lakes are partitioned
    by ingestion date/time.
    """
    return (
        f"weather/{LOCATION_TAG}/"
        f"year={ingestion_ts.year:04d}/"
        f"month={ingestion_ts.month:02d}/"
        f"day={ingestion_ts.day:02d}/"
        f"hour={ingestion_ts.hour:02d}/"
        f"weather_{ingestion_ts.strftime('%Y%m%dT%H%M%S')}.json"
    )


def land_to_minio(client: Minio, bucket: str, object_key: str, payload: dict) -> None:
    """
    Idempotency check: if this exact object already exists, skip the
    write rather than duplicating data. Batch jobs get re-run (retries,
    backfills, manual reruns) — idempotent writes prevent duplicate
    landing zone files from the same batch window.
    """
    try:
        client.stat_object(bucket, object_key)
        logger.info(f"Object already exists, skipping write: {object_key}")
        return
    except S3Error as exc:
        if exc.code != "NoSuchKey":
            raise  # unexpected error — don't swallow it

    body = json.dumps(payload, indent=2).encode("utf-8")
    client.put_object(
        bucket_name=bucket,
        object_name=object_key,
        data=io.BytesIO(body),
        length=len(body),
        content_type="application/json",
    )
    logger.info(f"Landed object: s3://{bucket}/{object_key} ({len(body)} bytes)")


# ---------------------------------------------------------------------------
# 6. ORCHESTRATION — the batch run
# ---------------------------------------------------------------------------
def run_batch_ingestion() -> None:
    ingestion_ts = datetime.now(timezone.utc)
    logger.info(f"=== Starting batch ingestion run: {ingestion_ts.isoformat()} ===")

    # Extract
    raw_data = fetch_weather_data(LATITUDE, LONGITUDE)

    # Transform (metadata wrap only — raw landing zone keeps data as-is)
    enriched_payload = enrich_with_metadata(raw_data, ingestion_ts)

    # Load
    client = get_minio_client()
    ensure_bucket_exists(client, BUCKET_NAME)
    object_key = build_object_key(ingestion_ts)
    land_to_minio(client, BUCKET_NAME, object_key, enriched_payload)

    logger.info("=== Batch ingestion run completed successfully ===")


if __name__ == "__main__":
    try:
        run_batch_ingestion()
    except Exception as e:
        logger.exception(f"Batch ingestion run FAILED: {e}")
        sys.exit(1)

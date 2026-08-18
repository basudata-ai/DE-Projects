# Batch Ingestion: Public API → MinIO

A minimal but production-shaped batch ingestion pipeline. Pulls weather data
from the Open-Meteo public API (no key needed) and lands it in MinIO as
partitioned JSON — the raw/landing layer of a data lake.

## 1. Setup

```bash
python -m venv venv
source venv/bin/activate        # on Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## 2. Start MinIO (if not already running via your Docker Compose project)

```bash
docker run -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  minio/minio server /data --console-address ":9001"
```

Console: http://localhost:9001 (login: minioadmin / minioadmin)

## 3. Configure environment variables (optional — defaults work for local dev)

```bash
export MINIO_ENDPOINT=localhost:9000
export MINIO_ACCESS_KEY=minioadmin
export MINIO_SECRET_KEY=minioadmin
export MINIO_BUCKET=raw-landing-zone
export INGEST_LAT=12.9716
export INGEST_LON=77.5946
export INGEST_LOCATION=bengaluru
```

## 4. Run the ingestion job

```bash
python ingest_weather_to_minio.py
```

You should see log output confirming the API call, bucket check, and the
final object landed at a path like:

```
weather/bengaluru/year=2026/month=07/day=13/hour=10/weather_20260713T100512.json
```

## 5. Schedule it (turning this into a real batch pipeline)

For a portfolio project, show this running on a schedule instead of manually:

- **Cron** (simplest): `0 * * * * /path/to/venv/bin/python /path/to/ingest_weather_to_minio.py`
- **Airflow** (more impressive for a DE portfolio): wrap `run_batch_ingestion()`
  in a PythonOperator inside a DAG that runs hourly, with retries and alerting
  configured on the task itself.

## Design notes (why it's built this way)

| Concept | Where it shows up |
|---|---|
| **Partitioning** | Object keys are partitioned by `year/month/day/hour`, so Trino/Spark can prune partitions instead of scanning everything |
| **Idempotency** | `stat_object` check before write — safe to re-run the same batch window without duplicating data |
| **Retry/backoff** | `fetch_weather_data` retries transient API failures with exponential backoff |
| **Metadata lineage** | Every landed file is wrapped with `metadata` (source, timestamp, location) separate from the raw `data` |
| **Separation of concerns** | Extract / Transform / Load are separate functions — easy to test, easy to swap (e.g., different API, different sink) |
| **Config via env vars** | No secrets hardcoded — mirrors how you'd deploy this in Airflow/Docker with env injection |

## Next steps to extend this for your project

1. Add a second ingestion script for a different source (e.g., a CSV drop or another API) to show you can handle heterogeneous sources.
2. Convert the landing format from JSON to **Parquet** (via `pandas`/`pyarrow`) — more realistic for a lake and directly queryable by Trino.
3. Register the MinIO path as an external table in Trino/Hive Metastore so it's queryable end-to-end.
4. Add a simple **data quality check** post-load (e.g., row count > 0, expected fields present) before marking the batch "successful."

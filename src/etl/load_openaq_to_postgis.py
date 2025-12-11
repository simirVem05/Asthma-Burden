import os
import re
from typing import List, Tuple
import pandas as pd
import psycopg2
import psycopg2.extras
import pyarrow.parquet as pq
from dotenv import load_dotenv
PARQUET_PATH = "data/raw/openaq_bulk_filtered/filtered_openaq.parquet"
PARQUET_PATH = "data/raw/openaq_bulk_filtered/filtered_openaq.parquet"
BATCH_SIZE = 5000
load_dotenv()
def slugify_location(loc: str) -> str:
    s = re.sub(r"[^A-Za-z0-9]+", "_", loc).strip("_")
    return f"OAQ_{s}" if s else "OAQ_unknown"
def get_conn():
    required = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        raise SystemExit(f"missing env vars: {', '.join(missing)}")
    return psycopg2.connect(
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT"),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
    )
def upsert_monitors(cur, monitor_rows: List[Tuple[str, str, float, float]]):
    if not monitor_rows:
        return
    sql = """
        INSERT INTO pollution_monitors (monitor_id, name, source, sensor_type, geom)
        VALUES %s
        ON CONFLICT (monitor_id) DO NOTHING
    """
    template = (
        "(%s, %s, 'OpenAQ', 'pm25/no2 bulk', ST_SetSRID(ST_MakePoint(%s, %s), 4326))"
    )
    psycopg2.extras.execute_values(
        cur, sql, monitor_rows, template=template, page_size=1000
    )
def insert_readings(cur, reading_rows: List[Tuple[str, str, float, str]]):
    if not reading_rows:
        return
    sql = """
        INSERT INTO pollution_readings (monitor_id, timestamp, pollutant, value, unit)
        VALUES %s
        ON CONFLICT ON CONSTRAINT unique_reading DO NOTHING
    """
    template = "(%s, %s, %s, %s, %s)"
    psycopg2.extras.execute_values(
        cur, sql, reading_rows, template=template, page_size=2000
    )
def process_parquet(path: str):
    pf = pq.ParquetFile(path)
    with get_conn() as conn:
        with conn.cursor() as cur:
            for batch in pf.iter_batches(batch_size=BATCH_SIZE):
                df = batch.to_pandas()
                monitors = {}
                for loc, lon, lat in zip(
                    df["location"], df["longitude"], df["latitude"]
                ):
                    if pd.isna(loc) or pd.isna(lon) or pd.isna(lat):
                        continue
                    mid = slugify_location(str(loc))
                    monitors[mid] = (mid, str(loc), float(lon), float(lat))
                upsert_monitors(cur, list(monitors.values()))
                readings = []
                for _, row in df.iterrows():
                    loc = row.get("location")
                    if pd.isna(loc):
                        continue
                    mid = slugify_location(str(loc))
                    ts = row.get("timestamp_utc")
                    param = row.get("parameter")
                    val = row.get("value")
                    if pd.isna(ts) or pd.isna(param) or pd.isna(val):
                        continue
                    readings.append(
                        (mid, ts.to_pydatetime(), str(param), float(val), None)
                    )
            conn.commit()
if __name__ == "__main__":
    import pandas as pd
    if not os.path.exists(PARQUET_PATH):
        raise SystemExit(f"parquet not found: {PARQUET_PATH}")
    process_parquet(PARQUET_PATH)
    print("load complete")

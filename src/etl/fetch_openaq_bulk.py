import os
import tempfile
from typing import Iterable, List, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
class Config:
    BULK_URLS: List[str] = []
    OUTPUT_DIR = "data/raw/openaq_bulk_filtered"
    OUTPUT_FILE = os.path.join(OUTPUT_DIR, "filtered_openaq.parquet")
    POLLUTANTS = {"pm25", "no2"}
    COUNTRY = "US"
    BBOX = {
        "lat_min": 38.5,
        "lat_max": 42.3,
        "lon_min": -75.5,
        "lon_max": -71.5,
    }
    START_DATE = "2019-01-01"
    END_DATE = "2024-12-31"
    BATCH_SIZE = 50_000
def _first_present(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None
def _download_to_temp(url: str) -> str:
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    fd, tmp_path = tempfile.mkstemp(suffix=os.path.splitext(url)[1] or ".parquet")
    with os.fdopen(fd, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
    return tmp_path
def _filter_batch(df: pd.DataFrame) -> pd.DataFrame:
    param_col = _first_present(df, ["parameter", "pollutant"])
    country_col = _first_present(df, ["country"])
    lat_col = _first_present(df, ["latitude", "coordinates.latitude", "lat"])
    lon_col = _first_present(df, ["longitude", "coordinates.longitude", "lon", "lng"])
    ts_col = _first_present(
        df, ["date_utc", "utc", "timestamp", "datetime", "date.utc"]
    )
    if param_col is None or lat_col is None or lon_col is None or ts_col is None:
        return pd.DataFrame()
    df = df.copy()
    df[param_col] = df[param_col].astype(str).str.lower()
    df = df[df[param_col].isin(Config.POLLUTANTS)]
    if country_col:
        df = df[df[country_col].astype(str).str.upper() == Config.COUNTRY]
    if df.empty:
        return df
    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
    start = pd.to_datetime(Config.START_DATE, utc=True)
    end = (
        pd.to_datetime(Config.END_DATE, utc=True)
        + pd.Timedelta(days=1)
        - pd.Timedelta(seconds=1)
    )
    df = df[(df[ts_col] >= start) & (df[ts_col] <= end)]
    df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
    df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")
    bbox = Config.BBOX
    df = df[
        (df[lat_col] >= bbox["lat_min"])
        & (df[lat_col] <= bbox["lat_max"])
        & (df[lon_col] >= bbox["lon_min"])
        & (df[lon_col] <= bbox["lon_max"])
    ]
    if df.empty:
        return df
    output_cols = {
        "parameter": param_col,
        "value": _first_present(df, ["value"]),
        "unit": _first_present(df, ["unit"]),
        "country": country_col,
        "latitude": lat_col,
        "longitude": lon_col,
        "timestamp_utc": ts_col,
        "location": _first_present(df, ["location"]),
        "city": _first_present(df, ["city"]),
        "sourceName": _first_present(df, ["sourceName", "source"]),
    }
    selected = {k: df[v] for k, v in output_cols.items() if v}
    return pd.DataFrame(selected)
def process_parquet_file(
    path: str, writer: Optional[pq.ParquetWriter]
) -> pq.ParquetWriter:
    pf = pq.ParquetFile(path)
    for batch in pf.iter_batches(batch_size=Config.BATCH_SIZE):
        df = batch.to_pandas()
        filtered = _filter_batch(df)
        if filtered.empty:
            continue
        table = pa.Table.from_pandas(filtered, preserve_index=False)
        if writer is None:
            os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
            writer = pq.ParquetWriter(Config.OUTPUT_FILE, table.schema)
        writer.write_table(table)
    return writer
def run():
    if not Config.BULK_URLS:
        print("Config.BULK_URLS is empty. Add bulk parquet URLs and rerun.")
        return
    writer = None
    try:
        for url in Config.BULK_URLS:
            print(f"Downloading bulk file: {url}")
            tmp_path = _download_to_temp(url)
            try:
                writer = process_parquet_file(tmp_path, writer)
            finally:
                os.remove(tmp_path)
    finally:
        if writer is not None:
            writer.close()
    if writer is None:
        print("No data matched filters; no parquet written.")
    else:
        print(f"Wrote filtered data to {Config.OUTPUT_FILE}")
if __name__ == "__main__":
    run()

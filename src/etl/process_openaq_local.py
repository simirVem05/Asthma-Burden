import glob
import os
from typing import Iterable, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
class Config:
    INPUT_ROOT = "data/raw/openaq_bulk"
    OUTPUT_DIR = "data/raw/openaq_bulk_filtered"
    OUTPUT_FILE = os.path.join(OUTPUT_DIR, "filtered_openaq.parquet")
    POLLUTANTS = {"pm25", "no2"}
    COUNTRY = "US"
    BBOX = {"lat_min": 38.5, "lat_max": 42.3, "lon_min": -75.5, "lon_max": -71.5}
    START_DATE = "2019-01-01"
    END_DATE = "2024-12-31"
    CHUNK_ROWS = 200_000
def _first_present(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None
def _filter_chunk(df: pd.DataFrame) -> pd.DataFrame:
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
def process_all():
    pattern = os.path.join(
        Config.INPUT_ROOT, "locationid=*", "year=*", "month=*", "*.csv.gz"
    )
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"no files found under {Config.INPUT_ROOT}, check if you synced from S3")
        return
    writer = None
    os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
    for path in files:
        print(f"processing {path}")
        for chunk in pd.read_csv(path, compression="gzip", chunksize=Config.CHUNK_ROWS):
            filtered = _filter_chunk(chunk)
            if filtered.empty:
                continue
            table = pa.Table.from_pandas(filtered, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(Config.OUTPUT_FILE, table.schema)
            writer.write_table(table)
    if writer:
        writer.close()
        print(f"wrote filtered parquet to {Config.OUTPUT_FILE}")
    else:
        print("no data matched filters, parquet not written")
if __name__ == "__main__":
    process_all()

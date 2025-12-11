import os
import time
from datetime import datetime
import pandas as pd
import requests
from dotenv import load_dotenv
load_dotenv()
class Config:
    API_URL = "https://api.openaq.org/v2/measurements"
    OUTPUT_DIR = "data/raw/openaq"
    QUERY_MODE = "coordinates"
    CITY_REGION = "New York-Northern New Jersey-Long Island"
    COORDINATES = "40.7128,-74.0060"
    RADIUS_METERS = 50000
    LOCATION_IDS = []
    PARAMETERS = ["pm25", "no2"]
    DATE_RANGE = pd.date_range(
        start="2024-06-01", end=pd.Timestamp.now().normalize(), freq="MS"
    )
    LIMIT = 10000
def get_api_params(parameter, date_from, date_to, page=1):
    params = {
        "parameter": parameter,
        "limit": Config.LIMIT,
        "date_from": date_from,
        "date_to": date_to,
        "page": page,
        "order_by": "date",
    }
    mode = Config.QUERY_MODE.lower()
    if mode == "coordinates":
        params["coordinates"] = Config.COORDINATES
        params["radius"] = Config.RADIUS_METERS
    elif mode == "locations" and Config.LOCATION_IDS:
        params["location_id"] = ",".join(map(str, Config.LOCATION_IDS))
    else:
        params["city"] = Config.CITY_REGION
    return params
def process_and_save_data(records, filepath):
    if not records:
        return
    df = pd.DataFrame(records)
    if "date" in df.columns:
        df["timestamp_utc"] = df["date"].apply(
            lambda x: x.get("utc") if isinstance(x, dict) else None
        )
        df["timestamp_local"] = df["date"].apply(
            lambda x: x.get("local") if isinstance(x, dict) else None
        )
    if "coordinates" in df.columns:
        df["latitude"] = df["coordinates"].apply(
            lambda x: x.get("latitude") if isinstance(x, dict) else None
        )
        df["longitude"] = df["coordinates"].apply(
            lambda x: x.get("longitude") if isinstance(x, dict) else None
        )
    cols_to_drop = ["date", "coordinates"]
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    df.to_csv(filepath, index=False)
    print(f"    Saved {len(df)} records to {filepath}")
def fetch_page(params):
    api_key = os.getenv("OPENAQ_API_KEY")
    headers = {"X-API-Key": api_key} if api_key else {}
    try:
        response = requests.get(Config.API_URL, params=params, headers=headers)
        if response.status_code == 429:
            print("rate limit hit, sleeping for 60 seconds")
            time.sleep(60)
            return fetch_page(params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"error fetching data: {e}")
        return None
def fetch_data_for_interval(start_date, end_date, parameter):
    date_from_str = start_date.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    date_to_str = end_date.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    year = start_date.year
    month = start_date.month
    output_file = os.path.join(
        Config.OUTPUT_DIR, str(year), parameter, f"{parameter}_{year}_{month:02d}.csv"
    )
    if os.path.exists(output_file):
        print(f"skipping {output_file}, already exists")
        return
    print(f"starting to fetching {start_date.strftime('%Y-%m')} for {parameter}")
    all_records = []
    page = 1
    while True:
        params = get_api_params(parameter, date_from_str, date_to_str, page)
        data = fetch_page(params)
        if not data or "results" not in data:
            break
        results = data["results"]
        if not results:
            break
        all_records.extend(results)
        print(f"    Page {page}: Retrieved {len(results)} records")
        meta = data.get("meta", {})
        found = meta.get("found", 0)
        if len(all_records) >= found or len(results) < Config.LIMIT:
            break
        page += 1
        time.sleep(0.5)
    if all_records:
        process_and_save_data(all_records, output_file)
    else:
        print(f"no data found for {start_date.strftime('%Y-%m')}")
def run_collection():
    if not os.path.exists(Config.OUTPUT_DIR):
        os.makedirs(Config.OUTPUT_DIR)
    for start_date in Config.DATE_RANGE:
        next_month = start_date + pd.DateOffset(months=1)
        end_date = next_month - pd.Timedelta(seconds=1)
        if start_date > pd.Timestamp.now():
            break
        for param in Config.PARAMETERS:
            fetch_data_for_interval(start_date, end_date, param)
if __name__ == "__main__":
    if not os.getenv("OPENAQ_API_KEY"):
        print(
            "WARNING: OPENAQ_API_KEY not found in environment so requests might be rate limited check lines 140ish."
        )
    run_collection()

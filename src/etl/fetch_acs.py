import csv
import os
import requests
from dotenv import load_dotenv
load_dotenv()
class Config:
    YEAR = "2022"
    DATASET = "acs/acs5"
    VARS = ["B01003_001E", "B17001_001E", "B17001_002E"]
    STATES = {"36": "NY", "34": "NJ", "09": "CT"}
    OUTPUT_DIR = "data/raw/acs"
    OUTPUT_FILE = os.path.join(OUTPUT_DIR, "acs_2022.csv")
def fetch_state(state_fips: str):
    api_key = os.getenv("CENSUS_API_KEY")
    params = {
        "get": ",".join(Config.VARS),
        "for": "tract:*",
        "in": f"state:{state_fips}",
    }
    if api_key:
        params["key"] = api_key
    url = f"https://api.census.gov/data/{Config.YEAR}/{Config.DATASET}"
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    header = data[0]
    rows = data[1:]
    records = []
    for row in rows:
        rec = dict(zip(header, row))
        geoid = f"{rec['state']}{rec['county']}{rec['tract']}"
        records.append(
            {
                "geo_id": geoid,
                "population": rec.get("B01003_001E"),
                "poverty_total": rec.get("B17001_001E"),
                "poverty_below": rec.get("B17001_002E"),
            }
        )
    return records
def run():
    os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
    all_recs = []
    for fips in Config.STATES.keys():
        print(f"Fetching ACS for state {fips}")
        all_recs.extend(fetch_state(fips))
    with open(Config.OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["geo_id", "population", "poverty_total", "poverty_below"]
        )
        writer.writeheader()
        writer.writerows(all_recs)
    print(f"Wrote {len(all_recs)} rows to {Config.OUTPUT_FILE}")
if __name__ == "__main__":
    run()

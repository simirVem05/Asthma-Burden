import io
import os
import zipfile
import requests
class Config:
    STATES = {
        "NY": "36",
        "NJ": "34",
        "CT": "09",
    }
    BASE_URL = "https://www2.census.gov/geo/tiger/TIGER2020/TRACT"
    OUTPUT_DIR = "data/raw/tracts"
def download_state(state: str, fips: str):
    fname = f"tl_2020_{fips}_tract.zip"
    url = f"{Config.BASE_URL}/{fname}"
    print(f"downloading tracts for {state} ({fips}) from {url}")
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        z.extractall(Config.OUTPUT_DIR)
    print(f"extracted to {Config.OUTPUT_DIR}")
def run():
    for st, fips in Config.STATES.items():
        download_state(st, fips)
if __name__ == "__main__":
    run()

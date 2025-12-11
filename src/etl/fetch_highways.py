import io
import os
import zipfile
import requests
class Config:
    STATES = {"NY": "36", "NJ": "34", "CT": "09"}
    BASE_URL = "https://www2.census.gov/geo/tiger/TIGER2023/PRISECROADS"
    OUTPUT_DIR = "data/raw/highways"
def download_state_highway(state, fips):
    filename = f"tl_2023_{fips}_prisecroads.zip"
    url = f"{Config.BASE_URL}/{filename}"
    print(f"downloading highway data for {state} (FIPS {fips}) from this url:{url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        print(f"Download complete for {state}. Extracting...")
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall(Config.OUTPUT_DIR)
        print(
            f"successfully extracted {state} shapefiles to this directory: {Config.OUTPUT_DIR}"
        )
    except Exception as e:
        print(f"failed to download or extract highway data for {state}: {e}")
def run_collection():
    if not os.path.exists(Config.OUTPUT_DIR):
        os.makedirs(Config.OUTPUT_DIR)
    for state, fips in Config.STATES.items():
        download_state_highway(state, fips)
if __name__ == "__main__":
    run_collection()

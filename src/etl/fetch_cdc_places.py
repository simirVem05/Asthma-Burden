import os
import requests
class Config:
    CSV_URL = (
        "https://chronicdata.cdc.gov/api/views/cwsq-ngmh/rows.csv?accessType=DOWNLOAD"
    )
    OUTPUT_DIR = "data/raw/cdc"
    OUTPUT_FILE = os.path.join(OUTPUT_DIR, "places_tracts.csv")
def download_cdc_places():
    if not os.path.exists(Config.OUTPUT_DIR):
        os.makedirs(Config.OUTPUT_DIR)
    if os.path.exists(Config.OUTPUT_FILE):
        print(f"File {Config.OUTPUT_FILE} already exists. Skipping download.")
        return
    print(f"Downloading the CDC PLACES data from {Config.CSV_URL}...")
    print("downloading a 700mb file will take super duper long")
    try:
        with requests.get(Config.CSV_URL, stream=True) as r:
            r.raise_for_status()
            with open(Config.OUTPUT_FILE, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"successfully downloaded to this file: {Config.OUTPUT_FILE}")
    except Exception as e:
        print(f"error in downloading CDC data: {e}")
if __name__ == "__main__":
    download_cdc_places()

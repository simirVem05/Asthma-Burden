import io
import os
import zipfile
import requests
class Config:
    URL = "https://www.atsdr.cdc.gov/placeandhealth/svi/data/SVI2020_US_tract.zip"
    OUTPUT_DIR = "data/raw/svi"
def run():
    print(f"downloading SVI from {Config.URL}")
    resp = requests.get(Config.URL, stream=True)
    resp.raise_for_status()
    os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        z.extractall(Config.OUTPUT_DIR)
    print(f"extracted to {Config.OUTPUT_DIR}")
if __name__ == "__main__":
    run()

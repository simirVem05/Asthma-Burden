import os
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
CSV_PATH = "data/raw/cdc/places_tracts.csv"
STATES = {"NY", "NJ", "CT"}
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
def run():
    if not os.path.exists(CSV_PATH):
        raise SystemExit(f"missing CDC PLACES file: {CSV_PATH}")
    usecols = ["StateAbbr", "LocationID", "MeasureId", "Data_Value"]
    df = pd.read_csv(CSV_PATH, usecols=usecols)
    df = df[df["StateAbbr"].isin(STATES)]
    df = df[df["MeasureId"] == "CASTHMA"]
    df["geo_id"] = df["LocationID"].astype(str).str.zfill(11)
    records = list(zip(df["geo_id"], df["Data_Value"]))
    if not records:
        print("no asthma records to load")
        return
    sql = """
        UPDATE tracts
        SET asthma_prev = data.asthma_prev
        FROM (VALUES %s) AS data(geo_id, asthma_prev)
        WHERE tracts.geo_id = data.geo_id
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, records, page_size=2000)
        conn.commit()
    print(f"Updated asthma_prev for {len(records)} tracts.")
if __name__ == "__main__":
    run()

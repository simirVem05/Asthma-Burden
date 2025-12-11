import os
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
load_dotenv()
CSV_PATH = "data/raw/svi/SVI2020_US_tract.csv"
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
        raise SystemExit(f"missing svi file: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH, dtype={"FIPS": str})
    df = df[df["STATE"].isin(STATES)]
    df["geo_id"] = df["FIPS"].str.zfill(11)
    df["svi"] = pd.to_numeric(df["RPL_THEMES"], errors="coerce")
    records = list(zip(df["geo_id"], df["svi"]))
    sql = """
        UPDATE tracts
        SET svi_ranking = data.svi
        FROM (VALUES %s) AS data(geo_id, svi)
        WHERE tracts.geo_id = data.geo_id;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, records, page_size=2000)
        conn.commit()
    print(f"updated svi_ranking for {len(records)} tracts")
if __name__ == "__main__":
    run()

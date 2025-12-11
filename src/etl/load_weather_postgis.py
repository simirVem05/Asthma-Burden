import os
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
load_dotenv()
CSV_PATH = "data/raw/weather/daily_covariates.csv"
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
        print(f"weather data not found at {CSV_PATH}, run fetch_weather.py first")
        return
    print(f"loading weather data from {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)
    if "smoke_surge" in df.columns:
        df["smoke_surge"] = df["smoke_surge"].astype(bool)
    records = []
    for _, row in df.iterrows():
        records.append(
            (
                row["date"],
                row["avg_temp_celsius"],
                row["avg_humidity"],
                row["pollen_level"],
                row["smoke_surge"],
            )
        )
    sql = """
        INSERT INTO daily_covariates
        (date, avg_temp_celsius, avg_humidity, pollen_level, smoke_surge)
        VALUES %s
        ON CONFLICT (date) DO UPDATE SET
            avg_temp_celsius = EXCLUDED.avg_temp_celsius,
            avg_humidity = EXCLUDED.avg_humidity,
            pollen_level = EXCLUDED.pollen_level,
            smoke_surge = EXCLUDED.smoke_surge;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, records, page_size=1000)
        conn.commit()
    print(f"successfully loaded {len(records)} daily weather records")
if __name__ == "__main__":
    run()

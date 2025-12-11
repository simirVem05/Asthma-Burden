import os
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
load_dotenv()
CSV_PATH = "data/raw/acs/acs_2022.csv"
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
        raise SystemExit(f"missing ACS file: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH, dtype={"geo_id": str})
    records = []
    for _, row in df.iterrows():
        try:
            pop = int(row["population"])
        except Exception:
            pop = None
        try:
            pov_total = float(row["poverty_total"])
            pov_below = float(row["poverty_below"])
            pov_rate = pov_below / pov_total if pov_total > 0 else None
        except Exception:
            pov_rate = None
        records.append((row["geo_id"].zfill(11), pop, pov_rate))
    sql = """
        UPDATE tracts
        SET population = data.population,
            poverty_rate = data.poverty_rate
        FROM (VALUES %s) AS data(geo_id, population, poverty_rate)
        WHERE tracts.geo_id = data.geo_id;
        UPDATE tracts
        SET population_density = CASE
            WHEN population IS NOT NULL AND ST_Area(geom::geography) > 0
            THEN population / (ST_Area(geom::geography) / 1000000.0)
            ELSE NULL
        END;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, records, page_size=2000)
        conn.commit()
    print(f"updated population/poverty_rate for {len(records)} tracts")
if __name__ == "__main__":
    run()

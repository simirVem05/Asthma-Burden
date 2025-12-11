import os
import geopandas as gpd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
TRACT_FILES = [
    "data/raw/tracts/tl_2020_36_tract.shp",
    "data/raw/tracts/tl_2020_34_tract.shp",
    "data/raw/tracts/tl_2020_09_tract.shp",
]
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
def load_file(path: str, cur):
    if not os.path.exists(path):
        print(f"Missing shapefile: {path}")
        return
    gdf = gpd.read_file(path)
    gdf = gdf.to_crs(epsg=4326)
    records = []
    for _, row in gdf.iterrows():
        geoid = row.get("GEOID")
        name = row.get("NAMELSAD")
        state = row.get("STATEFP")
        county = row.get("COUNTYFP")
        pop = row.get("POPULATION") if "POPULATION" in row else None
        area = row.get("ALAND") if "ALAND" in row else None
        geom = row.geometry
        if geoid is None or geom is None:
            continue
        records.append(
            (
                geoid,
                state,
                county,
                name,
                pop,
                None,
                None,
                None,
                None,
                geom.wkt,
            )
        )
    if not records:
        return
    sql = """
        INSERT INTO tracts
        (geo_id, state_code, county_code, name, population, population_density, svi_ranking, poverty_rate, asthma_prev, geom)
        VALUES %s
        ON CONFLICT (geo_id) DO UPDATE
          SET state_code = EXCLUDED.state_code,
              county_code = EXCLUDED.county_code,
              name = EXCLUDED.name,
              population = EXCLUDED.population,
              geom = EXCLUDED.geom
    """
    template = "(%s,%s,%s,%s,%s,%s,%s,%s,%s, ST_SetSRID(ST_GeomFromText(%s),4326))"
    psycopg2.extras.execute_values(cur, sql, records, template=template, page_size=1000)
    print(f"inserted {len(records)} tracts from {path}")
def run():
    with get_conn() as conn:
        with conn.cursor() as cur:
            for path in TRACT_FILES:
                load_file(path, cur)
        conn.commit()
if __name__ == "__main__":
    run()

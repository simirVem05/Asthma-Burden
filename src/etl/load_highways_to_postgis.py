import glob
import os
import geopandas as gpd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
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
    gdf = gpd.read_file(path)
    gdf = gdf.to_crs(epsg=4326)
    records = []
    for _, row in gdf.iterrows():
        linear_id = row.get("LINEARID")
        fullname = row.get("FULLNAME")
        mtfcc = row.get("MTFCC")
        geom = row.geometry
        if geom is None:
            continue
        records.append(
            (
                linear_id,
                fullname,
                mtfcc,
                None,
                None,
                geom.wkt,
            )
        )
    if not records:
        return
    sql = """
        INSERT INTO highways (linear_id, fullname, mtfcc, type, traffic_volume, geom)
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    template = "(%s,%s,%s,%s,%s, ST_SetSRID(ST_GeomFromText(%s),4326))"
    psycopg2.extras.execute_values(cur, sql, records, template=template, page_size=2000)
    print(f"inserted {len(records)} highways from {os.path.basename(path)}")
def run():
    paths = glob.glob("data/raw/highways/tl_2023_*_prisecroads.shp")
    if not paths:
        print("no highway shapefiles found in data/raw/highways/")
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            for p in paths:
                load_file(p, cur)
        conn.commit()
if __name__ == "__main__":
    run()

import os
import joblib
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
load_dotenv()
def get_db_engine():
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    dbname = os.getenv("PGDATABASE", "asthma")
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")
def load_data():
    engine = get_db_engine()
    query = """
    SELECT geo_id, population, asthma_prev,
           pm25_mean, no2_mean, poverty_rate, dist_primary_road_meters,
           population_density, year
    FROM modeling_data
    WHERE year = 2024;
    """
    print("Loading 2024 data for simulation...")
    df = pd.read_sql(query, engine)
    cols = [
        "asthma_prev",
        "pm25_mean",
        "no2_mean",
        "poverty_rate",
        "dist_primary_road_meters",
        "population_density",
        "year",
    ]
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=cols)
    return df
def run_scenarios(model, df):
    X_base = df[
        [
            "pm25_mean",
            "no2_mean",
            "poverty_rate",
            "dist_primary_road_meters",
            "population_density",
            "year",
        ]
    ].values
    pred_base = np.clip(model.predict(X_base), 0, 100)
    burden_base = np.sum((pred_base / 100) * df["population"])
    print(f"Baseline (2024) Estimated Asthma Cases: {burden_base:,.0f}")
    results = []
    print("Simulating Scenario 1: Traffic Buffer (-20% NO2 <500m)...")
    df_s1 = df.copy()
    mask_road = df_s1["dist_primary_road_meters"] < 500
    df_s1.loc[mask_road, "no2_mean"] *= 0.80
    X_s1 = df_s1[
        [
            "pm25_mean",
            "no2_mean",
            "poverty_rate",
            "dist_primary_road_meters",
            "population_density",
            "year",
        ]
    ].values
    pred_s1 = np.clip(model.predict(X_s1), 0, 100)
    burden_s1 = np.sum((pred_s1 / 100) * df_s1["population"])
    results.append(
        {
            "Scenario": "Traffic Buffer (-20% NO2 <500m)",
            "Cases": burden_s1,
            "Prevented": burden_base - burden_s1,
        }
    )
    print("Simulating Scenario 2: PM2.5 Cap at 8.0...")
    df_s2 = df.copy()
    mask_cap = df_s2["pm25_mean"] > 8.0
    df_s2.loc[mask_cap, "pm25_mean"] = 8.0
    X_s2 = df_s2[
        [
            "pm25_mean",
            "no2_mean",
            "poverty_rate",
            "dist_primary_road_meters",
            "population_density",
            "year",
        ]
    ].values
    pred_s2 = np.clip(model.predict(X_s2), 0, 100)
    burden_s2 = np.sum((pred_s2 / 100) * df_s2["population"])
    results.append(
        {
            "Scenario": "PM2.5 Cap (8.0 ug/m3)",
            "Cases": burden_s2,
            "Prevented": burden_base - burden_s2,
        }
    )
    print("Simulating Scenario 3: Equity Focus...")
    df_s3 = df.copy()
    if df["poverty_rate"].max() <= 1.0:
        mask_pov = df_s3["poverty_rate"] > 0.20
    else:
        mask_pov = df_s3["poverty_rate"] > 20.0
    df_s3.loc[mask_pov, "pm25_mean"] *= 0.90
    X_s3 = df_s3[
        [
            "pm25_mean",
            "no2_mean",
            "poverty_rate",
            "dist_primary_road_meters",
            "population_density",
            "year",
        ]
    ].values
    pred_s3 = np.clip(model.predict(X_s3), 0, 100)
    burden_s3 = np.sum((pred_s3 / 100) * df_s3["population"])
    results.append(
        {
            "Scenario": "Equity Focus (-10% PM2.5 in High Poverty)",
            "Cases": burden_s3,
            "Prevented": burden_base - burden_s3,
        }
    )
    print("Simulating Scenario 4: Aggressive PM2.5 (-20% All)...")
    df_s4 = df.copy()
    df_s4["pm25_mean"] *= 0.80
    X_s4 = df_s4[
        [
            "pm25_mean",
            "no2_mean",
            "poverty_rate",
            "dist_primary_road_meters",
            "population_density",
            "year",
        ]
    ].values
    pred_s4 = np.clip(model.predict(X_s4), 0, 100)
    burden_s4 = np.sum((pred_s4 / 100) * df_s4["population"])
    results.append(
        {
            "Scenario": "Aggressive PM2.5 (-20% All)",
            "Cases": burden_s4,
            "Prevented": burden_base - burden_s4,
        }
    )
    res_df = pd.DataFrame(results).sort_values("Prevented", ascending=False)
    print("\nIntervention Results")
    print(res_df)
    res_df.to_csv("reports/intervention_impact.csv", index=False)
    print("Saved results to reports/intervention_impact.csv")
def run():
    model_path = "models/gam_asthma.pkl"
    if not os.path.exists(model_path):
        print("Model not found. Run model_gam.py first.")
        return
    print("Loading model...")
    model = joblib.load(model_path)
    df = load_data()
    run_scenarios(model, df)

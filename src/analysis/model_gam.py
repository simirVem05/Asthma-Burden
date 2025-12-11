import os
import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pygam import LinearGAM, s, te
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import KFold
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
    SELECT geo_id, state_code, county_code, asthma_prev,
           pm25_mean, no2_mean, poverty_rate, dist_primary_road_meters,
           population_density, year
    FROM modeling_data;
    """
    print("Loading modeling data...")
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
    print(f"Loaded {len(df)} clean records.")
    return df
def train_and_validate(df):
    X = df[
        [
            "pm25_mean",
            "no2_mean",
            "poverty_rate",
            "dist_primary_road_meters",
            "population_density",
            "year",
        ]
    ].values
    y = df["asthma_prev"].values
    groups = df["county_code"].values
    gam = LinearGAM(s(0) + s(1) + s(2) + s(3) + s(4) + s(5))
    print("\nStarting Spatial Cross-Validation (Grouped by County)")
    unique_counties = np.unique(groups)
    kf = KFold(n_splits=5, shuffle=True, random_state=42)
    rmse_scores = []
    r2_scores = []
    for train_idx, test_idx in kf.split(unique_counties):
        test_counties = unique_counties[test_idx]
        train_mask = ~np.isin(groups, test_counties)
        test_mask = np.isin(groups, test_counties)
        X_train, y_train = X[train_mask], y[train_mask]
        X_test, y_test = X[test_mask], y[test_mask]
        if len(X_test) == 0:
            continue
        model = LinearGAM(s(0) + s(1) + s(2) + s(3) + s(4) + s(5)).fit(X_train, y_train)
        preds = model.predict(X_test)
        rmse = np.sqrt(mean_squared_error(y_test, preds))
        r2 = r2_score(y_test, preds)
        rmse_scores.append(rmse)
        r2_scores.append(r2)
        print(
            f"Fold Results -> RMSE: {rmse:.4f}, R2: {r2:.4f} (Held out {len(test_counties)} counties)"
        )
    print(f"\nAverage RMSE: {np.mean(rmse_scores):.4f}")
    print(f"Average R2: {np.mean(r2_scores):.4f}")
    print("\nRetraining on full dataset...")
    final_model = gam.fit(X, y)
    print(final_model.summary())
    return final_model
def save_model_and_plots(model):
    os.makedirs("models", exist_ok=True)
    os.makedirs("reports/figures", exist_ok=True)
    joblib.dump(model, "models/gam_asthma.pkl")
    print("Saved model to models/gam_asthma.pkl")
    titles = ["PM2.5", "NO2", "Poverty Rate", "Distance to Road", "Pop Density", "Year"]
    plt.figure(figsize=(15, 10))
    for i, title in enumerate(titles):
        ax = plt.subplot(2, 3, i + 1)
        XX = model.generate_X_grid(term=i)
        plt.plot(XX[:, i], model.partial_dependence(term=i, X=XX))
        plt.plot(
            XX[:, i],
            model.partial_dependence(term=i, X=XX, width=0.95)[1],
            c="r",
            ls="
        )
        plt.title(title)
        plt.xlabel("Value")
        plt.ylabel("Partial Effect")
        plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig("reports/figures/gam_partial_dependence.png")
    print("Saved partial dependence plot.")
def run():
    df = load_data()
    model = train_and_validate(df)
    save_model_and_plots(model)
if __name__ == "__main__":
    run()

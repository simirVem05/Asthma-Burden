import os
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from dotenv import load_dotenv
from sqlalchemy import create_engine
load_dotenv()
def load_data():
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    dbname = os.getenv("PGDATABASE", "asthma")
    db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    engine = create_engine(db_url)
    query = "SELECT * FROM modeling_data;"
    print("Loading data from postgis")
    df = pd.read_sql(query, engine)
    print(f"Loaded {len(df)} records")
    return df
def generate_summary(df):
    print("\nStatistical Summary")
    cols = [
        "asthma_prev",
        "pm25_mean",
        "no2_mean",
        "poverty_rate",
        "dist_primary_road_meters",
        "population_density",
    ]
    summary = df[cols].describe()
    print(summary)
    summary.to_csv("reports/summary_stats.csv")
    print("Saved summary to reports/summary_stats.csv")
def plot_distributions(df):
    print("\nGenerating Distributions")
    os.makedirs("reports/figures", exist_ok=True)
    vars_to_plot = ["asthma_prev", "pm25_mean", "no2_mean", "poverty_rate"]
    for var in vars_to_plot:
        plt.figure(figsize=(8, 6))
        sns.histplot(df[var], kde=True, bins=30)
        plt.title(f"Distribution of {var}")
        plt.xlabel(var)
        plt.ylabel("Frequency")
        plt.tight_layout()
        path = f"reports/figures/hist_{var}.png"
        plt.savefig(path)
        plt.close()
        print(f"Saved {path}")
def plot_correlations(df):
    print("\nGenerating Correlation Matrix")
    cols = [
        "asthma_prev",
        "pm25_mean",
        "no2_mean",
        "poverty_rate",
        "dist_primary_road_meters",
        "population_density",
    ]
    corr = df[cols].corr()
    plt.figure(figsize=(10, 8))
    sns.heatmap(corr, annot=True, cmap="coolwarm", fmt=".2f")
    plt.title("Correlation Matrix")
    plt.tight_layout()
    path = "reports/figures/correlation_matrix.png"
    plt.savefig(path)
    plt.close()
    print(f"Saved {path}")
def plot_scatters(df):
    print("\nGenerating Scatter Plots")
    predictors = ["pm25_mean", "no2_mean", "poverty_rate", "dist_primary_road_meters"]
    target = "asthma_prev"
    for pred in predictors:
        plt.figure(figsize=(8, 6))
        sns.scatterplot(data=df, x=pred, y=target, alpha=0.3)
        plt.title(f"Asthma Prevalence vs {pred}")
        plt.xlabel(pred)
        plt.ylabel("Asthma Prevalence (%)")
        plt.tight_layout()
        path = f"reports/figures/scatter_asthma_vs_{pred}.png"
        plt.savefig(path)
        plt.close()
        print(f"Saved {path}")
def run():
    df = load_data()
    df = df.dropna(subset=["asthma_prev", "pm25_mean", "no2_mean", "poverty_rate"])
    generate_summary(df)
    plot_distributions(df)
    plot_correlations(df)
    plot_scatters(df)
    print("\nEDA Complete")
if __name__ == "__main__":
    run()

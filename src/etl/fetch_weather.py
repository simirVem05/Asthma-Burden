import os
from datetime import date
import pandas as pd
import requests
class Config:
    URL = "https://archive-api.open-meteo.com/v1/archive"
    LAT = 40.7128
    LON = -74.0060
    START_DATE = "2019-01-01"
    END_DATE = date.today().strftime("%Y-%m-%d")
    OUTPUT_DIR = "data/raw/weather"
    OUTPUT_FILE = os.path.join(OUTPUT_DIR, "daily_covariates.csv")
def fetch_weather_data():
    print(
        f"fetching weather data for NYC from {Config.START_DATE} to {Config.END_DATE}"
    )
    params = {
        "latitude": Config.LAT,
        "longitude": Config.LON,
        "start_date": Config.START_DATE,
        "end_date": Config.END_DATE,
        "daily": "temperature_2m_mean,relative_humidity_2m_mean",
        "timezone": "America/New_York",
    }
    try:
        response = requests.get(Config.URL, params=params)
        response.raise_for_status()
        data = response.json()
        daily_data = data.get("daily", {})
        if not daily_data:
            print("no daily data found in response")
            return pd.DataFrame()
        df = pd.DataFrame(
            {
                "date": daily_data["time"],
                "avg_temp_celsius": daily_data["temperature_2m_mean"],
                "avg_humidity": daily_data["relative_humidity_2m_mean"],
            }
        )
        return df
    except requests.exceptions.RequestException as e:
        print(f"api request error: {e}")
        return pd.DataFrame()
def add_derived_covariates(df):
    if df.empty:
        return df
    print("adding derived covariates (smoke surge, pollen)")
    smoke_days = ["2023-06-06", "2023-06-07", "2023-06-08"]
    df["smoke_surge"] = df["date"].isin(smoke_days)
    def get_pollen_proxy(row_date):
        dt = pd.to_datetime(row_date)
        month = dt.month
        if month in [3, 4, 5]:
            return "High"
        elif month in [6, 7, 8, 9]:
            return "Medium"
        else:
            return "Low"
    df["pollen_level"] = df["date"].apply(get_pollen_proxy)
    return df
def run():
    if not os.path.exists(Config.OUTPUT_DIR):
        os.makedirs(Config.OUTPUT_DIR)
    df = fetch_weather_data()
    if not df.empty:
        df = add_derived_covariates(df)
        df.to_csv(Config.OUTPUT_FILE, index=False)
        print(f"Successfully saved {len(df)} rows to {Config.OUTPUT_FILE}")
        print("Sample Data:")
        print(df.head())
    else:
        print("Failed to fetch data.")
if __name__ == "__main__":
    run()

Prerequisites

Python 3.10+
PostgreSQL 15+ with PostGIS
API Keys:
[OpenAQ API Key](https://openaq.org/) (for pollution data).
[US Census API Key](https://api.census.gov/data/key_signup.html) (for ACS demographics).

Setup Guide

1. Database Setup
Create a local PostgreSQL database and enable PostGIS:

```bash
createdb asthma
psql -d asthma -c "CREATE EXTENSION IF NOT EXISTS postgis;"
```
Apply the schema:
```bash
psql -d asthma -f src/database/schema.sql
```

2. Environment Configuration
Copy the template and fill in your credentials:
```bash
cp .env.example .env
# Edit .env with your API keys and DB credentials
```

3. Installation
Create a venv and install Python dependencies:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Data Engineering (ETL)

Run the ETL scripts in this order to populate the database. Some downloads like OpenAQ and Shapefiles are large.

1.  Download & Load Highways:
    ```bash
    python src/etl/fetch_highways.py
    python src/etl/load_highways_to_postgis.py
    ```

2.  Download & Load Census Tracts:
    ```bash
    python src/etl/fetch_tracts.py
    python src/etl/load_tracts_to_postgis.py
    ```

3.  Load Demographics:
    ```bash
    python src/etl/fetch_acs.py
    python src/etl/load_acs_to_postgis.py
    Note: SVI 2022 CSV must be downloaded manually to data/raw/svi/ due to bot protection
    python src/etl/load_svi_to_postgis.py
    ```

4.  Load Health Outcomes:
    ```bash
    python src/etl/fetch_cdc_places.py
    python src/etl/load_cdc_places_asthma.py
    ```

5.  Load Pollution:
    This uses the bulk archive for historical coverage from 2019-2024
    *   Step 1: Identify target location IDs or edit `src/etl/fetch_openaq_bulk.py` to get fresh lists
    *   Step 2: Sync data
    *   Step 3: Filter and Load:
        ```bash
        python src/etl/process_openaq_local.py
        python src/etl/load_openaq_to_postgis.py
        ```

6. Load Weather and Covariates:
    Fetches daily temperature, humidity, and pollen proxy data for NYC from 2019-2024
    ```bash
    python src/etl/fetch_weather.py
    python src/etl/load_weather_to_postgis.py
    ```

Analysis & Modeling

Create modeling view
```bash
psql -d asthma -f src/database/modeling_data.sql
```

Once the database is populated, run the full analysis pipeline:
This runs EDA, GAM training, and intervention simulation
```bash
./run_analysis.sh
```

1.  EDA: Generate histograms and correlations in `reports/figures/`
2.  Model: Train a Spatial GAM to predict asthma prevalence
3.  Simulate: Estimate cases prevented under different intervention scenarios

Results
Figures: `reports/figures/`
Intervention Data: `reports/intervention_impact.csv`

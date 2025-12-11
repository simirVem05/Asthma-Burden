#!/bin/bash
set -e

# Load env vars if .env exists
if [ -f .env ]; then
    export $(cat .env | grep -v '#' | xargs)
fi

echo "--- 1. Running Exploratory Data Analysis (EDA) ---"
python src/analysis/eda.py

echo "--- 2. Training GAM Model with Spatial CV ---"
python src/analysis/model_gam.py

echo "--- 3. Running Intervention Analysis ---"
python src/analysis/intervention.py

echo "--- Analysis Pipeline Complete! ---"
echo "Check reports/figures/ for plots and reports/intervention_impact.csv for results."

"""
  Decarga de scripts de python
"""
url_base='https://raw.githubusercontent.com/leocorbur/test_GreenMiles_NYC_Taxis/main/GCP/'
files=(
  "cs_to_bigquery_tlc.py" \
  "cs_to_bigquery_airPollution.py" \
  "cs_to_bigquery_weather.py" \
  "cs_to_bq_altFuelVehicles.py" \
  "cs_to_bq_fuelConsumption.py" \
  "cs_to_bigquery_carPrices.py"
)

for file in "${files[@]}"; do
    curl -LJO "${url_base}${file}" &&
    gsutil cp "$file" gs://job_dataproc/ &&
    rm "$file"
done



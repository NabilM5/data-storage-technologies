#!/usr/bin/env bash
# Source download step. Run ON THE HOST from the infra/ directory:
#     bash ../student-01-hw01/download_source.sh
#
# Source: Open-Meteo Historical Weather API (ERA5 archive), CC BY 4.0.
# Documentation and terms: https://open-meteo.com/en/docs/historical-weather-api
# Files are saved as-is, without manual edits: infra/data/hw01/raw/<city>.csv
set -euo pipefail

START_DATE="2024-01-01"
END_DATE="2025-12-31"
HOURLY="temperature_2m,relative_humidity_2m,precipitation"
OUT_DIR="data/hw01/raw"

# city|latitude|longitude
CITIES=(
  "Moscow|55.75|37.62"
  "Saint_Petersburg|59.94|30.31"
  "Novosibirsk|55.03|82.92"
  "Yekaterinburg|56.84|60.61"
  "Kazan|55.79|49.11"
  "Sochi|43.60|39.73"
  "Murmansk|68.97|33.08"
  "Vladivostok|43.12|131.89"
  "Krasnoyarsk|56.01|92.87"
  "Astrakhan|46.35|48.04"
  "Arkhangelsk|64.54|40.54"
  "Irkutsk|52.29|104.30"
)

mkdir -p "$OUT_DIR"

for entry in "${CITIES[@]}"; do
  IFS='|' read -r city lat lon <<< "$entry"
  url="https://archive-api.open-meteo.com/v1/archive?latitude=${lat}&longitude=${lon}&start_date=${START_DATE}&end_date=${END_DATE}&hourly=${HOURLY}&timezone=UTC&format=csv"
  echo "GET ${city} ..."
  curl -fsS "$url" -o "${OUT_DIR}/${city}.csv"
  # Pause: the free Open-Meteo tier rate-limits requests.
  sleep 2
done

echo
echo "Downloaded files: $(ls -1 "${OUT_DIR}" | wc -l | tr -d ' ')"
ls -lh "${OUT_DIR}"
echo
echo "Total size:"
du -sh "${OUT_DIR}"

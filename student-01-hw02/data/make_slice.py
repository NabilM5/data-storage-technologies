"""Build the control slice and the scenario inputs from the Citi Bike month file.

Source: https://s3.amazonaws.com/tripdata/JC-202608-citibike-tripdata.csv.zip
(Citi Bike Data License Agreement). Slice = trips started between DATE_FROM
and DATE_TO at STATIONS. Run from the project directory:

    python3 data/make_slice.py

Writes data/slice/ (control slice + station reference) and data/scenarios/
(synthetic test inputs for the four cases).
"""
import csv
import io
import os
import sys
import urllib.request
import zipfile
from datetime import datetime, timedelta

MONTH_FILE = "JC-202608-citibike-tripdata.csv.zip"
URL = f"https://s3.amazonaws.com/tripdata/{MONTH_FILE}"
DATE_FROM, DATE_TO = "2026-08-03", "2026-08-09"   # Monday .. Sunday
STATIONS = ["JC115", "HB609", "JC109", "HB107", "JC009", "HB103"]
AREA = {"JC": "Jersey City", "HB": "Hoboken"}

HERE = os.path.dirname(os.path.abspath(__file__))
SLICE_DIR = os.path.join(HERE, "slice")
SCEN_DIR = os.path.join(HERE, "scenarios")
CACHE = os.path.join(HERE, MONTH_FILE)
COLS = ["ride_id", "rideable_type", "started_at", "ended_at",
        "start_station_name", "start_station_id", "end_station_name",
        "end_station_id", "start_lat", "start_lng", "end_lat", "end_lng",
        "member_casual"]


def download():
    if not os.path.exists(CACHE):
        print(f"downloading {URL}")
        urllib.request.urlretrieve(URL, CACHE)
    with zipfile.ZipFile(CACHE) as z:
        name = [n for n in z.namelist() if n.endswith(".csv") and "__MACOSX" not in n][0]
        return list(csv.DictReader(io.TextIOWrapper(z.open(name), encoding="utf-8")))


def write(path, rows, cols):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows({c: r.get(c, "") for c in cols} for r in rows)
    print(f"{len(rows):6d} rows -> {os.path.relpath(path, HERE)}")


def shift(ts, minutes):
    fmt = "%Y-%m-%d %H:%M:%S.%f"
    return (datetime.strptime(ts, fmt) + timedelta(minutes=minutes)).strftime(fmt)[:-3]


def main():
    month = download()
    if list(month[0].keys()) != COLS:
        sys.exit(f"unexpected columns: {list(month[0].keys())}")

    # 1. Control slice: trips started in the interval at the chosen stations.
    trips = [r for r in month
             if DATE_FROM <= r["started_at"][:10] <= DATE_TO
             and r["start_station_id"] in STATIONS]
    trips.sort(key=lambda r: (r["started_at"], r["ride_id"]))
    write(os.path.join(SLICE_DIR, f"trips_{DATE_FROM}_{DATE_TO}.csv"), trips, COLS)

    # 2. Station reference: one version per station, area from the id prefix.
    seen = {}
    for r in month:
        sid = r["start_station_id"]
        if sid and sid not in seen:
            seen[sid] = {"station_id": sid, "station_name": r["start_station_name"],
                         "area": AREA.get(sid[:2], "Other"),
                         "lat": r["start_lat"], "lng": r["start_lng"],
                         "valid_from": "2026-08-01", "valid_to": ""}
    stations = sorted(seen.values(), key=lambda s: s["station_id"])
    scols = list(stations[0].keys())
    write(os.path.join(SLICE_DIR, "stations_2026-08.csv"), stations, scols)

    # 3. Synthetic test inputs, derived from slice rows.
    day5 = [r for r in trips if r["started_at"].startswith("2026-08-05")]
    day6 = [r for r in trips if r["started_at"].startswith("2026-08-06")]

    # duplicate: the same row delivered twice.
    write(os.path.join(SCEN_DIR, "duplicate.csv"), [day5[0]], COLS)

    # late_fix: ended_at corrected by +15 min, started_at unchanged.
    fix = dict(day5[1]); fix["ended_at"] = shift(fix["ended_at"], 15)
    write(os.path.join(SCEN_DIR, "late_fix.csv"), [fix], COLS)

    # second_source: a partner sends different trips under the same ride_ids.
    partner = []
    for r in day6[:3]:
        p = dict(r)
        other = [s for s in STATIONS if s != r["start_station_id"]][0]
        p["start_station_id"] = other
        p["start_station_name"] = seen[other]["station_name"]
        p["start_lat"], p["start_lng"] = seen[other]["lat"], seen[other]["lng"]
        p["ended_at"] = shift(p["ended_at"], 7)
        partner.append(p)
    write(os.path.join(SCEN_DIR, "second_source.csv"), partner, COLS)

    # history_overlap: a second JC115 version while the first is still open.
    v2 = dict(seen["JC115"]); v2["station_name"] += " (renamed)"; v2["valid_from"] = "2026-08-05"
    write(os.path.join(SCEN_DIR, "history_overlap.csv"), [v2], scols)

    # zero_duration: broken input for the quality gate, ends when it starts.
    bad = dict(day5[2]); bad["ride_id"] = "TEST0000ZERODUR1"; bad["ended_at"] = bad["started_at"]
    write(os.path.join(SCEN_DIR, "zero_duration.csv"), [bad], COLS)


if __name__ == "__main__":
    main()

"""Recompute one mart cell from the source zip with pandas, to check the DWH.

    python3 data/independent_check.py 2026-08-05 JC115
"""
import os
import sys
import zipfile

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ZIP = os.path.join(HERE, "JC-202608-citibike-tripdata.csv.zip")


def main(day, station):
    with zipfile.ZipFile(ZIP) as z:
        name = [n for n in z.namelist() if n.endswith(".csv") and "__MACOSX" not in n][0]
        df = pd.read_csv(z.open(name))
    df["started_at"] = pd.to_datetime(df["started_at"])
    df["ended_at"] = pd.to_datetime(df["ended_at"])
    cell = df[(df["started_at"].dt.strftime("%Y-%m-%d") == day) & (df["start_station_id"] == station)].copy()
    cell["duration_sec"] = (cell["ended_at"] - cell["started_at"]).dt.total_seconds().astype(int)
    out = cell.groupby("member_casual").agg(
        trips=("ride_id", "count"),
        electric_trips=("rideable_type", lambda s: int((s == "electric_bike").sum())),
        total_duration_sec=("duration_sec", "sum"),
    )
    out["avg_duration_sec"] = (out["total_duration_sec"] / out["trips"]).round(1)
    print(f"source file: {os.path.basename(ZIP)}; day={day}; start_station_id={station}")
    print(out.to_string())
    print(f"total trips: {len(cell)}")


if __name__ == "__main__":
    main(*(sys.argv[1:3] or ("2026-08-05", "JC115")))

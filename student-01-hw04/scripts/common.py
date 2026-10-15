from pathlib import Path
import subprocess


PROJECT = Path(__file__).resolve().parents[1]
EVIDENCE = PROJECT / "evidence"


def command(args, *, data=None, check=True):
    result = subprocess.run(
        args,
        cwd=PROJECT,
        input=data,
        capture_output=True,
        text=True,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"command failed: {' '.join(args)}\n{result.stdout}{result.stderr}"
        )
    return result


def postgres(sql, *, plain=False, separator="|"):
    args = [
        "docker", "compose", "exec", "-T", "postgres",
        "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "dwh", "-d", "dwh",
    ]
    if plain:
        args.extend(["-qAt", "-F", separator])
    return command(args, data=sql).stdout


def clickhouse(sql, *, query_id=None):
    args = [
        "docker", "compose", "exec", "-T", "clickhouse",
        "clickhouse-client", "--user", "dwh", "--password", "dwh",
        "--database", "dwh", "--multiquery",
    ]
    if query_id:
        args.extend(["--query_id", query_id])
    args.extend(["--query", sql])
    return command(args).stdout


def clickhouse_insert(table, csv_text):
    columns = (
        "trip_key, source_system, ride_id, rideable_type, started_at, ended_at, "
        "start_station_id, start_station_name, end_station_id, end_station_name, "
        "member_casual, duration_sec, fare_amount"
    )
    query = f"INSERT INTO {table} ({columns}) FORMAT CSV"
    args = [
        "docker", "compose", "exec", "-T", "clickhouse",
        "clickhouse-client", "--user", "dwh", "--password", "dwh",
        "--database", "dwh", "--query", query,
    ]
    command(args, data=csv_text)


def read_sql(path):
    return (PROJECT / path).read_text().strip().rstrip(";")


def save_evidence(name, text, *, show=True):
    EVIDENCE.mkdir(exist_ok=True)
    path = EVIDENCE / name
    path.write_text(text)
    if show:
        print(text)
    print(f"saved: {path.relative_to(PROJECT)}")

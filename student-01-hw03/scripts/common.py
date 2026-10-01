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
        text=isinstance(data, str) or data is None,
    )
    if check and result.returncode != 0:
        stderr = result.stderr.decode() if isinstance(result.stderr, bytes) else result.stderr
        raise RuntimeError(f"command failed: {' '.join(args)}\n{stderr}")
    return result


def psql(sql, *, database="dwh", service="postgres", user="dwh", plain=False, check=True):
    args = ["docker", "compose", "exec", "-T", service,
            "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", user, "-d", database]
    if plain:
        args.extend(["-qAt"])
    return command(args, data=sql, check=check)


def write_evidence(name, text):
    EVIDENCE.mkdir(exist_ok=True)
    path = EVIDENCE / name
    path.write_text(text)
    print(text)
    print(f"saved: {path.relative_to(PROJECT)}")


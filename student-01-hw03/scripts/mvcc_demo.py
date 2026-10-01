#!/usr/bin/env python3
"""Run the Read Committed visibility and same-row locking experiments."""

import subprocess
import time

from common import PROJECT, psql, write_evidence


def session(app_name, sql):
    args = [
        "docker", "compose", "exec", "-T", "-e", f"PGAPPNAME={app_name}",
        "postgres", "psql", "-X", "-v", "ON_ERROR_STOP=1",
        "-U", "dwh", "-d", "dwh",
    ]
    return subprocess.Popen(
        args, cwd=PROJECT, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True
    ), sql


def main():
    psql("UPDATE hw3.mvcc_demo SET note = 'original' WHERE id = 1;")

    a, a_sql = session("hw3_session_a", """
BEGIN;
SHOW transaction_isolation;
UPDATE hw3.mvcc_demo SET note = 'changed by session A' WHERE id = 1;
SELECT 'session A keeps the transaction open' AS step, pg_sleep(3);
COMMIT;
""")
    a.stdin.write(a_sql)
    a.stdin.close()
    a.stdin = None
    time.sleep(0.6)

    b, b_sql = session("hw3_session_b", """
BEGIN;
SHOW transaction_isolation;
SELECT 'before A commits' AS step, note FROM hw3.mvcc_demo WHERE id = 1;
SELECT pg_sleep(4);
SELECT 'after A commits' AS step, note FROM hw3.mvcc_demo WHERE id = 1;
COMMIT;
""")
    b_out, b_err = b.communicate(b_sql)
    a_out, a_err = a.communicate()

    psql("UPDATE hw3.mvcc_demo SET note = 'lock test' WHERE id = 1;")
    lock_a, lock_a_sql = session("hw3_lock_holder", """
BEGIN;
UPDATE hw3.mvcc_demo SET note = 'held by A' WHERE id = 1;
SELECT pg_sleep(4);
COMMIT;
""")
    lock_a.stdin.write(lock_a_sql)
    lock_a.stdin.close()
    lock_a.stdin = None
    time.sleep(0.6)

    lock_b, lock_b_sql = session("hw3_lock_waiter", """
BEGIN;
SET LOCAL statement_timeout = '2500ms';
UPDATE hw3.mvcc_demo SET note = 'wanted by B' WHERE id = 1;
COMMIT;
""")
    lock_b.stdin.write(lock_b_sql)
    lock_b.stdin.close()
    lock_b.stdin = None
    time.sleep(0.6)

    monitor = psql("""
SELECT application_name, state, wait_event_type, wait_event,
       pg_blocking_pids(pid) AS blocking_pids
FROM pg_stat_activity
WHERE application_name IN ('hw3_lock_holder', 'hw3_lock_waiter')
ORDER BY application_name;
""").stdout

    lock_b_out, lock_b_err = lock_b.communicate()
    lock_a_out, lock_a_err = lock_a.communicate()

    text = (
        "HW3 TWO-SESSION EVIDENCE\n\n"
        "VISIBILITY EXPERIMENT\n"
        "Session A output:\n" + a_out + a_err + "\n"
        "Session B output:\n" + b_out + b_err + "\n"
        "LOCK EXPERIMENT\n"
        "Monitor output while B waits:\n" + monitor + "\n"
        "Session B same-row UPDATE output:\n" + lock_b_out + lock_b_err + "\n"
        "Session A lock-holder output:\n" + lock_a_out + lock_a_err + "\n"
        f"Waiting UPDATE exit code (timeout is expected): {lock_b.returncode}\n"
    )
    write_evidence("two_sessions.txt", text)


if __name__ == "__main__":
    main()

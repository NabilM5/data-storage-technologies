-- Run the Session A block in one psql window.
BEGIN;
SHOW transaction_isolation;
UPDATE hw3.mvcc_demo SET note = 'changed by session A' WHERE id = 1;
SELECT pg_sleep(10);
COMMIT;

-- During the sleep, run these statements in Session B.
BEGIN;
SHOW transaction_isolation;
SELECT * FROM hw3.mvcc_demo WHERE id = 1;
-- Run this SELECT again after Session A commits.
SELECT * FROM hw3.mvcc_demo WHERE id = 1;
COMMIT;


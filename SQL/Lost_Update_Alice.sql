-- Lost Update -- Alice

BEGIN;

SELECT * FROM accounts WHERE id = 1;

UPDATE accounts SET balance = 1000 - 100 WHERE id = 1;

COMMIT;
-- Bob: commit

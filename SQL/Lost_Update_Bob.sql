-- Lost Update -- Bob

BEGIN;

SELECT * FROM accounts WHERE id = 1;

-- Alice: update
UPDATE accounts SET balance = 1000 + 200 WHERE id = 1;

-- Alice: commit
COMMIT;
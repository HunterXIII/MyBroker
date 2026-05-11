-- Phantom Read -- Alice

BEGIN;

SELECT * FROM accounts WHERE balance > 800;
-- Bob: changed
SELECT * FROM accounts WHERE balance > 800;

COMMIT;

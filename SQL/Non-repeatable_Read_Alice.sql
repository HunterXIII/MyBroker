-- Non-repeatable Read -- Alice

BEGIN;

SELECT * FROM accounts order by id;
-- Bob: update and commit
SELECT * FROM accounts order by id;

COMMIT;
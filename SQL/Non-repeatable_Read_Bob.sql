-- Non-repeatable Read -- Bob

BEGIN;
-- Alice: select
UPDATE accounts SET balance = 1200 WHERE id = 1;
COMMIT;
-- Alice: select 
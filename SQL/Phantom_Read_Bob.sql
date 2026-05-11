-- Phantom Read -- Bob

BEGIN;

INSERT INTO accounts (name, balance) VALUES ('Artem', 900);
COMMIT;
-- Alice: select
-- init
CREATE TABLE accounts (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    balance DECIMAL(10, 2)
);

INSERT INTO accounts (name, balance) VALUES ('Alice', 1000.00), ('Bob', 500.00);

select * from accounts

-- RETURN
update accounts set balance = 1000 where id = 1;
update accounts set balance = 500 where id = 2;

delete from accounts where id = 3
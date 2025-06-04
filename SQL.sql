BEGIN;

INSERT INTO customers (name, address, phone, ssn)
VALUES ('Anna', 'Address 1', '0701234567', '9001011268');

INSERT INTO customers (name, address, phone, ssn)
VALUES ('Bosse', 'Address 2', '0707654321', '9001011268');

INSERT INTO customers (name, address, phone, ssn)
VALUES ('Charlie', 'Address 3', '0705555555', '9010101010');

ROLLBACK;

COMMIT;
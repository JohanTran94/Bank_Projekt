BEGIN;

INSERT INTO customers (name, address, phone, ssn)
VALUES ('Alice', '123 Main St', '0701234567', '9001011289');

COMMIT;

BEGIN;
DELETE FROM customers WHERE ssn = '9001011289';
COMMIT;
BEGIN;

INSERT INTO customers (name, address, phone, ssn, migration_run_id)
VALUES ('Anna', 'Address 1', '0701234567', '9001011268', 'a1f480eb-b260-418d-9141-aa31f7d220c2');

INSERT INTO customers (name, address, phone, ssn, migration_run_id)
VALUES ('Bosse', 'Address 2', '0707654321', '9001011268', 'a1f480eb-b260-418d-9141-aa31f7d220c2');

INSERT INTO customers (name, address, phone, ssn, migration_run_id)
VALUES ('Charlie', 'Address 3', '0705555555', '9010101010', 'a1f480eb-b260-418d-9141-aa31f7d220c2');

ROLLBACK;

COMMIT;
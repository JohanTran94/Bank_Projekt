BEGIN;

INSERT INTO customers (name, address, phone, ssn, migration_run_id)
VALUES ('Anna', 'Address 1', '0701234567', '9001011268', '83737373773');

INSERT INTO customers (name, address, phone, ssn, migration_run_id)
VALUES ('Bosse', 'Address 2', '0707654321', '9001011268', '83737373773');

INSERT INTO customers (name, address, phone, ssn, migration_run_id)
VALUES ('Charlie', 'Address 3', '0705555555', '9010101010', '83737373773');

ROLLBACK;

COMMIT;
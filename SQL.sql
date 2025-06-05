BEGIN;

INSERT INTO customers (name, address, phone, ssn, migration_run_id)
VALUES ('Anna', 'Address 1', '0701234567', '9001011268', 'f6fb4d1d-4d06-4959-9ec6-db0f83e2859d');

INSERT INTO customers (name, address, phone, ssn, migration_run_id)
VALUES ('Bosse', 'Address 2', '0707654321', '9001011268', 'f6fb4d1d-4d06-4959-9ec6-db0f83e2859d');

INSERT INTO customers (name, address, phone, ssn, migration_run_id)
VALUES ('Charlie', 'Address 3', '0705555555', '9010101010', 'f6fb4d1d-4d06-4959-9ec6-db0f83e2859d');

ROLLBACK;

COMMIT;
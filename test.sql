BEGIN;


INSERT INTO customers (name, address, phone, ssn)
VALUES ('MaxMAX', 'Testvägen 5', '+46123456789', '2333-1234');


ROLLBACK;


BEGIN;

INSERT INTO customers (name, address, phone, ssn)
VALUES ('MaxMAX', 'Testvägen 5', '+46123456789', '2345-3656');
COMMIT;
ROLLBACK;
BEGIN;

-- Sätt in en ny kund
INSERT INTO customers (name, address, phone, ssn)
VALUES ('Maxmaxmax', 'Testvägen 2', '+46123456789', '2323-1234');

-- Sätt in tillhörande konto
INSERT INTO accounts (account_number, customer_id)
VALUES ('SE8902001234567890433456', (SELECT id FROM customers WHERE ssn = '1234-2222'));

-- Om vi kommit hit, commit
COMMIT;

ROLLBACK;
-- Annars fångas felet nedan

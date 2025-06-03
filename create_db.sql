create table banks
(
    id    serial
        primary key,
    name  text not null,
    banknr text not null
        unique
);

create table customers
(
    id    serial
        primary key,
    name  text not null,
    ssn  text not null
        unique,
    approved boolean not null default false
);

create table accounts
(
    id    serial
        primary key,
    customer int not null,
    bank int not null,
    type text not null,
    nr text not null
        unique,
    credit int not null default 0
);

create table transactions
(
    id    serial
        primary key,
    amount  int not null default 0,
    account_nr text not null,
    time TIMESTAMP DEFAULT now()
);
BEGIN;

-- Sätt in en ny kund
INSERT INTO customers (name, address, phone, ssn)
VALUES ('Test Testsson', 'Testvägen 1', '+46123456789', '19900101-1234');

-- Sätt in tillhörande konto
INSERT INTO accounts (account_number, customer_id)
VALUES ('SE8902001234567890123456', (SELECT id FROM customers WHERE ssn = '19900101-1234'));

-- Medvetet fel: lägg till en transaktion med ogiltig kolumn (för att trigga rollback)
INSERT INTO transactions (transaction_id, timestamp, amount, currency, sender_account, receiver_account, transaction_type, location_id, notes, invalid_column)
VALUES ('TX123', now(), 100.00, 'SEK', 'SE8902001234567890123456', 'SE8902001234567890123456', 'Transfer', NULL, 'Test', 'fail');

-- Om vi kommit hit, commit
COMMIT;

-- Annars fångas felet nedan

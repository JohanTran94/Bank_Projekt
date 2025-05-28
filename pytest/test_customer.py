import pytest
from unittest.mock import patch, MagicMock
from customer import Customer
from account import Account

@pytest.fixture
def mock_conn_cursor():
    with patch('customer.Db.get_conn') as mock_get_conn:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn
        yield mock_conn, mock_cursor

@pytest.fixture
def customer_instance(mock_conn_cursor):
    cust = Customer()
    return cust

def test_create_customer_success(mock_conn_cursor, customer_instance):
    mock_conn, mock_cursor = mock_conn_cursor

    mock_cursor.fetchone.return_value = (1, 'Test Name', '1234567890')
    customer_instance.get = MagicMock(return_value=customer_instance)

    result = customer_instance.create('Test Name', '1234567890')

    mock_cursor.execute.assert_called_with(
        "INSERT INTO customers (name, ssn) VALUES (%s, %s)",
        ['Test Name', '1234567890']
    )
    customer_instance.get.assert_called_with('1234567890')
    assert result == customer_instance

def test_get_customer_found(mock_conn_cursor, customer_instance):
    mock_conn, mock_cursor = mock_conn_cursor

    mock_cursor.fetchone.return_value = (1, 'Test Name', '1234567890')
    customer_instance.get_accounts = MagicMock(return_value=['acc1', 'acc2'])

    result = customer_instance.get('1234567890')

    mock_cursor.execute.assert_called_with(
        "SELECT * FROM customers WHERE ssn = %s",
        ['1234567890']
    )
    assert result.id == 1
    assert result.name == 'Test Name'
    assert result.ssn == '1234567890'
    assert result.accounts == ['acc1', 'acc2']

def test_get_customer_not_found(mock_conn_cursor, customer_instance):
    mock_conn, mock_cursor = mock_conn_cursor

    mock_cursor.fetchone.return_value = None
    result = customer_instance.get('notexist')

    assert result is None

def test_get_accounts(mock_conn_cursor, customer_instance):
    mock_conn, mock_cursor = mock_conn_cursor

    customer_instance.id = 1

    mock_cursor.fetchall.return_value = [
        (1, 1, 1, 'Personal', '1111-1111', 0),
        (2, 1, 1, 'Savings', '1111-2222', 0)
    ]

    with patch('customer.Account.get') as mock_get:
        mock_get.side_effect = lambda nr: f"Account-{nr}"
        accounts = customer_instance.get_accounts()

    assert accounts == ['Account-1111-1111', 'Account-1111-2222']

import pytest
from unittest.mock import MagicMock, patch
from bank import Bank
from customer import Customer
from account import Account


@pytest.fixture
def mock_conn_cursor():
    with patch('bank.Db.get_conn') as mock_get_conn:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn
        yield mock_conn, mock_cursor


@pytest.fixture
def bank_instance(mock_conn_cursor):
    return Bank()


def test_create_bank_success(mock_conn_cursor, bank_instance):
    mock_conn, mock_cursor = mock_conn_cursor
    mock_cursor.fetchone.return_value = (1, "TestBank", "9999")

    bank = bank_instance.create("TestBank", "9999")

    mock_cursor.execute.assert_any_call(
        "INSERT INTO banks (name, banknr) VALUES (%s, %s)", ["TestBank", "9999"]
    )
    assert bank.name == "TestBank"
    assert bank.banknr == "9999"
    assert bank.id == 1


def test_get_existing_bank(mock_conn_cursor, bank_instance):
    mock_conn, mock_cursor = mock_conn_cursor
    mock_cursor.fetchone.return_value = (2, "ExistingBank", "1234")

    bank = bank_instance.get("1234")

    mock_cursor.execute.assert_called_with(
        "SELECT * FROM banks WHERE banknr = %s", ["1234"]
    )
    assert bank.name == "ExistingBank"
    assert bank.banknr == "1234"
    assert bank.id == 2


def test_get_non_existing_bank(mock_conn_cursor, bank_instance):
    mock_conn, mock_cursor = mock_conn_cursor
    mock_cursor.fetchone.return_value = None

    bank = bank_instance.get("0000")

    assert bank is None


@patch.object(Account, 'create')
def test_add_account_calls_account_create(mock_account_create, mock_conn_cursor, bank_instance):
    mock_customer = MagicMock(id=5, ssn="5555")
    mock_account = MagicMock(nr="1234-5555")
    mock_account_create.return_value = mock_account

    # Fake internal state
    bank_instance.id = 1
    bank_instance.name = "TestBank"
    bank_instance.banknr = "1234"

    account = bank_instance.add_account(mock_customer, "Personal_account", mock_customer.ssn)

    mock_account_create.assert_called_once_with(mock_customer, bank_instance, "Personal_account", "5555")
    assert account.nr == "1234-5555"
    assert account in bank_instance.accounts


@patch.object(Bank, 'add_account')
def test_add_customer_adds_customer_and_account(mock_add_account, mock_conn_cursor, bank_instance):
    mock_customer = MagicMock(id=10, ssn="1010101010")
    mock_add_account.return_value = MagicMock(nr="1234-1010101010")

    # Fake internal state
    bank_instance.id = 1
    bank_instance.name = "TestBank"
    bank_instance.banknr = "1234"

    customer = bank_instance.add_customer(mock_customer)

    assert customer in bank_instance.customers
    mock_add_account.assert_called_once_with(mock_customer, "Personal_account", "1010101010")

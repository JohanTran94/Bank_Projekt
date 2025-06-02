import pytest
from unittest.mock import MagicMock, patch
from account import Account
from transaction import Transaction

@pytest.fixture
def mock_conn_cursor():
    with patch('account.Db.get_conn') as mock_get_conn:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn
        yield mock_conn, mock_cursor

@pytest.fixture
def account_instance(mock_conn_cursor):
    acc = Account()
    return acc

def test_generate_nr():
    nr = Account.generate_nr()
    assert isinstance(nr, str)
    assert len(nr) == 10

def test_create_account_new(mock_conn_cursor, account_instance):
    mock_conn, mock_cursor = mock_conn_cursor

    mock_cursor.execute.return_value = None
    mock_cursor.fetchone.return_value = (1, 1, 1, "Personal_account", "1234-5678901234", 0)
    mock_conn.commit.return_value = None

    result = account_instance.create(customer=MagicMock(id=1), bank=MagicMock(id=1, banknr="1234"), type="Personal_account", nr="5678901234")


    mock_cursor.execute.assert_any_call(
        "INSERT INTO accounts (customer, bank, type, nr, credit) VALUES (%s, %s, %s, %s, %s)",
        [1, 1, "Personal_account", "1234-5678901234", 0]
    )

    assert result.nr == "1234-5678901234"

def test_get_account_found(mock_conn_cursor, account_instance):
    mock_conn, mock_cursor = mock_conn_cursor
    mock_cursor.fetchone.side_effect = [
        (1, 1, 1, "Personal_account", "1234-5678901234", 0),
        None
    ]
    mock_cursor.fetchall.return_value = []

    account = account_instance.get("1234-5678901234")

    assert account is not None
    assert account.nr == "1234-5678901234"
    assert account.balance == 0

    result = account_instance.get("nonexistent")
    assert result is None


@patch.object(Transaction, 'create')
def test_deposit_calls_transaction_create(mock_tx_create, mock_conn_cursor, account_instance):
    mock_tx_create.return_value = None
    account_instance.nr = "1234-5678901234"
    account_instance.get_transactions = MagicMock(return_value=[])
    account_instance.deposit(100)

    mock_tx_create.assert_called_once_with(100, account_instance)

@patch.object(Transaction, 'create')
def test_withdraw_enough_balance(mock_tx_create, mock_conn_cursor, account_instance):
    mock_tx_create.return_value = None
    account_instance.nr = "1234-5678901234"
    # Giả lập số dư 200, credit 100
    account_instance.get_transactions = MagicMock(return_value=[{"amount": 200}])
    account_instance.credit = 100

    withdrawn = account_instance.withdraw(250)
    mock_tx_create.assert_called_once_with(-250, account_instance)
    assert withdrawn == -250

@patch.object(Transaction, 'create')
def test_withdraw_not_enough_balance(mock_tx_create, mock_conn_cursor, account_instance):
    mock_tx_create.return_value = None
    account_instance.nr = "1234-5678901234"
    account_instance.get_transactions = MagicMock(return_value=[{"amount": 100}])
    account_instance.credit = 50

    withdrawn = account_instance.withdraw(200)
    mock_tx_create.assert_not_called()
    assert withdrawn == 0

def test_get_balance_calculation(mock_conn_cursor, account_instance):
    mock_conn, mock_cursor = mock_conn_cursor
    mock_cursor.fetchall.return_value = [
        (1, 100, "1234-5678901234"),
        (2, -20, "1234-5678901234"),
        (3, 50, "1234-5678901234")
    ]
    account_instance.nr = "1234-5678901234"
    balance = account_instance.get_balance()

    assert balance == 130
    assert account_instance.balance == 130

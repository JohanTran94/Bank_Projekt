import pytest
from unittest.mock import MagicMock, patch
from transaction import Transaction
from account import Account

@pytest.fixture
def mock_conn_cursor():
    with patch('transaction.Db.get_conn') as mock_get_conn:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn
        yield mock_conn, mock_cursor

@pytest.fixture
def account_instance():
    acc = MagicMock()
    acc.nr = '1234567890'
    return acc

def test_create_transaction_success(mock_conn_cursor, account_instance):
    mock_conn, mock_cursor = mock_conn_cursor
    transaction = Transaction()

    amount = 100
    result = transaction.create(amount, account_instance)

    mock_cursor.execute.assert_called_once_with(
        "INSERT INTO transactions (amount, account_nr) VALUES (%s, %s)",
        [amount, account_instance.nr]
    )
    mock_conn.commit.assert_called_once()
    assert result == amount

def test_create_transaction_exception(mock_conn_cursor, account_instance, capsys):
    mock_conn, mock_cursor = mock_conn_cursor
    mock_cursor.execute.side_effect = Exception("DB error")

    transaction = Transaction()
    amount = 200
    result = transaction.create(amount, account_instance)

    mock_conn.commit.assert_not_called()
    captured = capsys.readouterr()
    assert "[Warning] Transaction blocked due to constraint violation" in captured.out
    assert result == amount

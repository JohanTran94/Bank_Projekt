import pytest
from unittest.mock import MagicMock, patch
from account import Account

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
    return Account()

# ✅ Test: Tạo account thành công
def test_create_account_success(mock_conn_cursor, account_instance):
    mock_conn, mock_cursor = mock_conn_cursor
    mock_cursor.fetchone.return_value = [1]  # giả sử trả về id mới
    result = account_instance.create(1000, '1234567890')
    assert result == 1
    mock_cursor.execute.assert_called_with(
        "INSERT INTO accounts (balance, ssn) VALUES (%s, %s) RETURNING id",
        [1000, '1234567890']
    )

# ✅ Test: Lấy balance khi account tồn tại
def test_get_balance_success(mock_conn_cursor, account_instance):
    mock_conn, mock_cursor = mock_conn_cursor
    mock_cursor.fetchone.return_value = [1500]
    result = account_instance.get_balance(1)
    assert result == 1500
    mock_cursor.execute.assert_called_with(
        "SELECT balance FROM accounts WHERE id = %s", [1]
    )

# ✅ Test: Lấy balance khi account không tồn tại
def test_get_balance_not_found(mock_conn_cursor, account_instance):
    mock_conn, mock_cursor = mock_conn_cursor
    mock_cursor.fetchone.return_value = None
    result = account_instance.get_balance(999)
    assert result is None

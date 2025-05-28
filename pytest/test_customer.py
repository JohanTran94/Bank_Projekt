import pytest
from unittest.mock import patch, MagicMock
from customer import Customer

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
    customer = Customer()
    return customer

def test_create_customer(mock_conn_cursor, customer_instance):
    mock_conn, mock_cursor = mock_conn_cursor
    customer_instance.create_customer('John Doe', 'john@example.com')

    mock_cursor.execute.assert_called_once_with(
        "INSERT INTO customers (name, email) VALUES (%s, %s)",
        ('John Doe', 'john@example.com')
    )
    mock_conn.commit.assert_called_once()

def test_get_customer_by_id(mock_conn_cursor, customer_instance):
    mock_conn, mock_cursor = mock_conn_cursor
    mock_cursor.fetchone.return_value = (1, 'John Doe', 'john@example.com')

    result = customer_instance.get_customer_by_id(1)

    mock_cursor.execute.assert_called_once_with(
        "SELECT * FROM customers WHERE id = %s", (1,)
    )
    assert result == (1, 'John Doe', 'john@example.com')

def test_update_customer_email(mock_conn_cursor, customer_instance):
    mock_conn, mock_cursor = mock_conn_cursor

    customer_instance.update_customer_email(1, 'new@example.com')

    mock_cursor.execute.assert_called_once_with(
        "UPDATE customers SET email = %s WHERE id = %s",
        ('new@example.com', 1)
    )
    mock_conn.commit.assert_called_once()

def test_delete_customer(mock_conn_cursor, customer_instance):
    mock_conn, mock_cursor = mock_conn_cursor

    customer_instance.delete_customer(1)

    mock_cursor.execute.assert_called_once_with(
        "DELETE FROM customers WHERE id = %s", (1,)
    )
    mock_conn.commit.assert_called_once()

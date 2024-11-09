from src.main import get_petstore_inventory, clean_stats, insert_results, collect_petstore_info
# from prefect.context import FlowRunContext
from prefect.testing.utilities import prefect_test_harness
import pytest
from unittest.mock import patch, MagicMock


def test_clean_stats_data():
    test_input = {
        "sold": 8,
        "{PetStatus-Updated}": 1,
        "string": 758,
        "pending": 1,
        "Pending": 1,
        "available": 227
    }

    with prefect_test_harness():
        result_output = clean_stats.fn(test_input)
        assert result_output == {
            "sold": 8,
            "pending": 2,
            "available": 227,
            "unavailable": 0
        }

@pytest.fixture
def sample_api_response():
    return {
        "sold": 10,
        "Sold": 5,
        "pending": 2,
        "Pending": 1,
        "available": 8,
        "Available": 3,
        "not available": 1,
        "Unavailable": 0
    }

@pytest.fixture
def cleaned_data():
    return {
        "sold": 15,
        "pending": 3,
        "available": 11,
        "unavailable": 1
    }

@patch("src.main.httpx.get")
def test_get_petstore_inventory(mock_get, sample_api_response):
    mock_get.return_value.json.return_value = sample_api_response
    mock_get.return_value.status_code = 200

    result = get_petstore_inventory.fn()
    assert result == sample_api_response
    mock_get.assert_called_once_with("https://petstore.swagger.io/v2/store/inventory")

def test_clean_stats(sample_api_response, cleaned_data):
    result = clean_stats.fn(sample_api_response)
    assert result == cleaned_data

@patch("psycopg2.connect")
def test_insert_results(mock_connect, cleaned_data):
    mock_conn = MagicMock()
    mock_connect.return_value.__enter__.return_value = mock_conn

    insert_results.fn(cleaned_data, "user", "pass", "db", "localhost")

    mock_connect.assert_called_once_with(user="user", password="pass", dbname="db", host="localhost")
    mock_conn.cursor().__enter__().execute.assert_called_once_with(
        """
insert into inventory_history (
    fetch_timestamp,
    sold,
    pending,
    available,
    unavailable
) values (now(), %(sold)s, %(pending)s, %(available)s, %(unavailable)s)
        """.strip(),  # Strip extra whitespace to match exactly
        cleaned_data,
    )

@patch("src.main.get_petstore_inventory")
@patch("src.main.clean_stats")
@patch("src.main.insert_results")
def test_collect_petstore_info(mock_insert, mock_clean, mock_get, sample_api_response, cleaned_data):
    mock_get.return_value = sample_api_response
    mock_clean.return_value = cleaned_data

    collect_petstore_info()

    mock_get.assert_called_once()
    mock_clean.assert_called_once_with(sample_api_response)
    mock_insert.assert_called_once_with(
        cleaned_data, "root", "root", "petstore", "localhost"
    )
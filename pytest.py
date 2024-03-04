import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from api_data import get_article, get_price_cart, create_record_bd, created_api
import datetime

# Constants for use in tests
API_RESPONSE = {
    "cards": [
        {
            "nmID": "12345678",
            "title": "Test Title",
            "description": "Test Description",
            "characteristics": "Test Characteristics"
        }
    ]
}
PRICE_API_RESPONSE = {
    "data": {
        "products": [
            {
                "salePriceU": 10000
            }
        ]
    }
}
DB_RECORD = {
    "id": [1],
    "article": ["12345678"],
    "title": ["Previous Title"],
    "description": ["Previous Description"],
    "characteristics": ["Previous Characteristics"],
    "price": [500]
}

# Mocks for database and requests
engine_mock = MagicMock()
requests_mock = MagicMock()
connection_mock = MagicMock()
cursor_mock = MagicMock()

# Patch the functions that interact with external systems
@pytest.fixture(autouse=True)
def mock_external_systems(monkeypatch):
    monkeypatch.setattr("api_data.get_engine_from_settings", lambda: engine_mock)
    monkeypatch.setattr("api_data.connect_bd", lambda: connection_mock)
    monkeypatch.setattr("requests.post", requests_mock)
    monkeypatch.setattr("requests.get", requests_mock)
    connection_mock.cursor.return_value = cursor_mock
    cursor_mock.__enter__.return_value = cursor_mock

# Parametrized test for get_article function
@pytest.mark.parametrize(
    "api_data, expected_calls, test_id",
    [
        # Happy path test with realistic values
        (
            pd.DataFrame({
                "id": [1],
                "api": ["api_key_1"]
            }),
            1,
            "happy_path"
        ),
        # Edge case with empty API key
        (
            pd.DataFrame({
                "id": [1],
                "api": [""]
            }),
            0,
            "edge_case_empty_api_key"
        ),
        # Error case with invalid API response
        (
            pd.DataFrame({
                "id": [1],
                "api": ["api_key_1"]
            }),
            0,
            "error_case_invalid_api_response"
        ),
    ],
    ids=["happy_path", "edge_case_empty_api_key", "error_case_invalid_api_response"]
)
def test_get_article(api_data, expected_calls, test_id, mock_external_systems):
    # Arrange
    engine_mock.execute.return_value.fetchall.return_value = api_data
    requests_mock.post.return_value.json.return_value = API_RESPONSE
    requests_mock.get.return_value.json.return_value = PRICE_API_RESPONSE
    pd.read_sql.return_value = pd.DataFrame(DB_RECORD)

    # Act
    get_article()

    # Assert
    assert requests_mock.post.call_count == expected_calls

# Parametrized test for get_price_cart function
@pytest.mark.parametrize(
    "list_article, expected_calls, test_id",
    [
        # Happy path test with realistic values
        (
            ["12345678"],
            1,
            "happy_path"
        ),
        # Edge case with no articles
        (
            [],
            0,
            "edge_case_no_articles"
        ),
        # Error case with invalid price response
        (
            ["12345678"],
            0,
            "error_case_invalid_price_response"
        ),
    ],
    ids=["happy_path", "edge_case_no_articles", "error_case_invalid_price_response"]
)
def test_get_price_cart(list_article, expected_calls, test_id, mock_external_systems):
    # Arrange
    pd.read_sql.return_value = pd.DataFrame(DB_RECORD)

    # Act
    get_price_cart([], list_article, [], [], [])

    # Assert
    assert requests_mock.get.call_count == expected_calls

# Parametrized test for create_record_bd function
@pytest.mark.parametrize(
    "list_id, list_article, list_title, list_description, list_characteristics, list_price, expected_query, test_id",
    [
        # Happy path test with realistic values
        (
            [1],
            ["12345678"],
            ["Test Title"],
            ["Test Description"],
            ["Test Characteristics"],
            ["100"],
            "INSERT INTO users_api_data",
            "happy_path"
        ),
        # Edge case with empty lists
        (
            [],
            [],
            [],
            [],
            [],
            [],
            "INSERT INTO users_api_data",
            "edge_case_empty_lists"
        ),
    ],
    ids=["happy_path", "edge_case_empty_lists"]
)
def test_create_record_bd(list_id, list_article, list_title, list_description, list_characteristics, list_price, expected_query, test_id, mock_external_systems):
    # Arrange
    connection_mock.autocommit = True

    # Act
    create_record_bd(list_id, list_article, list_title, list_description, list_characteristics, list_price)

    # Assert
    cursor_mock.execute.assert_called_with(expected_query)

# Parametrized test for created_api function
@pytest.mark.parametrize(
    "api_key, user_id, expected_query, test_id",
    [
        # Happy path test with realistic values
        (
            "api_key_1",
            1,
            "UPDATE user_api SET api",
            "happy_path"
        ),
        # Edge case with empty API key
        (
            "",
            1,
            "UPDATE user_api SET api",
            "edge_case_empty_api_key"
        ),
    ],
    ids=["happy_path", "edge_case_empty_api_key"]
)
def test_created_api(api_key, user_id, expected_query, test_id, mock_external_systems):
    # Arrange
    connection_mock.autocommit = True

    # Act
    created_api()

    # Assert
    cursor_mock.execute.assert_called_with(expected_query, [api_key, user_id]) 
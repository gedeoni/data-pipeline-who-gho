from unittest.mock import MagicMock
import pytest
from etl.extract import ODataClient

@pytest.fixture
def mock_state_repo():
    """Fixture for a mock state repository."""
    return MagicMock()

def test_odata_client_pagination(mocker, mock_state_repo):
    """Tests that the OData client correctly handles pagination."""

    mock_response_1 = {
        "value": [{"id": 1}, {"id": 2}]
    }
    mock_response_2 = {
        "value": [{"id": 3}, {"id": 4}]
    }

    mocker.patch(
        "httpx.Client.get",
        side_effect=[
            MagicMock(status_code=200, json=lambda: mock_response_1),
            MagicMock(status_code=200, json=lambda: mock_response_2),
            MagicMock(status_code=200, json=lambda: {"value": []}),
        ]
    )

    client = ODataClient("http://test.com/api", state_repo=mock_state_repo)

    all_data = list(client.get_all_data("entity", "test_process", page_size=2))

    assert len(all_data) == 4
    assert all_data[0]["id"] == 1
    assert all_data[3]["id"] == 4

    # Check that checkpoint was set
    mock_state_repo.set_checkpoint_state.assert_any_call(
        "test_process", {"next_link": "http://test.com/api/entity?%24top=2&%24skip=2"}
    )
    # Check that checkpoint is cleared at the end
    mock_state_repo.set_checkpoint_state.call_args_list[-1].assert_called_with("test_process", {})


def test_odata_client_resumes_from_checkpoint(mocker, mock_state_repo):
    """Tests that the OData client resumes from a checkpoint."""

    mock_state = MagicMock()
    mock_state.checkpoint_state = {"next_link": "http://test.com/api/entity?$top=2&$skip=2"}
    mock_state_repo.get_state.return_value = mock_state

    mock_response = {
        "value": [{"id": 3}]
    }

    mock_get = mocker.patch(
        "httpx.Client.get",
        return_value=MagicMock(status_code=200, json=lambda: mock_response)
    )

    client = ODataClient("http://test.com/api", state_repo=mock_state_repo)

    all_data = list(client.get_all_data("entity", "test_process", page_size=2))

    assert len(all_data) == 1
    assert all_data[0]["id"] == 3

    # Assert that the request was made to the resumed URL
    mock_get.assert_called_once()
    called_url = mock_get.call_args[0][0]
    assert called_url == "http://test.com/api/entity?$top=2&$skip=2"

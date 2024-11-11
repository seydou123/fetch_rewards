from unittest import mock

from user_login import USER_LOGIN_SCHEMA
from user_login.consume import format_data


@mock.patch('confluent_kafka.Producer')
def test_valid_message(mock_producer):
    """
    For a valid message, ensures all expected fields are in the returned processed message
    """
    expected_fields = [
        "user_id",
        "app_version",
        "device_type",
        "ip",
        "locale",
        "device_id",
        "created_at",
        "processed_at",
    ]
    msg = {
        "user_id": "424cdd21-063a-43a7-b91b-7ca1a833afae",
        "app_version": "2.3.0",
        "device_type": "android",
        "ip": "199.172.111.135",
        "locale": "RU",
        "device_id": "593-47-5928",
        "timestamp": 1694479551,
    }
    processed_msg = format_data(msg, USER_LOGIN_SCHEMA, mock_producer)
    assert all(field in processed_msg for field in expected_fields)


@mock.patch('confluent_kafka.Producer')
@mock.patch('user_login.consume.redirect_bad_messages')
def test_invalid_message(mock_redirect_bad_messages, mock_producer):
    """
    1) Ensures an invalid message returns None
    2) Ensures an invalid message triggers a call to redirect_bad_message()
    """
    msg = {
        "user_id": None,
        "app_version": "2.3.0",
        "device_type": "android",
        "ip": "199.172.111.135",
        "locale": "RU",
        "device_id": "593-47-5928",
        "timestamp": 1694479551,
    }

    processed_msg = format_data(msg, USER_LOGIN_SCHEMA, mock_producer)
    assert mock_redirect_bad_messages.called
    assert not processed_msg

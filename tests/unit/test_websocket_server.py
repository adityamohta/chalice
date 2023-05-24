import asyncio
import collections
import json

import mock
import uuid
import pytest

from datetime import datetime, timezone
from typing import Optional

from pytest import fixture
from websockets.datastructures import Headers
from websockets.legacy.client import WebSocketClientProtocol

from chalice import app
from chalice.config import Config
from chalice.websocket_server import (
    create_websocket_server,
    WebSocketRequest,
    WebSocketClientConnection,
    WebSocketGateway,
    ChaliceWebsocketMessageHandler,
    ConnectionGoneException,
)

mock_timestamp_ms = int(
    datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000
)


@fixture
def sample_app():
    demo = app.Chalice("demo-app")
    demo.debug = True
    demo.experimental_feature_flags.update(["WEBSOCKETS"])
    demo.websocket_api.configure(domain_name="localhost", stage="api")

    @demo.on_ws_connect()
    def websocket_connect(event):
        return {"statusCode": 200, "route": "connect"}

    @demo.on_ws_disconnect()
    def websocket_disconnect(event):
        return {"statusCode": 200, "route": "disconnect"}

    @demo.on_ws_message()
    def websocket_message(event):
        return {"statusCode": 200, "route": "message"}

    return demo


@fixture
def connect_client():
    client = FakeWebSocketClientProtocol()
    client.id = mock.MagicMock(hex="connection-uuid")
    client_connection = WebSocketClientConnection(client)
    client_connection.connected_at = mock_timestamp_ms
    client_connection.last_active_at = mock_timestamp_ms
    return client_connection


@fixture
def connect_client_without_query():
    client = FakeWebSocketClientProtocol()
    client.id = mock.MagicMock(hex="connection-uuid")
    client.path = "/api"
    client_connection = WebSocketClientConnection(client)
    client_connection.connected_at = mock_timestamp_ms
    client_connection.last_active_at = mock_timestamp_ms
    return client_connection


@fixture
def disconnect_client():
    client = FakeWebSocketClientProtocol()
    client.id = mock.MagicMock(hex="connection-uuid")
    client.close_code = "1001"
    client.close_reason = ""
    client_connection = WebSocketClientConnection(client)
    client_connection.connected_at = mock_timestamp_ms
    client_connection.last_active_at = mock_timestamp_ms
    return client_connection


@fixture
def message_client():
    client = FakeWebSocketClientProtocol()
    client.id = mock.MagicMock(hex="connection-uuid")
    client_connection = WebSocketClientConnection(client)
    client_connection.connected_at = mock_timestamp_ms
    client_connection.last_active_at = mock_timestamp_ms
    return client_connection


@fixture
def connect_websocket_request(connect_client):
    mock.Mock(spec=uuid.uuid4)
    websocket_request = WebSocketRequest.from_connect_event(
        client=connect_client
    )
    websocket_request.message_id = "uuid"
    websocket_request.request_id = "uuid"
    websocket_request.request_time_epoch = mock_timestamp_ms
    return websocket_request


@fixture
def disconnect_websocket_request(disconnect_client):
    websocket_request = WebSocketRequest.from_disconnect_event(
        client=disconnect_client
    )
    websocket_request.message_id = "uuid"
    websocket_request.request_id = "uuid"
    websocket_request.request_time_epoch = mock_timestamp_ms
    return websocket_request


@fixture
def message_websocket_request(message_client):
    test_message = json.dumps({"message": "test"})
    websocket_request = WebSocketRequest.from_message_event(
        client=message_client, message=test_message
    )
    websocket_request.message_id = "uuid"
    websocket_request.request_id = "uuid"
    websocket_request.request_time_epoch = mock_timestamp_ms
    return websocket_request


class TestWebsocketRequest(object):
    def test_query_string_parsed_correctly(
        self, connect_client, connect_client_without_query
    ):
        request1 = WebSocketRequest.from_connect_event(connect_client)
        assert request1.query_string == "q=foo&q=bar"

        request2 = WebSocketRequest.from_connect_event(
            connect_client_without_query
        )
        assert request2.query_string == ""

    def test_websocket_request_can_create_lambda_event_correctly_on_connect(
        self, sample_app, connect_websocket_request
    ):
        assert connect_websocket_request.create_lambda_event() == {
            "headers": {
                "cache-control": "no-cache",
                "host": "localhost:8001",
                "origin": "test",
                "sec-websocket-version": "1",
                "user-agent": "test",
            },
            "isBase64Encoded": False,
            "multiValueHeaders": {
                "cache-control": ["no-cache"],
                "host": ["localhost:8001"],
                "origin": ["test"],
                "sec-websocket-version": ["1"],
                "user-agent": ["test"],
            },
            "multiValueQueryStringParameters": {"q": ["foo", "bar"]},
            "queryStringParameters": {"q": "foo"},
            "requestContext": {
                "apiId": "",
                "connectedAt": 1672531200000,
                "connectionId": "connection-uuid",
                "domainName": "",
                "eventType": "CONNECT",
                "extendedRequestId": "uuid",
                "identity": {"sourceIp": "localhost", "userAgent": "test"},
                "messageDirection": "IN",
                "requestId": "uuid",
                "requestTime": "01/01/2023:00:00:00 +0000",
                "requestTimeEpoch": 1672531200000,
                "routeKey": "$connect",
                "stage": "api",
            },
        }

    def test_websocket_request_can_create_lambda_event_correctly_on_disconnect(
        self, sample_app, disconnect_websocket_request
    ):
        assert disconnect_websocket_request.create_lambda_event() == {
            "headers": {
                "cache-control": "no-cache",
                "host": "localhost:8001",
                "origin": "test",
                "sec-websocket-version": "1",
                "user-agent": "test",
            },
            "isBase64Encoded": False,
            "multiValueHeaders": {
                "cache-control": ["no-cache"],
                "host": ["localhost:8001"],
                "origin": ["test"],
                "sec-websocket-version": ["1"],
                "user-agent": ["test"],
            },
            "requestContext": {
                "apiId": "",
                "connectedAt": 1672531200000,
                "connectionId": "connection-uuid",
                "disconnectReason": "",
                "disconnectStatusCode": "1001",
                "domainName": "",
                "eventType": "DISCONNECT",
                "extendedRequestId": "uuid",
                "identity": {"sourceIp": "localhost", "userAgent": "test"},
                "messageDirection": "IN",
                "requestId": "uuid",
                "requestTime": "01/01/2023:00:00:00 +0000",
                "requestTimeEpoch": 1672531200000,
                "routeKey": "$disconnect",
                "stage": "api",
            },
        }

    def test_websocket_request_can_create_lambda_event_correctly_on_message(
        self, sample_app, message_websocket_request
    ):
        assert message_websocket_request.create_lambda_event() == {
            "body": '{"message": "test"}',
            "isBase64Encoded": False,
            "requestContext": {
                "apiId": "",
                "connectedAt": 1672531200000,
                "connectionId": "connection-uuid",
                "domainName": "",
                "eventType": "MESSAGE",
                "extendedRequestId": "uuid",
                "identity": {"sourceIp": "localhost", "userAgent": "test"},
                "messageDirection": "IN",
                "messageId": "uuid",
                "requestId": "uuid",
                "requestTime": "01/01/2023:00:00:00 +0000",
                "requestTimeEpoch": 1672531200000,
                "routeKey": "$default",
                "stage": "api",
            },
        }


class TestWebSocketGateway(object):
    def test_gateway_handles_websocket_connect_event_correctly(
        self, sample_app, connect_websocket_request
    ):
        gateway = WebSocketGateway(app_object=sample_app, config=Config())
        response = gateway.handle_websocket_event(
            request=connect_websocket_request
        )
        assert response == {"statusCode": 200, "route": "connect"}

    def test_gateway_handles_websocket_disconnect_event_correctly(
        self, sample_app, disconnect_websocket_request
    ):
        gateway = WebSocketGateway(app_object=sample_app, config=Config())
        response = gateway.handle_websocket_event(
            request=disconnect_websocket_request
        )
        assert response == {"statusCode": 200, "route": "disconnect"}

    def test_gateway_handles_websocket_message_event_correctly(
        self, sample_app, message_websocket_request
    ):
        gateway = WebSocketGateway(app_object=sample_app, config=Config())
        response = gateway.handle_websocket_event(
            request=message_websocket_request
        )
        assert response == {"statusCode": 200, "route": "message"}


class TestChaliceWebsocketMessageHandler(object):
    def test_handlers_attached_to_server_correctly(self, sample_app):
        config = Config()
        dev_server = create_websocket_server(
            sample_app, config, "127.0.0.1", port=8001
        )
        message_handler = ChaliceWebsocketMessageHandler(
            dev_server.server, sample_app, config
        )

        assert (
            dev_server.server.chalice_connect_handler
            == message_handler.connect
        )
        assert (
            dev_server.server.chalice_disconnect_handler
            == message_handler.disconnect
        )
        assert (
            dev_server.server.chalice_message_handler
            == message_handler.default
        )


class TestWebsocketServer(object):
    def test_can_provide_port_to_local_server(self, sample_app):
        config = Config()
        dev_server = create_websocket_server(
            sample_app, config, "127.0.0.1", port=8001
        )
        assert dev_server.server.port == 8001

    def test_can_provide_host_to_local_server(self, sample_app):
        config = Config()
        dev_server = create_websocket_server(
            sample_app, config, host="0.0.0.0", port=23456
        )
        assert dev_server.host == "0.0.0.0"

    def test_raises_error_on_getting_gone_client(self, sample_app):
        config = Config()
        dev_server = create_websocket_server(
            sample_app, config, host="0.0.0.0", port=23456
        )
        with pytest.raises(ConnectionGoneException):
            dev_server.server.get_client_connection("invalid_connection_id")

    def test_add_client_correctly(self, sample_app, connect_client):
        config = Config()
        dev_server = create_websocket_server(
            sample_app, config, host="0.0.0.0", port=23456
        )
        server = dev_server.server

        assert len(server.connected_clients) == 0

        server.add_connection(connect_client)

        assert len(server.connected_clients) == 1
        assert (
            list(server.connected_clients)[0].connection_id
            == connect_client.connection_id
        )

    def test_get_client_correctly(self, sample_app, connect_client):
        config = Config()
        dev_server = create_websocket_server(
            sample_app, config, "127.0.0.1", port=8001
        )
        dev_server.server.add_connection(connect_client)
        assert (
            dev_server.server.get_client_connection("connection-uuid")
            == connect_client
        )

    def test_delete_client_correctly(self, sample_app, connect_client):
        config = Config()
        dev_server = create_websocket_server(
            sample_app, config, host="0.0.0.0", port=23456
        )
        dev_server.server.add_connection(connect_client)
        assert len(dev_server.server.connected_clients) == 1
        dev_server.server.delete_connection("connection-uuid")
        assert len(dev_server.server.connected_clients) == 0

    @pytest.mark.asyncio
    async def test_send_message_to_connection_correctly(
        self, sample_app, message_client
    ):
        config = Config()
        dev_server = create_websocket_server(
            sample_app, config, host="localhost", port=23456
        )
        test_message = json.dumps({"message": "test"})
        dev_server.server.add_connection(message_client)

        assert message_client.last_active_at == mock_timestamp_ms

        task = dev_server.server.send_message_to_connection(
            message_client.connection_id, test_message
        )

        await asyncio.wait_for(task, 2)
        assert message_client.last_active_at > mock_timestamp_ms

        assert len(message_client.client.messages) == 1
        assert message_client.client.messages.pop() == test_message


class FakeWebSocketClientProtocol(WebSocketClientProtocol):
    remote_address: str = ""
    path: str = "/api?q=foo&q=bar"
    request_headers: Headers = Headers(
        {
            "Host": "localhost:8001",
            "Cache-Control": "no-cache",
            "User-Agent": "test",
            "Origin": "test",
            "Sec-WebSocket-Version": "1",
        }
    )
    close_code: Optional[int] = None
    close_reason: Optional[str] = None
    messages = collections.deque()

    async def send(self, message) -> None:
        self.messages.append(message)

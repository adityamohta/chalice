import asyncio
import datetime
import logging
import time
import uuid

from asyncio import Task

from enum import Enum
from urllib.parse import parse_qs

from websockets.server import serve
from websockets.datastructures import Headers
from websockets.legacy.client import WebSocketClientProtocol
from typing import Callable, Optional, Set, Dict, List, Any
from typing_extensions import Literal

from chalice import Chalice
from chalice.app import EventSourceHandler, WebsocketEvent
from chalice.config import Config

logger = logging.getLogger(__name__)
logging.basicConfig()


class ConnectionGoneException(Exception):
    pass


class WebSocketExceptionFactory(object):
    GoneException = ConnectionGoneException


def parse_query_string(query_string: str) -> Dict[str, str]:
    parsed_data = parse_qs(query_string)
    result = {}
    for key, value in parsed_data.items():
        result[key] = value[0]
    return result


def parse_multi_query_string(query_string: str) -> Dict[str, List[str]]:
    return parse_qs(query_string)


def parse_headers(headers: Headers) -> Dict[str, str]:
    return dict(headers)


def parse_multi_headers(headers: Headers) -> Dict[str, List[str]]:
    parsed_headers: Dict[str, List[str]] = {}
    for key, value in headers.items():
        parsed_headers[key] = parsed_headers.get(key, [])
        parsed_headers[key].append(value)
    return parsed_headers


WebsocketRouteType = Literal["$connect", "$disconnect", "$default"]


class EventType(str, Enum):
    MESSAGE = "MESSAGE"
    CONNECT = "CONNECT"
    DISCONNECT = "DISCONNECT"


event_type_to_route_key_map: Dict[EventType, WebsocketRouteType] = {
    EventType.CONNECT: "$connect",
    EventType.DISCONNECT: "$disconnect",
    EventType.MESSAGE: "$default",
}


class WebSocketClientConnection:
    def __init__(self, client: WebSocketClientProtocol) -> None:
        self.client = client
        self.connected_at = int(time.time() * 1000)
        self.last_active_at = int(time.time() * 1000)

    @property
    def connection_id(self) -> str:
        return self.client.id.hex

    @property
    def identity(self) -> Dict[str, str]:
        return {
            "userAgent": self.client.request_headers.get("User-Agent"),
            "sourceIp": "localhost",
        }


class WebSocketRequest:
    def __init__(
        self,
        client: WebSocketClientConnection,
        event_type: EventType,
        message: Optional[str] = None,
    ) -> None:
        self._client = client
        self.event_type = event_type
        self.request_time_epoch = int(time.time() * 1000)
        self.request_id = uuid.uuid4().hex
        self.message = message
        self.message_id = uuid.uuid4().hex

    @classmethod
    def from_connect_event(
        cls, client: WebSocketClientConnection
    ) -> "WebSocketRequest":
        return cls(client=client, event_type=EventType.CONNECT)

    @classmethod
    def from_disconnect_event(
        cls, client: WebSocketClientConnection
    ) -> "WebSocketRequest":
        return cls(client=client, event_type=EventType.DISCONNECT)

    @classmethod
    def from_message_event(
        cls, client: WebSocketClientConnection, message: str
    ) -> "WebSocketRequest":
        return cls(
            client=client,
            event_type=EventType.MESSAGE,
            message=message,
        )

    @property
    def client(self) -> WebSocketClientProtocol:
        return self._client.client

    @property
    def query_string(self) -> str:
        if "?" in self.client.path:
            return self.client.path.split("?")[-1]
        return ""

    @property
    def request_time(self) -> str:
        return datetime.datetime.fromtimestamp(
            self.request_time_epoch / 1000,
            tz=datetime.timezone.utc,
        ).strftime("%m/%d/%Y:%H:%M:%S %z")

    @property
    def domain_name(self) -> str:
        return self.client.remote_address

    @property
    def api_id(self) -> str:
        return ""

    @property
    def route_key(self) -> WebsocketRouteType:
        return event_type_to_route_key_map[self.event_type]

    def get_request_context(self) -> Dict[str, Any]:
        common_context = {
            "eventType": self.event_type.value,
            "extendedRequestId": self.request_id,
            "requestTime": self.request_time,
            "messageDirection": "IN",
            "connectedAt": self._client.connected_at,
            "requestTimeEpoch": self.request_time_epoch,
            "identity": self._client.identity,
            "requestId": self.request_id,
            "domainName": self.domain_name,
            "connectionId": self._client.connection_id,
            "stage": "api",
            "apiId": self.api_id,
        }
        event_specific_context: Dict[str, Any] = {}

        if self.event_type == EventType.MESSAGE:
            event_specific_context = {
                "routeKey": self.route_key,
                "messageId": self.message_id,
            }
        elif self.event_type == EventType.CONNECT:
            event_specific_context = {
                "routeKey": self.route_key,
            }
        elif self.event_type == EventType.DISCONNECT:
            event_specific_context = {
                "routeKey": self.route_key,
                "disconnectStatusCode": self.client.close_code,
                "disconnectReason": self.client.close_reason,
            }
        return {**common_context, **event_specific_context}

    def create_lambda_event(self) -> Dict[str, Any]:
        lambda_event: Dict[str, Any] = {}
        if self.event_type == EventType.MESSAGE:
            lambda_event = {
                "requestContext": self.get_request_context(),
                "body": self.message,
                "isBase64Encoded": False,
            }
        elif self.event_type == EventType.CONNECT:
            lambda_event = {
                "headers": parse_headers(self.client.request_headers),
                "multiValueHeaders": parse_multi_headers(
                    self.client.request_headers
                ),
                "requestContext": self.get_request_context(),
                "isBase64Encoded": False,
                "queryStringParameters": parse_query_string(self.query_string),
                "multiValueQueryStringParameters": parse_multi_query_string(
                    self.query_string
                ),
            }
        elif self.event_type == EventType.DISCONNECT:
            lambda_event = {
                "headers": parse_headers(self.client.request_headers),
                "multiValueHeaders": parse_multi_headers(
                    self.client.request_headers
                ),
                "requestContext": self.get_request_context(),
                "isBase64Encoded": False,
            }
        return lambda_event


class WebSocketServer:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.connected_clients: Set[WebSocketClientConnection] = set()
        self.server = None
        self.chalice_connect_handler: Optional[
            Callable[[WebSocketRequest], None]
        ] = None
        self.chalice_disconnect_handler: Optional[
            Callable[[WebSocketRequest], None]
        ] = None
        self.chalice_message_handler: Optional[
            Callable[[WebSocketRequest], None]
        ] = None

    def add_connect_handler(
        self, handler: Callable[[WebSocketRequest], None]
    ) -> None:
        self.chalice_connect_handler = handler

    def add_disconnect_handler(
        self, handler: Callable[[WebSocketRequest], None]
    ) -> None:
        self.chalice_disconnect_handler = handler

    def add_message_handler(
        self, handler: Callable[[WebSocketRequest], None]
    ) -> None:
        self.chalice_message_handler = handler

    def get_client_connection(
        self, connection_id: str
    ) -> WebSocketClientConnection:
        for connected_client in self.connected_clients:
            if connected_client.connection_id == connection_id:
                return connected_client
        raise ConnectionGoneException(
            f"connection with id: {connection_id} not found."
        )

    def send_message_to_connection(
        self, connection_id: str, message: str
    ) -> Task:
        loop = asyncio.get_event_loop()
        connected_client = self.get_client_connection(connection_id)
        connected_client.last_active_at = int(time.time() * 1000)
        return loop.create_task(connected_client.client.send(message))

    def delete_connection(self, connection_id: str) -> None:
        loop = asyncio.get_event_loop()
        client_to_disconnect = self.get_client_connection(connection_id)
        loop.create_task(client_to_disconnect.client.close())
        self.connected_clients.remove(client_to_disconnect)

    def get_connection(self, connection_id: str) -> WebSocketClientConnection:
        return self.get_client_connection(connection_id)

    def add_connection(self, client: WebSocketClientConnection) -> None:
        self.connected_clients.add(client)

    async def handle_message(self, request: WebSocketRequest) -> None:
        # Process the received message
        print(f"Received message from client: {request.message}")
        if self.chalice_message_handler:
            self.chalice_message_handler(request)

    async def connect_handler(
        self, websocket_client: WebSocketClientProtocol
    ) -> None:
        # Add the new client to the connected_clients set
        client = WebSocketClientConnection(websocket_client)
        self.add_connection(client)

        if self.chalice_connect_handler:
            self.chalice_connect_handler(
                WebSocketRequest.from_connect_event(client)
            )
        try:
            async for message in client.client:
                await self.handle_message(
                    WebSocketRequest.from_message_event(
                        client, message=message
                    )
                )
        finally:
            # Remove the client from the connected_clients set
            # upon disconnection
            if self.chalice_disconnect_handler:
                self.chalice_disconnect_handler(
                    WebSocketRequest.from_disconnect_event(client)
                )
            self.connected_clients.remove(client)

    async def start_server(self) -> None:
        # Start the WebSocket server
        self.server = await serve(
            ws_handler=self.connect_handler,  # mypy ignore
            host=self.host,
            port=self.port,
            max_size=2**7,
        )
        # Keep the server running until it is closed
        if self.server:
            await self.server.wait_closed()

    def stop_server(self) -> None:
        print("\nGracefully closing the WebSocket server...")
        if self.server:
            self.server.close()

    async def run_server(self) -> None:
        await self.start_server()

    def run_forever(self) -> None:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.start_server())
        loop.close()


class WebSocketClient(object):
    def __init__(self, server: "WebSocketServer") -> None:
        self.server = server
        self.exceptions = WebSocketExceptionFactory()

    def post_to_connection(self, ConnectionId: str, Data: str) -> None:
        # pylint: disable=invalid-name
        self.server.send_message_to_connection(
            connection_id=ConnectionId, message=Data
        )

    def delete_connection(self, ConnectionId: str) -> None:
        # pylint: disable=invalid-name
        self.server.delete_connection(connection_id=ConnectionId)

    def get_connection(self, ConnectionId: str) -> Dict:
        # pylint: disable=invalid-name
        client = self.server.get_connection(connection_id=ConnectionId)
        identity = client.identity
        return {
            "ConnectedAt": datetime.datetime.fromtimestamp(
                client.connected_at, tz=datetime.timezone.utc
            ),
            "Identity": {
                "SourceIp": identity["sourceIp"],
                "UserAgent": identity["userAgent"],
            },
            "LastActiveAt": datetime.datetime.fromtimestamp(
                client.last_active_at, tz=datetime.timezone.utc
            ),
        }


class LocalWebSocketSession(object):
    def __init__(
        self, server: WebSocketServer, region_name: str = "us-west-2"
    ) -> None:
        self.server = server
        self._client = WebSocketClient(server=server)
        self.region_name = region_name

    def client(
        self, name: str, endpoint_url: Optional[str] = None
    ) -> WebSocketClient:
        return self._client


class LocalWebsocketEventSourceHandler(EventSourceHandler):
    WEBSOCKET_API_RESPONSE = {"statusCode": 200}

    def __call__(
        self, event: Dict[str, Any], context: Dict[str, Any]
    ) -> Dict[str, Any]:
        response = super(LocalWebsocketEventSourceHandler, self).__call__(
            event, context
        )
        data = None
        if isinstance(response, dict):
            data = response
            if "statusCode" not in data:
                data = {**self.WEBSOCKET_API_RESPONSE, **data}
        return data or self.WEBSOCKET_API_RESPONSE


class WebSocketGateway(object):
    """A class for faking the behavior of WebSockets API."""

    MAX_LAMBDA_EXECUTION_TIME = 900

    def __init__(self, app_object: Chalice, config: Config) -> None:
        self._app_object = app_object
        self._config = config

    def _generate_lambda_event(
        self, request: WebSocketRequest
    ) -> Dict[str, Any]:
        return request.create_lambda_event()

    def websocket_handler_for_route(
        self, route_key: WebsocketRouteType
    ) -> LocalWebsocketEventSourceHandler:
        fn = self._app_object.websocket_handlers[route_key].handler_function
        return LocalWebsocketEventSourceHandler(fn, WebsocketEvent)

    def handle_websocket_event(
        self, request: WebSocketRequest
    ) -> Dict[str, Any]:
        lambda_event = self._generate_lambda_event(request)
        websocket_handler = self.websocket_handler_for_route(
            route_key=request.route_key
        )
        response = websocket_handler(lambda_event, context={})
        return response


class ChaliceWebsocketMessageHandler(object):
    def __init__(
        self, server: "WebSocketServer", app_object: Chalice, config: Config
    ) -> None:
        self.local_gateway = WebSocketGateway(app_object, config)
        self.app_object = app_object
        self.app_object.websocket_api.session = LocalWebSocketSession(
            server=server
        )
        server.add_connect_handler(self.connect)
        server.add_message_handler(self.default)
        server.add_disconnect_handler(self.disconnect)

    def connect(self, request: WebSocketRequest) -> None:
        self.local_gateway.handle_websocket_event(request)

    def disconnect(self, request: WebSocketRequest) -> None:
        self.local_gateway.handle_websocket_event(request)

    def default(self, request: WebSocketRequest) -> None:
        self.local_gateway.handle_websocket_event(request)


class WebSocketDevServer(object):
    def __init__(
        self, app_object: Chalice, config: Config, host: str, port: int
    ) -> None:
        self.app_object = app_object
        self.host = host
        self.port = port
        self.server = WebSocketServer(host=host, port=port)
        self._message_handler = ChaliceWebsocketMessageHandler(
            server=self.server, app_object=app_object, config=config
        )

    def run_forever(self) -> None:
        print("Serving on ws://%s:%s" % (self.host, self.port))
        self.server.run_forever()

    def shutdown(self) -> None:
        self.server.stop_server()


def create_websocket_server(
    app_obj: Chalice, config: Config, host: str, port: int
) -> WebSocketDevServer:
    return WebSocketDevServer(app_obj, config, host, port)

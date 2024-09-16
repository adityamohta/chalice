import base64
import datetime

from typing import (
    Any,
    Dict,
    Union
)  # noqa

from websockets.sync.server import (
    ServerConnection as WebsocketConnection
)
from websockets.exceptions import (
    ConnectionClosed as WebsocketConnectionClosed
)


class WebsocketClientConnection:
    def __init__(self, connection: WebsocketConnection) -> None:
        self.connection = connection
        self._connected_at = datetime.datetime.utcnow()
        self._last_active_at = self._connected_at

    def _touch(self) -> None:
        self._last_active_at = datetime.datetime.utcnow()

    def recv(self) -> Union[str, bytes]:
        message = self.connection.recv()
        self._touch()
        return message

    def send(self, message: Union[str, bytes]) -> None:
        self.connection.send(message)
        self._touch()

    def close(self) -> None:
        self.connection.close()
        self._touch()

    def info(self) -> Dict[str, Any]:
        try:
            source_ip = self.connection.remote_address[0]
        except Exception:
            source_ip = ''
        if self.connection.request is not None:
            try:
                user_agent = next(iter(
                    self.connection.request.headers.get_all('User-Agent')))
            except StopIteration:
                user_agent = ''
        else:
            user_agent = ''
        return {
            'ConnectedAt': self._connected_at,
            'Identity': {
                'SourceIp': source_ip,
                'UserAgent': user_agent,
            },
            'LastActiveAt': self._last_active_at,
        }


class WebsocketClientExceptions:
    GoneException = Exception


class WebsocketClient:
    exceptions = WebsocketClientExceptions()

    def __init__(self) -> None:
        self._connections: Dict[str, WebsocketClientConnection] = {}

    def _get(self, connection_id: str) -> WebsocketClientConnection:
        try:
            return self._connections[connection_id]
        except KeyError:
            raise self.exceptions.GoneException('Connection not found')

    def _del(self, connection_id: str) -> None:
        try:
            del self._connections[connection_id]
        except KeyError:
            raise self.exceptions.GoneException('Connection not found')

    def get_connection_id(self, connection: WebsocketConnection) -> str:
        return base64.b64encode(connection.id.bytes).decode('ascii')

    def add_connection(self, connection: WebsocketConnection) -> None:
        self._connections[self.get_connection_id(connection)] = (
            WebsocketClientConnection(connection)
        )

    def receive_message(self,
                        ConnectionId: str  # pylint: disable=invalid-name
                        ) -> Union[str, bytes]:
        return self._get(ConnectionId).recv()

    def post_to_connection(self,
                           ConnectionId: str,  # pylint: disable=invalid-name
                           Data: str) -> None:  # pylint: disable=invalid-name
        try:
            self._get(ConnectionId).send(Data)
        except WebsocketConnectionClosed:
            self._del(ConnectionId)
            raise self.exceptions.GoneException('Connection closed')

    def delete_connection(self,
                          ConnectionId: str  # pylint: disable=invalid-name
                          ) -> None:
        self._get(ConnectionId).close()
        self._del(ConnectionId)

    def get_connection(self,
                       ConnectionId: str  # pylint: disable=invalid-name
                       ) -> Dict[str, Any]:
        return self._get(ConnectionId).info()

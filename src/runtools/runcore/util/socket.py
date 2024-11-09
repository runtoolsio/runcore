import abc
import logging
import os
import socket
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from threading import Thread, RLock
from types import coroutine
from typing import List, NamedTuple, Optional

# Default=32768, Max= 262142, https://docstore.mik.ua/manuals/hp-ux/en/B2355-60130/UNIX.7P.html
RECV_BUFFER_LENGTH = 65536  # Can be increased to 163840?

log = logging.getLogger(__name__)


class SocketServerException(Exception):
    pass


class SocketCreationException(SocketServerException):

    def __init__(self, socket_path):
        self.socket_path = socket_path
        super().__init__(f"Unable to create socket: {socket_path}")


class SocketServerStoppedAlready(SocketServerException):
    pass


class SocketServer(abc.ABC):

    def __init__(self, socket_path_provider, *, allow_ping=False):
        self._socket_path_provider = socket_path_provider
        self._allow_ping = allow_ping
        self._server: socket = None
        self._serving_thread = Thread(target=self._serve, name='Thread-ApiServer')
        self._lock = RLock()
        self._stopped = False

    def _bind_socket(self):
        if self._stopped:
            raise SocketServerStoppedAlready

        try:
            socket_path = self._socket_path_provider()
        except FileNotFoundError as e:
            raise SocketCreationException(e.filename) from e

        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        try:
            self._server.bind(str(socket_path))
        except PermissionError as e:
            raise SocketCreationException(socket_path) from e

    def start(self):
        with self._lock:
            self._bind_socket()
            self._serving_thread.start()

    def wait(self):
        self._serving_thread.join()

    def serve(self):
        with self._lock:
            self._bind_socket()
        self._serve()

    def _serve(self):
        log.debug('event=[server_started]')
        server = self._server  # This prevents None access error when the server is closed
        while not self._stopped:
            datagram, client_address = server.recvfrom(RECV_BUFFER_LENGTH)
            if not datagram:
                break

            req_body = datagram.decode()
            if self._allow_ping and req_body == 'ping':
                resp_body = 'pong'
            else:
                # TODO catch exceptions? TypeError: Object of type AggregatedResponse is not JSON serializable
                resp_body = self.handle(req_body)

            if resp_body:
                if client_address:
                    encoded = resp_body.encode()
                    try:
                        server.sendto(encoded, client_address)
                    except OSError as e:
                        if e.errno == 90:
                            log.error(f"event=[server_response_payload_too_large] length=[{len(encoded)}]")
                        raise e
                else:
                    log.warning('event=[missing_client_address]')
        log.debug('event=[server_stopped]')

    @abc.abstractmethod
    def handle(self, req_body):
        """
        Handle request and optionally return response
        :return: response body or None if no response
        """

    def stop(self):
        self.close()

    def close(self):
        with self._lock:
            self._stopped = True

            if self._server is None:
                return

            socket_name = self._server.getsockname()  # This must be executed before the socket is closed
            try:
                # self._server.shutdown(socket.SHUT_RD) TODO Delete this when confirmed not needed
                self._server.close()
            finally:
                self._server = None
                if os.path.exists(socket_name):
                    os.remove(socket_name)

    def close_and_wait(self):
        self.close()
        self.wait()


class Error(Enum):
    TIMEOUT = auto()


class ServerResponse(NamedTuple):
    server_id: str
    response: Optional[str]
    error: Error = None


@dataclass
class PingResult:
    active_servers: List[str]
    timed_out_servers: List[str]
    stale_sockets: List[Path]


class SocketClient:

    def __init__(self, servers_provider, bidirectional: bool, *, timeout=2):
        self._servers_provider = servers_provider
        self._bidirectional = bidirectional
        self._client = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        self._client_addr = None
        if bidirectional:
            self._client.bind(self._client.getsockname())
            self._client.settimeout(timeout)
        self.timed_out_servers = []
        self.stale_sockets = []

    @coroutine
    def servers(self, include=()):
        """

        :param include: server IDs exact match filter
        :return: response if bidirectional
        :raises PayloadTooLarge: when request payload is too large
        """
        req_body = '_'  # Dummy initialization to remove warnings
        resp = None
        skip = False
        for server_file in self._servers_provider():
            server_id = server_file.stem
            if (server_file in self.stale_sockets) or (include and server_id not in include):
                continue
            while True:
                if not skip:
                    req_body = yield resp
                skip = False  # reset
                if not req_body:
                    break  # next(this) called -> proceed to the next server

                encoded = req_body.encode()
                try:
                    self._client.sendto(encoded, str(server_file))
                    if self._bidirectional:
                        datagram = self._client.recv(RECV_BUFFER_LENGTH)
                        resp = ServerResponse(server_id, datagram.decode())
                except TimeoutError:
                    log.warning('event=[socket_timeout] socket=[{}]'.format(server_file))
                    self.timed_out_servers.append(server_id)
                    resp = ServerResponse(server_id, None, Error.TIMEOUT)
                except ConnectionRefusedError:  # TODO what about other errors?
                    log.warning('event=[stale_socket] socket=[{}]'.format(server_file))
                    self.stale_sockets.append(server_file)
                    skip = True  # Ignore this one and continue with another one
                    break
                except OSError as e:
                    if e.errno == 2 or e.errno == 32:
                        continue  # The server closed meanwhile
                    if e.errno == 90:
                        raise PayloadTooLarge(len(encoded))
                    raise e

    def communicate(self, req, include=()) -> List[ServerResponse]:
        server = self.servers(include=include)
        responses = []
        while True:
            try:
                next(server)
                responses.append(server.send(req))  # StopIteration is raised from this function if last socket is dead
            except StopIteration:
                break
        return responses

    def ping(self):
        responses = self.communicate('ping')
        active = [resp.server_id for resp in responses]
        timed_out = list(self.timed_out_servers)
        stale = list(self.stale_sockets)
        return PingResult(active, timed_out, stale)

    def close(self):
        self._client.shutdown(socket.SHUT_RDWR)
        self._client.close()


class PayloadTooLarge(Exception):
    """
    This exception is thrown when the operating system rejects sent datagram due to its size.
    """

    def __init__(self, payload_size):
        super().__init__("Datagram payload is too large: " + str(payload_size))

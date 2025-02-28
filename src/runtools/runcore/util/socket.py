import abc
import errno
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
        self._socket_path = None
        self._serving_thread = Thread(target=self._serve, name='Thread-ApiServer')
        self._lock = RLock()
        self._stopped = False

    def _bind_socket(self):
        if self._stopped:
            raise SocketServerStoppedAlready

        try:
            self._socket_path = self._socket_path_provider()
        except FileNotFoundError as e:
            raise SocketCreationException(e.filename) from e

        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        try:
            self._server.bind(str(self._socket_path))
        except PermissionError as e:
            raise SocketCreationException(self._socket_path) from e

    @property
    def address(self) -> str:
        return str(self._socket_path) if self._socket_path else None

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
            try:
                datagram, client_address = server.recvfrom(RECV_BUFFER_LENGTH)
            except OSError as e:
                if e.errno == 9 and self._stopped:
                    break  # MacOS behaviour on close
                raise e
            if not datagram:
                break  # Linux behaviour on close

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
                try:
                    self._server.shutdown(socket.SHUT_RD)
                except OSError as e:
                    if e.errno == 57:  # macOS: Socket is not connected
                        pass
                    else:
                        raise
                self._server.close()
            finally:
                self._server = None
                if os.path.exists(socket_name):
                    os.remove(socket_name)

    def close_and_wait(self):
        self.close()
        self.wait()


class ErrorType(Enum):
    COMMUNICATION = auto()  # Socket protocol/communication errors other than timeouts and stale sockets
    TIMEOUT = auto()        # Connection timeouts
    STALE = auto()          # Dead/stale sockets


class SocketRequestResult(NamedTuple):
    server_address: str
    response: Optional[str]
    error: Optional[Exception] = None

    @property
    def error_type(self) -> Optional[ErrorType]:
        if not self.error:
            return None
        if isinstance(self.error, (socket.timeout, TimeoutError)):
            return ErrorType.TIMEOUT
        if isinstance(self.error, ConnectionRefusedError):
            return ErrorType.STALE
        return ErrorType.COMMUNICATION


@dataclass
class PingResult:
    active_servers: List[str]
    timed_out_servers: List[str]
    failing_servers: List[str]
    stale_sockets: List[Path]


class SocketClient:

    def __init__(self, servers_provider, *, client_address=None, timeout=2):
        """

        Args:
            servers_provider: TBS
            client_address (str): bidirectional communication is assumed when this parameter is set
            timeout: TBS
        """
        self._servers_provider = servers_provider
        self._client = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        self._client_addr = client_address
        if client_address:
            self._client.bind(client_address)
            self._client.settimeout(timeout)

    @coroutine
    def servers(self, include=()):
        """

        :param include: server IDs exact match filter
        :return: response if bidirectional
        :raises PayloadTooLarge: when request payload is too large
        """
        req_body = '_'  # Dummy initialization to remove warnings
        res = None
        skip = False
        for server_file in self._servers_provider():
            server_id = str(server_file)
            if include and server_id not in include:
                continue
            while True:
                if not skip:
                    req_body = yield res
                skip = False  # reset
                if not req_body:
                    break  # next(this) called -> proceed to the next server

                encoded = req_body.encode()
                try:
                    self._client.sendto(encoded, str(server_file))
                    if self._client_addr:
                        datagram = self._client.recv(RECV_BUFFER_LENGTH)
                        res = SocketRequestResult(server_id, datagram.decode())
                except (socket.timeout, TimeoutError) as e:
                    log.warning('event=[socket_timeout] socket=[{}]'.format(server_file))
                    res = SocketRequestResult(server_id, None, e)
                except ConnectionRefusedError:
                    log.debug('event=[stale_socket] socket=[{}]'.format(server_file))
                    skip = True  # Ignore this one and continue with another one
                    break
                except OSError as e:
                    if e.errno in (errno.ENOENT, errno.EPIPE):  # No such file/directory (2) or Broken pipe (32)
                        continue  # The server closed meanwhile
                    if e.errno == errno.EMSGSIZE:
                        raise PayloadTooLarge(len(encoded))
                    res = SocketRequestResult(server_id, None, e)

    def communicate(self, req: str, include=()) -> List[SocketRequestResult]:
        server = self.servers(include=include)
        responses = []
        while True:
            try:
                next(server)
                responses.append(server.send(req))  # StopIteration is raised from this function if last socket is dead
            except StopIteration:
                break
        return responses

    def ping(self) -> PingResult:
        """
        Pings all available servers to check their status.

        Returns:
            PingResult containing lists of active, timed out, and stale servers
        """
        responses = self.communicate('ping')
        active = []
        timed_out = []
        failed = []
        stale = []

        for resp in responses:
            if resp.error_type is None:
                active.append(resp.server_address)
            elif resp.error_type == ErrorType.TIMEOUT:
                timed_out.append(resp.server_address)
            elif resp.error_type == ErrorType.STALE:
                stale.append(Path(resp.server_address))
            else:
                failed.append(resp.server_address)

        return PingResult(active, timed_out, failed, stale)

    def close(self):
        try:
            self._client.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass  # Socket might already be closed
        self._client.close()
        if self._client_addr and os.path.exists(self._client_addr):
            os.unlink(self._client_addr)
            self._client_addr = None


class PayloadTooLarge(Exception):
    """
    This exception is thrown when the operating system rejects sent datagram due to its size.
    """

    def __init__(self, payload_size):
        super().__init__("Datagram payload is too large: " + str(payload_size))

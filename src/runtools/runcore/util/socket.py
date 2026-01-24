import abc
import errno
import logging
import os
import socket
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from threading import Thread, RLock
from typing import Generator, List, NamedTuple, Optional

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

    def __init__(self, socket_path, *, allow_ping=False):
        self._socket_path = socket_path
        self._allow_ping = allow_ping
        self._server: Optional[socket.socket] = None
        self._serving_thread = Thread(target=self._serve, name='Thread-Socket-Server')
        self._lock = RLock()
        self._stopped = False

    def _bind_socket(self):
        if self._stopped:
            raise SocketServerStoppedAlready

        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        try:
            self._server.bind(str(self._socket_path))
        except (FileNotFoundError, PermissionError) as e:
            raise SocketCreationException(self._socket_path) from e

    @property
    def address(self) -> str:
        return str(self._socket_path)

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


class SocketErrorType(Enum):
    """Classification of socket-level errors."""
    COMMUNICATION = auto()  # Socket protocol/communication errors other than timeouts and stale sockets
    TIMEOUT = auto()        # Connection timeouts
    STALE = auto()          # Dead/stale sockets


class SocketRequestResult(NamedTuple):
    server_address: str
    response: Optional[str]
    error: Optional[Exception] = None

    @property
    def error_type(self) -> Optional[SocketErrorType]:
        if not self.error:
            return None
        if isinstance(self.error, (socket.timeout, TimeoutError)):
            return SocketSocketErrorType.TIMEOUT
        if isinstance(self.error, ConnectionRefusedError):
            return SocketSocketErrorType.STALE
        return SocketErrorType.COMMUNICATION


@dataclass
class PingResult:
    active_servers: List[str]
    timed_out_servers: List[str]
    failing_servers: List[str]
    stale_sockets: List[Path]


class SocketClient:

    def __init__(self, server_sockets_provider=None, *, client_address=None, timeout=2):
        """
        Args:
            server_sockets_provider: Callable returning iterable of server socket paths.
            client_address: Client socket path for receiving responses. If set, enables
                bidirectional communication.
            timeout: Socket timeout in seconds for receiving responses.
        """
        self._server_sockets_provider = server_sockets_provider
        self._client = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        self._client_addr = client_address
        if client_address:
            # Explicit client address wouldn't be needed on Linux: self._client.bind(self._client.getsockname())
            self._client.bind(client_address)
            self._client.settimeout(timeout)

    def servers(self, addresses=()) -> Generator[SocketRequestResult | None, str | None, None]:
        """
        Generator for iterating over servers and sending requests.

        This is a coroutine-style generator that yields responses and receives requests via send().
        Use next() to advance to next server, send(request) to send a request to current server.

        Args:
            addresses: Server addresses to communicate with. If empty, uses server_sockets_provider.

        Yields:
            SocketRequestResult with response, or None initially.

        Receives:
            Request body string to send, or None/empty to move to next server.

        Raises:
            PayloadTooLarge: When request payload exceeds socket limits.
        """
        req_body = '_'  # Dummy initialization to remove warnings
        res = None
        skip = False
        for server_socket in (addresses or self._server_sockets_provider()):
            server_address = str(server_socket)
            while True:
                if not skip:
                    req_body = yield res
                skip = False  # reset
                if not req_body:
                    break  # next(this) called -> proceed to the next server

                encoded = req_body.encode()
                try:
                    self._client.sendto(encoded, str(server_socket))
                    if self._client_addr:
                        datagram = self._client.recv(RECV_BUFFER_LENGTH)
                        res = SocketRequestResult(server_address, datagram.decode())
                except (socket.timeout, TimeoutError) as e:
                    log.warning('event=[socket_timeout] socket=[{}]'.format(server_socket))
                    res = SocketRequestResult(server_address, None, e)
                except ConnectionRefusedError:
                    log.debug('event=[stale_socket] socket=[{}]'.format(server_socket))
                    skip = True  # Ignore this one and continue with another one
                    break
                except OSError as e:
                    if e.errno in (errno.ENOENT, errno.EPIPE):  # No such file/directory (2) or Broken pipe (32)
                        skip = True  # The server not found or closed meanwhile
                        break
                    if e.errno == errno.EMSGSIZE:
                        raise PayloadTooLarge(len(encoded))
                    res = SocketRequestResult(server_address, None, e)

    def communicate(self, req: str, addresses=()) -> List[SocketRequestResult]:
        server = self.servers(addresses)
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
            elif resp.error_type == SocketErrorType.TIMEOUT:
                timed_out.append(resp.server_address)
            elif resp.error_type == SocketErrorType.STALE:
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

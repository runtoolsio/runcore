import abc
import errno
import logging
import os
import socket
import tempfile
import uuid
import zlib
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from threading import Thread, RLock
from typing import Generator, List, NamedTuple, Optional

from runtools.runcore import paths

# Default=32768, Max= 262142, https://docstore.mik.ua/manuals/hp-ux/en/B2355-60130/UNIX.7P.html
RECV_BUFFER_LENGTH = 65536  # Can be increased to 163840?

log = logging.getLogger(__name__)

_TOKEN_PREFIX = b'\x00TOKEN:'


def _encode_datagram(text: str) -> bytes:
    """Encode and compress a text payload for datagram transmission."""
    return zlib.compress(text.encode())


def _decode_datagram(data: bytes) -> str:
    """Decode a datagram payload, decompressing if needed.

    Detects zlib-compressed payloads by the zlib header byte (0x78).
    Uncompressed payloads are decoded directly.
    """
    if data and data[0] == 0x78:
        return zlib.decompress(data).decode()
    return data.decode()


def _write_payload_file(data: bytes) -> bytes:
    """Write oversized datagram payload to a spool file and return a token reference.

    When a compressed datagram exceeds the OS size limit (EMSGSIZE), the payload is written
    to ``paths.payload_dir()/<token>.bin`` and a small ``\\x00TOKEN:<token>`` reference is
    returned for transmission instead. The receiver resolves the token back to the payload
    via ``_resolve_datagram``.

    TODO: Spool files may be orphaned if the token datagram is never delivered or consumed.
        Consider a periodic TTL sweep of stale files in ``paths.payload_dir()``.
    """
    paths.payload_dir().mkdir(parents=True, exist_ok=True, mode=0o700)
    token = uuid.uuid4().hex
    path = paths.payload_dir() / f"{token}.bin"
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, data)
    except OSError:
        os.close(fd)
        path.unlink(missing_ok=True)
        raise
    os.close(fd)
    return _TOKEN_PREFIX + token.encode()


def _resolve_datagram(data: bytes) -> str:
    """Decode a datagram, resolving a token reference to a spool file if needed.

    Token references (``\\x00TOKEN:<hex>`` prefix) are looked up in ``paths.payload_dir()``,
    read, deleted, and then decoded as a normal (zlib-compressed) payload. Plain datagrams
    are decoded directly.
    """
    if data.startswith(_TOKEN_PREFIX):
        token = data[len(_TOKEN_PREFIX):].decode()
        if not token.isalnum():
            raise ValueError(f"Invalid payload token: {token!r}")
        path = paths.payload_dir() / f"{token}.bin"
        try:
            payload = path.read_bytes()
        finally:
            path.unlink(missing_ok=True)
        return _decode_datagram(payload)
    return _decode_datagram(data)


class SocketServerException(Exception):
    pass


class SocketCreationException(SocketServerException):

    def __init__(self, socket_path):
        self.socket_path = socket_path
        super().__init__(f"Unable to create socket: {socket_path}")


class SocketServerStoppedAlready(SocketServerException):
    pass


class DatagramSocketServer(abc.ABC):

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
                    break  # macOS behaviour on close
                raise e
            if not datagram:
                break  # Linux behaviour on close

            try:
                req_body = _resolve_datagram(datagram)
            except (FileNotFoundError, ValueError, zlib.error, UnicodeDecodeError) as e:
                log.warning("event=[datagram_decode_failed] error=[%s]", e)
                continue

            if self._allow_ping and req_body == 'ping':
                resp_body = 'pong'
            else:
                # TODO catch exceptions? TypeError: Object of type AggregatedResponse is not JSON serializable
                resp_body = self.handle(req_body)

            if resp_body:
                if client_address:
                    encoded = _encode_datagram(resp_body)
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
            return SocketErrorType.TIMEOUT
        if isinstance(self.error, ConnectionRefusedError):
            return SocketErrorType.STALE
        return SocketErrorType.COMMUNICATION


@dataclass
class PingResult:
    active_servers: List[str]
    timed_out_servers: List[str]
    failing_servers: List[str]
    stale_sockets: List[Path]


class DatagramSocketClient:

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

                encoded = _encode_datagram(req_body)
                try:
                    try:
                        self._client.sendto(encoded, str(server_socket))
                    except OSError as e:
                        if e.errno == errno.EMSGSIZE:
                            ref = _write_payload_file(encoded)
                            self._client.sendto(ref, str(server_socket))
                        else:
                            raise
                    if self._client_addr:
                        resp_datagram = self._client.recv(RECV_BUFFER_LENGTH)
                        res = SocketRequestResult(server_address, _decode_datagram(resp_datagram))
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


# --- Stream socket helpers and classes for RPC communication ---

def _recv_exact(conn: socket.socket, n: int) -> bytes:
    """Receive exactly n bytes from connection."""
    data = b''
    while len(data) < n:
        chunk = conn.recv(n - len(data))
        if not chunk:
            raise ConnectionError("Connection closed")
        data += chunk
    return data


def _send_message(conn: socket.socket, data: str) -> None:
    """Send length-prefixed message."""
    encoded = data.encode()
    header = len(encoded).to_bytes(4, 'big')
    conn.sendall(header + encoded)


def _recv_message(conn: socket.socket) -> str:
    """Receive length-prefixed message."""
    header = _recv_exact(conn, 4)
    length = int.from_bytes(header, 'big')
    return _recv_exact(conn, length).decode()


class StreamSocketServer(abc.ABC):
    """Stream-based socket server for RPC (request/response) communication."""

    def __init__(self, socket_path, *, allow_ping=False):
        self._socket_path = socket_path
        self._allow_ping = allow_ping
        self._server: Optional[socket.socket] = None
        self._serving_thread = Thread(target=self._serve, name='Thread-Stream-Server')
        self._lock = RLock()
        self._stopped = False

    def _bind_socket(self):
        if self._stopped:
            raise SocketServerStoppedAlready
        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
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
            self._server.listen(5)
            self._serving_thread.start()

    def wait(self):
        self._serving_thread.join()

    def serve(self):
        with self._lock:
            self._bind_socket()
            self._server.listen(5)
        self._serve()

    def _serve(self):
        log.debug('event=[stream_server_started]')
        server = self._server
        while not self._stopped:
            try:
                conn, _ = server.accept()
            except OSError as e:
                if self._stopped:
                    break
                raise
            Thread(target=self._handle_connection, args=(conn,), daemon=True).start()
        log.debug('event=[stream_server_stopped]')

    def _handle_connection(self, conn: socket.socket):
        try:
            with conn:
                req_body = _recv_message(conn)
                if self._allow_ping and req_body == 'ping':
                    resp_body = 'pong'
                else:
                    resp_body = self.handle(req_body)
                if resp_body:
                    _send_message(conn, resp_body)
        except (ConnectionError, OSError) as e:
            log.debug(f'event=[connection_error] error=[{e}]')

    @abc.abstractmethod
    def handle(self, req_body) -> Optional[str]:
        """Handle request and return response."""
        pass

    def stop(self):
        self.close()

    def close(self):
        with self._lock:
            self._stopped = True

            if self._server is None:
                return

            socket_name = self._server.getsockname()
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


class StreamSocketClient:
    """Stream-based socket client for RPC communication."""

    def __init__(self, server_sockets_provider=None, *, timeout=2):
        self._server_sockets_provider = server_sockets_provider
        self._timeout = timeout

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
        """
        req_body = '_'
        res = None
        skip = False
        for server_socket in (addresses or self._server_sockets_provider()):
            server_address = str(server_socket)
            while True:
                if not skip:
                    req_body = yield res
                skip = False
                if not req_body:
                    break

                try:
                    conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    conn.settimeout(self._timeout)
                    conn.connect(server_address)
                    try:
                        _send_message(conn, req_body)
                        response = _recv_message(conn)
                        res = SocketRequestResult(server_address, response)
                    finally:
                        conn.close()
                except (socket.timeout, TimeoutError) as e:
                    log.warning(f'event=[socket_timeout] socket=[{server_socket}]')
                    res = SocketRequestResult(server_address, None, e)
                except (ConnectionRefusedError, FileNotFoundError) as e:
                    log.debug(f'event=[stale_socket] socket=[{server_socket}]')
                    skip = True
                    break
                except OSError as e:
                    res = SocketRequestResult(server_address, None, e)

    def communicate(self, req: str, addresses=()) -> List[SocketRequestResult]:
        server = self.servers(addresses)
        responses = []
        while True:
            try:
                next(server)
                responses.append(server.send(req))
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
        pass  # No persistent connection to close


def clean_stale_sockets(server_sockets_provider) -> List[Path]:
    """
    Ping all sockets from the provider and remove any stale (dead) sockets.

    Args:
        server_sockets_provider: Callable returning iterable of server socket paths.

    Returns:
        List of Path objects for sockets that were removed.
    """
    client_path = os.path.join(tempfile.gettempdir(), f"runtools_client_{uuid.uuid4().hex}.sock")
    client = DatagramSocketClient(server_sockets_provider, client_address=client_path)
    try:
        ping_result = client.ping()
    finally:
        client.close()

    cleaned = []
    for stale_socket in ping_result.stale_sockets:
        stale_socket.unlink(missing_ok=True)
        cleaned.append(stale_socket)

    return cleaned

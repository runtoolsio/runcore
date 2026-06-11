"""Transport runtimes for runtools environments.

A transport selects how environment nodes and connectors communicate. Each transport
ships its own module here with the concrete bundle, layout, and RPC pieces.

Connector-side: the :class:`ConnectorTransport` protocol declares what the concrete connector
(``runtools.runcore.connector._Connector``) consumes from a transport. Concrete bundles live
in ``runtools.runcore.transport.<transport>`` (e.g. ``unix_socket.UnixSocketConnectorTransport``).
"""

from typing import Any, Callable, List, Protocol, runtime_checkable

from runtools.runcore.job import InstanceID, JobInstanceMetadata, JobRun
from runtools.runcore.output import Mode, OutputLine
from runtools.runcore.run import StopReason

NodeEventHandler = Callable[[str, JobInstanceMetadata, dict], None]
"""Signature of a handler registered with a :class:`NodeEventReceiver`.

Receives ``(event_type, instance_metadata, event_dict)`` for each event. The standard
transport-neutral implementation is
:class:`runtools.runcore.listening.InstanceEventReceiver`, which adapts these payloads into
typed observer notifications.
"""


@runtime_checkable
class NodeRpcClient(Protocol):
    """RPC client surface for talking to environment nodes.

    Covers both broadcast operations across all reachable nodes (``collect_active_runs``)
    and per-node operations identified by an opaque ``server_address`` routing token. The
    routing token is supplied by broadcast results and by :class:`UnixSocketJobInstanceProxy` — callers
    don't construct it directly. For the unix_socket transport it is a socket path; for other
    transports it may be any string-shaped routing key.

    This is the full surface consumed by both the concrete connector and the proxy layer
    (:class:`UnixSocketJobInstanceProxy`, ``_ProxyOutput``, :class:`PhaseControlProxy`). Concrete
    implementations may expose additional lower-level methods (``call_method``,
    ``broadcast_method``); those are implementation details and not part of the contract.
    """
    def collect_active_runs(self, run_match) -> List: ...

    def get_active_runs(self, server_address: str, run_match) -> List[JobRun]: ...

    def stop_instance(self, server_address: str, instance_id: InstanceID,
                      stop_reason: StopReason = StopReason.STOPPED) -> None: ...

    def get_output_tail(self, server_address: str, instance_id: InstanceID,
                        mode: Mode = Mode.TAIL, max_lines: int = 100) -> List[OutputLine]: ...

    def exec_phase_op(self, server_address: str, instance_id: InstanceID, phase_id: str,
                      op_name: str, *op_args) -> Any: ...

    def close(self) -> None: ...


@runtime_checkable
class NodeEventReceiver(Protocol):
    """Receives broadcast events from environment nodes and routes them to a handler.

    Handlers conform to :data:`NodeEventHandler` — they receive
    ``(event_type, instance_metadata, event_dict)`` for each event.
    """
    def register_handler(self, handler: NodeEventHandler, *event_types: str) -> None: ...

    def start(self) -> None: ...

    def close(self) -> None: ...


@runtime_checkable
class ConnectorTransport(Protocol):
    """Bundle of transport runtime pieces consumed by the concrete environment connector.

    Implementations expose:
      - ``node_client``: an RPC client (see :class:`NodeRpcClient`).
      - ``event_receiver``: a node-event receiver (see :class:`NodeEventReceiver`).
      - ``close()``: releases the transport's resources (sockets, component dirs, etc.).
    """
    node_client: NodeRpcClient
    event_receiver: NodeEventReceiver

    def close(self) -> None:
        ...

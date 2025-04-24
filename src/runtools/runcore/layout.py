from pathlib import Path

from pydantic import BaseModel, Field

class LayoutDefaults:
    """Default constants for local environment layouts"""
    NODE_DIR_PREFIX = "node_"
    CONNECTOR_DIR_PREFIX = "connector_"
    SOCKET_NAME_CLIENT_RPC = "client-rpc.sock"
    SOCKET_NAME_SERVER_RPC = "server-rpc.sock"
    SOCKET_NAME_LISTENER_EVENTS = "listener-events.sock"
    SOCKET_NAME_LISTENER_LIFECYCLE = "listener-lifecycle.sock"
    SOCKET_NAME_LISTENER_PHASE = "listener-phase.sock"
    SOCKET_NAME_LISTENER_OUTPUT = "listener-output.sock"


class LocalEnvironmentLayoutConfig(BaseModel):
    """
    Configuration for Local Environment Layouts.

    Contains all configurable parameters for creating and using a LocalEnvironmentLayout,
    including directory prefixes and socket filenames.
    """
    env_dir: Path
    component_dir: Path
    node_dir_prefix: str = Field(default=LayoutDefaults.NODE_DIR_PREFIX)
    connector_dir_prefix: str = Field(default=LayoutDefaults.CONNECTOR_DIR_PREFIX)
    socket_name_client_rpc: str = Field(default=LayoutDefaults.SOCKET_NAME_CLIENT_RPC)
    socket_name_server_rpc: str = Field(default=LayoutDefaults.SOCKET_NAME_SERVER_RPC)
    socket_name_listener_events: str = Field(default=LayoutDefaults.SOCKET_NAME_LISTENER_EVENTS)
    socket_name_listener_lifecycle: str = Field(default=LayoutDefaults.SOCKET_NAME_LISTENER_LIFECYCLE)
    socket_name_listener_phase: str = Field(default=LayoutDefaults.SOCKET_NAME_LISTENER_PHASE)
    socket_name_listener_output: str = Field(default=LayoutDefaults.SOCKET_NAME_LISTENER_OUTPUT)

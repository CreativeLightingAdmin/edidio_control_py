"""Module for Python Integration of eDIDIO S10 Controller."""

import asyncio
import contextlib
import logging
import ssl

from . import eDS10_ProtocolBuffer_pb2 as pb
from .exceptions import (
    EDIDIOCommunicationError,
    EDIDIOConnectionError,
    EDIDIOInvalidMessageError,
    EDIDIOTimeoutError,
)

_LOGGER = logging.getLogger(__name__)

DALI_ARC_LEVEL_MAX = 254
KEEP_ALIVE_INTERVAL_SECONDS = 15
KEEP_ALIVE_MESSAGE = bytes([0xFF, 0xF6])


class EdidioClient:
    """Client for communicating with the Control Freak eDIDIO device."""

    def __init__(
        self,
        host: str,
        port: int,
        timeout: float = 5.0,
        *,
        use_tls: bool = False,
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        """Initialize the eDIDIO client.

        Args:
            host: The IP address or hostname of the eDIDIO device.
            port: The port number of the eDIDIO device. Typically 23 for plain
                TCP or 443 for TLS.
            timeout: Default timeout for network operations in seconds.
            use_tls: If True, use TLS for the connection.
            ssl_context: Optional SSL context for TLS connections. If use_tls is
                True and this is None, a default SSL context (system CA bundle)
                is used. To connect to devices with self-signed certificates,
                pass an SSLContext with check_hostname=False and
                verify_mode=ssl.CERT_NONE.

        """
        self._host = host
        self._port = port
        self._timeout = timeout
        self._use_tls = use_tls
        self._ssl_context = ssl_context
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._connected = False
        self._reconnect_lock = asyncio.Lock()
        self._keep_alive_task: asyncio.Task | None = None

    @property
    def host(self) -> str:
        """Return the host address."""
        return self._host

    @property
    def port(self) -> int:
        """Return the port number."""
        return self._port

    @property
    def use_tls(self) -> bool:
        """Return True if TLS is enabled."""
        return self._use_tls

    @property
    def connected(self) -> bool:
        """Return True if the client is currently connected and not closing."""
        return (
            self._writer is not None
            and not self._writer.is_closing()
            and self._connected
        )

    async def __aenter__(self) -> "EdidioClient":
        """Connect on entering the async context manager."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Disconnect on exiting the async context manager."""
        await self.disconnect()

    async def connect(self) -> None:
        """Establish a connection to the eDIDIO device and start keep-alive."""
        if self.connected:
            return

        async with self._reconnect_lock:
            if self.connected:
                return

            ssl_arg: ssl.SSLContext | bool | None = None
            if self._use_tls:
                ssl_arg = self._ssl_context if self._ssl_context is not None else True

            _LOGGER.debug(
                "Attempting to connect to eDIDIO device at %s:%s (TLS: %s)",
                self._host,
                self._port,
                self._use_tls,
            )
            try:
                self._reader, self._writer = await asyncio.wait_for(
                    asyncio.open_connection(self._host, self._port, ssl=ssl_arg),
                    timeout=self._timeout,
                )
                self._connected = True
                _LOGGER.info(
                    "Successfully connected to eDIDIO device at %s:%s",
                    self._host,
                    self._port,
                )

                if not self._keep_alive_task or self._keep_alive_task.done():
                    self._keep_alive_task = asyncio.create_task(self._keep_alive())
                    _LOGGER.debug(
                        "Started keep-alive task for %s:%s", self._host, self._port
                    )

            except TimeoutError as e:
                self._connected = False
                _LOGGER.error("Connection to eDIDIO device timed out: %s", e)
                raise EDIDIOTimeoutError(f"Connection timed out: {e}") from e
            except ssl.SSLError as e:
                self._connected = False
                _LOGGER.error("TLS error connecting to eDIDIO device: %s", e)
                raise EDIDIOConnectionError(f"TLS error: {e}") from e
            except (OSError, ConnectionRefusedError) as e:
                self._connected = False
                _LOGGER.error("Failed to connect to eDIDIO device: %s", e)
                raise EDIDIOConnectionError(f"Connection failed: {e}") from e
            except Exception as e:
                self._connected = False
                _LOGGER.error("An unexpected error occurred during connection: %s", e)
                raise EDIDIOConnectionError(f"Unexpected connection error: {e}") from e

    async def disconnect(self) -> None:
        """Close the connection to the eDIDIO device and stop keep-alive."""
        if self._keep_alive_task:
            self._keep_alive_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._keep_alive_task
            self._keep_alive_task = None
            _LOGGER.debug("Cancelled keep-alive task for %s:%s", self._host, self._port)

        if self._writer:
            _LOGGER.debug(
                "Closing connection to eDIDIO device %s:%s", self._host, self._port
            )
            self._writer.close()
            with contextlib.suppress(ConnectionResetError, asyncio.TimeoutError):
                await asyncio.wait_for(
                    self._writer.wait_closed(), timeout=self._timeout
                )

        self._reader = None
        self._writer = None
        self._connected = False
        _LOGGER.info("Disconnected from eDIDIO device %s:%s", self._host, self._port)

    async def _send_raw_bytes(self, message: bytes) -> None:
        """Send raw bytes over the TCP connection."""
        if not self.connected:
            _LOGGER.warning(
                "Attempted to send message while not connected. Reconnecting"
            )
            try:
                await self.connect()
                if not self.connected:
                    raise EDIDIOConnectionError("Not connected to eDIDIO device.")
            except EDIDIOConnectionError as e:
                _LOGGER.error("Failed to reconnect before sending message: %s", e)
                raise

        assert self._writer is not None  # guaranteed by connected check above

        try:
            self._writer.write(message)
            await asyncio.wait_for(self._writer.drain(), timeout=self._timeout)
            _LOGGER.debug("Sent raw bytes: %s", message.hex())
        except TimeoutError as e:
            _LOGGER.error("Timeout during raw byte send: %s", e)
            self._connected = False
            raise EDIDIOTimeoutError(f"Send operation timed out: {e}") from e
        except (OSError, ConnectionResetError) as e:
            self._connected = False
            _LOGGER.error(
                "Socket error during raw byte send, marking as disconnected: %s", e
            )
            raise EDIDIOCommunicationError(
                f"Socket error during raw byte send: {e}"
            ) from e
        except Exception as e:
            if isinstance(e, (asyncio.CancelledError, KeyboardInterrupt)):
                raise
            _LOGGER.error("Unexpected send error: %s", e)
            raise EDIDIOCommunicationError(f"Unexpected send error: {e}") from e

    async def _receive_raw_bytes(self, num_bytes: int = 100) -> bytes:
        """Receive raw bytes from the TCP connection."""
        if not self.connected:
            _LOGGER.warning(
                "Attempt to receive message while not connected. Reconnecting"
            )
            try:
                await self.connect()
                if not self.connected:
                    raise EDIDIOConnectionError(
                        "Not connected to eDIDIO device for receiving."
                    )
            except EDIDIOConnectionError as e:
                _LOGGER.error("Failed to reconnect before receive: %s", e)
                raise

        assert self._reader is not None  # guaranteed by connected check above

        try:
            data = await asyncio.wait_for(
                self._reader.read(num_bytes), timeout=self._timeout
            )
            _LOGGER.debug("Received raw bytes: %s", data.hex())
            return data
        except TimeoutError as e:
            _LOGGER.error("Timeout during raw byte receive: %s", e)
            raise EDIDIOTimeoutError(f"Receive operation timed out: {e}") from e
        except asyncio.IncompleteReadError as e:
            self._connected = False
            _LOGGER.error("Incomplete read, connection lost: %s", e)
            raise EDIDIOCommunicationError(f"Incomplete read, connection lost: {e}") from e
        except (OSError, ConnectionResetError) as e:
            self._connected = False
            _LOGGER.error("Failed to receive raw bytes: %s", e)
            raise EDIDIOCommunicationError(f"Failed to receive raw bytes: {e}") from e
        except Exception as e:
            if isinstance(e, (asyncio.CancelledError, KeyboardInterrupt)):
                raise
            _LOGGER.error("Unexpected receive error: %s", e)
            raise EDIDIOCommunicationError(f"Unexpected receive error: {e}") from e

    async def _keep_alive(self) -> None:
        """Send periodic keep-alive messages."""
        while self.connected:
            try:
                await self._send_raw_bytes(KEEP_ALIVE_MESSAGE)
            except (
                EDIDIOConnectionError,
                EDIDIOCommunicationError,
                EDIDIOTimeoutError,
            ) as e:
                _LOGGER.debug(
                    "Keep-alive failed for %s:%s: %s. Will attempt to reconnect",
                    self._host,
                    self._port,
                    e,
                )
            except asyncio.CancelledError:
                _LOGGER.debug(
                    "Keep-alive task for %s:%s cancelled", self._host, self._port
                )
                break
            except Exception as e:
                if isinstance(e, (KeyboardInterrupt, SystemExit)):
                    raise
                _LOGGER.error(
                    "Unexpected error in keep-alive task for %s:%s: %s",
                    self._host,
                    self._port,
                    e,
                )

            await asyncio.sleep(KEEP_ALIVE_INTERVAL_SECONDS)
        _LOGGER.debug("Keep-alive task for %s:%s stopped", self._host, self._port)

    async def send_protobuf_message(self, message: bytes) -> None:
        """Send a protobuf message to the eDIDIO device."""
        await self._send_raw_bytes(message)

    async def receive_protobuf_response(self) -> bytes:
        """Receive a protobuf response from the eDIDIO device."""
        if not self.connected:
            raise EDIDIOConnectionError("Not connected to eDIDIO device for receiving.")

        assert self._reader is not None  # guaranteed by connected check above

        try:
            # Read header (0xCD and 2-byte length)
            header = await asyncio.wait_for(
                self._reader.readexactly(3), timeout=self._timeout
            )
            if header[0] != 0xCD:
                raise EDIDIOInvalidMessageError(
                    "Invalid message header. Expected 0xCD."
                )

            length = (header[1] << 8) | header[2]
            if length <= 0:
                raise EDIDIOInvalidMessageError(
                    f"Invalid message length received: {length}"
                )

            # Read the protobuf message payload
            payload = await asyncio.wait_for(
                self._reader.readexactly(length), timeout=self._timeout
            )

        except TimeoutError as e:
            _LOGGER.error("Timeout during protobuf receive: %s", e)
            raise EDIDIOTimeoutError(f"Receive operation timed out: {e}") from e
        except asyncio.IncompleteReadError as e:
            self._connected = False
            _LOGGER.error("Incomplete read from socket, connection lost: %s", e)
            raise EDIDIOCommunicationError(
                f"Incomplete read, connection lost: {e}"
            ) from e
        except (OSError, ConnectionResetError) as e:
            self._connected = False
            _LOGGER.error(
                "Socket error during protobuf receive, marking as disconnected: %s", e
            )
            raise EDIDIOCommunicationError(f"Failed to receive protobuf: {e}") from e
        except EDIDIOInvalidMessageError as e:
            _LOGGER.warning("Received invalid protobuf message: %s", e)
            raise
        except Exception as e:
            if isinstance(e, (asyncio.CancelledError, KeyboardInterrupt)):
                raise
            _LOGGER.error("An unexpected error occurred during protobuf receive: %s", e)
            raise EDIDIOCommunicationError(
                f"Unexpected protobuf receive error: {e}"
            ) from e

        _LOGGER.debug("Received protobuf payload: %s", payload.hex())
        return payload

    # --- Message Creation Helper Methods ---
    @staticmethod
    def create_dmx_message(
        message_id: int,
        zone: int,
        universe_mask: int,
        channel: int,
        repeat: int,
        level: list[int],
        fade_time_by_10ms: int = 0,
    ) -> bytes:
        """Create a DMX protobuf message and encapsulate it."""
        dmx = pb.DMXMessage(
            zone=zone,
            universe_mask=universe_mask,
            channel=channel,
            repeat=repeat,
            level=level,
            fade_time_by_10ms=fade_time_by_10ms,
        )
        message = pb.EdidioMessage(
            message_id=message_id, dmx_message=dmx
        ).SerializeToString()

        length = len(message)
        length_msb = (length >> 8) & 0xFF
        length_lsb = length & 0xFF

        return bytes([0xCD, length_msb, length_lsb]) + message

    @staticmethod
    def create_dali_message(
        message_id: int,
        line_mask: int,
        address: int,
        *,
        frame_25_bit=None,
        frame_25_bit_reply=None,
        command=None,
        custom_command=None,
        query=None,
        type8=None,
        frame_16_bit=None,
        frame_16_bit_reply=None,
        frame_24_bit=None,
        frame_24_bit_reply=None,
        type8_reply=None,
        device24_setting=None,
        arg=None,
        dtr=None,
        instance_type=None,
        op_code=None,
    ) -> bytes:
        """Create a DALI protobuf message and encapsulate it."""
        dali_msg = pb.DALIMessage(line_mask=line_mask, address=address)

        action_fields = {
            "frame_25_bit": frame_25_bit,
            "frame_25_bit_reply": frame_25_bit_reply,
            "command": command,
            "custom_command": custom_command,
            "query": query,
            "type8": type8,
            "frame_16_bit": frame_16_bit,
            "frame_16_bit_reply": frame_16_bit_reply,
            "frame_24_bit": frame_24_bit,
            "frame_24_bit_reply": frame_24_bit_reply,
            "type8_reply": type8_reply,
            "device24_setting": device24_setting,
        }

        set_count = sum(1 for v in action_fields.values() if v is not None)
        if set_count != 1:
            raise ValueError("Must set exactly one action field in DALIMessage")

        for field_name, value in action_fields.items():
            if value is not None:
                if field_name == "device24_setting":
                    getattr(dali_msg, field_name).CopyFrom(value)
                else:
                    setattr(dali_msg, field_name, value)
                break

        if arg is not None:
            if isinstance(arg, list):
                if len(arg) == 1:
                    dali_msg.arg = arg[0]
                else:
                    _LOGGER.error(
                        "`arg` provided as list with multiple elements but protobuf field `dali_msg.arg` is not a repeated field. Only the first element will be used"
                    )
                    dali_msg.arg = arg[0]
            else:
                dali_msg.arg = arg

        if dtr is not None:
            dali_msg.dtr.dtr.extend(dtr)
        if instance_type is not None:
            dali_msg.instance_type = instance_type
        if op_code is not None:
            dali_msg.op_code = op_code

        message = pb.EdidioMessage(
            message_id=message_id, dali_message=dali_msg
        ).SerializeToString()

        length = len(message)
        length_msb = (length >> 8) & 0xFF
        length_lsb = length & 0xFF

        return bytes([0xCD, length_msb, length_lsb]) + message

    # --- Public Methods for Light Control ---
    async def set_dmx_level(
        self,
        message_id: int,
        zone: int,
        universe_mask: int,
        channel: int,
        level: list[int],
        fade_time_by_10ms: int = 0,
    ) -> None:
        """Send a DMX level command."""
        msg = self.create_dmx_message(
            message_id, zone, universe_mask, channel, 1, level, fade_time_by_10ms
        )
        await self.send_protobuf_message(msg)

    async def set_dali_arc_level(
        self, message_id: int, line_mask: int, address: int, arc_level: int
    ) -> None:
        """Send a DALI ARC_LEVEL command."""
        safe_arc_level = min(max(0, arc_level), DALI_ARC_LEVEL_MAX)
        msg = self.create_dali_message(
            message_id=message_id,
            line_mask=line_mask,
            address=address,
            custom_command=pb.CustomDALICommandType.DALI_ARC_LEVEL,
            arg=[safe_arc_level],
        )
        await self.send_protobuf_message(msg)

    async def send_dali_commands_sequence(self, commands: list[bytes]) -> None:
        """Send a sequence of raw DALI protobuf messages."""
        for cmd in commands:
            await self.send_protobuf_message(cmd)
            await asyncio.sleep(0.05)

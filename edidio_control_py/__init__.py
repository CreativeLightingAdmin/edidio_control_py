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
# eDIDIO DALI address encoding: 0-63 = individual short address, 64-79 = group
# (address = 64 + group), 80 = broadcast. Scenes are recalled with the standard
# DALI "GO TO SCENE X" command (0x10 + scene) sent to the target address.
DALI_GROUP_ADDRESS_BASE = 64
DALI_BROADCAST_ADDRESS = 80
DALI_GO_TO_SCENE_COMMAND_BASE = 0x10
KEEP_ALIVE_INTERVAL_SECONDS = 15
KEEP_ALIVE_MESSAGE = bytes([0xFF, 0xF6])
MAX_RESYNC_BYTES = 4096  # bytes to skip while hunting for a 0xCD frame start

# Re-export the protocol enums so consumers can reference command/scene/Spektra
# constants without reaching into the generated protobuf module.
DALICommandType = pb.DALICommandType
CustomDALICommandType = pb.CustomDALICommandType
SpektraTargetType = pb.SpektraTargetType
SpektraActionType = pb.SpektraActionType
# Authoring enums (0.4.0): sequences/themes/schedules.
SpektraTransitionType = pb.SpektraTransitionType
AlarmRepeatType = pb.AlarmRepeatType
AlarmAstroType = pb.AlarmAstroType
TriggerType = pb.TriggerType
AckMessageType = pb.AckMessageType
DiagnosticMessageType = pb.DiagnosticMessageType
ReadType = pb.ReadType

# Authoring limits (documented; the device also reports exact counts via the
# DIAGNOSTIC_SYSTEM_INFO diagnostic).
SPEKTRA_SEQUENCE_MAX_INDEX = 143   # 144 sequences (0-143)
SPEKTRA_THEME_MAX_INDEX = 15       # firmware MAX_THEMES; confirm at runtime
SPEKTRA_ZONE_MAX_INDEX = 9         # 10 zones (0-9)
ALARM_MAX_INDEX = 8                # 9 user alarms (0-8); index 9 is internal
SPEKTRA_COLOURS_MAX = 20           # colours per sequence/theme
SPEKTRA_TITLE_MAX_LEN = 28
SPEKTRA_MIN_MS_PER_STEP = 50


def _build_trigger(spec: dict | None) -> "pb.TriggerMessage":
    """Build a TriggerMessage from a dict (or an empty trigger if None)."""
    spec = spec or {}
    return pb.TriggerMessage(
        type=spec.get("type", 0),
        zone=spec.get("zone", 0),
        line_mask=spec.get("line_mask", 0),
        target_index=spec.get("target_index", 0),
        value=spec.get("value", 0),
        query_index=spec.get("query_index", 0),
    )


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

    # --- Spektra Message Creation Helpers ---
    @staticmethod
    def create_spektra_control_message(
        message_id: int,
        spektra_type: int,
        zone: int,
        index: int,
        action: int,
    ) -> bytes:
        """Create a SpektraPlus control protobuf message and encapsulate it.

        Args:
            message_id: The message identifier.
            spektra_type: A ``SpektraTargetType`` value (SEQUENCE, THEME, STATIC).
            zone: The target SpektraPlus zone.
            index: The stored sequence/theme/static index.
            action: A ``SpektraActionType`` value (START, STOP, PAUSE).

        """
        control = pb.SpektraControlMessage(
            type=spektra_type, zone=zone, index=index, action=action
        )
        message = pb.EdidioMessage(
            message_id=message_id, spektra_control=control
        ).SerializeToString()

        length = len(message)
        length_msb = (length >> 8) & 0xFF
        length_lsb = length & 0xFF

        return bytes([0xCD, length_msb, length_lsb]) + message

    @staticmethod
    def create_spektra_stop_message(
        message_id: int, zone: int, line_mask: int = 0xFF
    ) -> bytes:
        """Create a SpektraPlus stop external-trigger message and encapsulate it.

        Unlike a SpektraControl STOP action (which just halts), the
        ``SPEKTRA_STOP_SEQ`` external trigger also turns the output off, matching
        how the SpektraPlus app stops playback.
        """
        trigger = pb.TriggerMessage(
            type=pb.TriggerType.SPEKTRA_STOP_SEQ,
            zone=zone,
            line_mask=line_mask,
            target_index=0,
            value=0,
        )
        external = pb.ExternalTriggerMessage(trigger=trigger)
        message = pb.EdidioMessage(
            message_id=message_id, external_trigger=external
        ).SerializeToString()

        length = len(message)
        length_msb = (length >> 8) & 0xFF
        length_lsb = length & 0xFF

        return bytes([0xCD, length_msb, length_lsb]) + message

    # --- Authoring helpers (0.4.0): time packing -----------------------------
    @staticmethod
    def pack_time(hour: int, minute: int, second: int = 0) -> int:
        """Pack an hour/minute/second into the device's TimeClock ``time`` field.

        Layout (from firmware): ``(hour << 16) | (minute << 8) | second``.
        """
        return ((hour & 0xFF) << 16) | ((minute & 0xFF) << 8) | (second & 0xFF)

    @staticmethod
    def pack_date(year: int = 0, month: int = 0, date: int = 0, weekday: int = 0) -> int:
        """Pack a date into the device's TimeClock ``date`` field.

        Layout (from firmware): ``(year << 24) | (month << 16) | (date << 8) |
        weekday``. Year is 0-99, month 1-12, date is day-of-month, weekday is
        1 (Mon) - 7 (Sun). All parts are optional (0) for a time-only alarm.
        """
        return (
            ((year & 0xFF) << 24)
            | ((month & 0xFF) << 16)
            | ((date & 0xFF) << 8)
            | (weekday & 0xFF)
        )

    # --- Authoring builders (0.4.0): sequences / themes / schedules ----------
    @staticmethod
    def create_spektra_sequence_message(
        message_id: int,
        index: int,
        seq_type: int,
        colours: list[list[int]],
        *,
        transition: int = 0,
        fade_time_by_10ms: int = 0,
        time_per_colour: int = 0,
        time_per_colour_unit: int = 0,
        time_per_step: int = 0,
        time_per_step_unit: int = 0,
        range: int = 0,
        is_randomised_type: int = 0,
        random_types_mask: int = 0,
        is_reverse_direction: int = 0,
        is_cycle_direction: int = 0,
        title: str = "",
        has_random_colour_order: bool = False,
        args: list[int] | None = None,
    ) -> bytes:
        """Create a SpektraSequenceConfigMessage (authors + saves a sequence).

        Args:
            index: Sequence index (0-143).
            seq_type: Animation type (0=BLEND, 4=ROTATE, 5=TWINKLE, ... see the
                capability guide / firmware for the full list).
            colours: A list of colours, each a list of per-channel values (0-255),
                length matching the target zone's channels-per-light.
            transition: A ``SpektraTransitionType`` (BLEND/SNAP/FADE_TO_BLACK).
            time_per_step / time_per_colour: Animation timing (with their _unit
                fields: 0=ms, 1=s, 2=min, 3=hr). Min 50ms per step.
            title: Sequence name (<= 28 chars).

        The controller auto-persists the sequence to flash on receipt.
        """
        colour_msgs = [
            pb.SpektraColourConfigMessage(channel_value=list(c)) for c in colours
        ]
        seq = pb.SpektraSequenceConfigMessage(
            index=index,
            type=seq_type,
            transition=transition,
            fade_time_by_10ms=fade_time_by_10ms,
            time_per_colour=time_per_colour,
            time_per_colour_unit=time_per_colour_unit,
            time_per_step=time_per_step,
            time_per_step_unit=time_per_step_unit,
            range=range,
            is_randomised_type=is_randomised_type,
            random_types_mask=random_types_mask,
            is_reverse_direction=is_reverse_direction,
            is_cycle_direction=is_cycle_direction,
            title=title[:SPEKTRA_TITLE_MAX_LEN],
            has_random_colour_order=has_random_colour_order,
            colours=colour_msgs,
            args=list(args or []),
        )
        message = pb.EdidioMessage(
            message_id=message_id, spektra_sequence=seq
        ).SerializeToString()
        length = len(message)
        return bytes([0xCD, (length >> 8) & 0xFF, length & 0xFF]) + message

    @staticmethod
    def create_spektra_theme_message(
        message_id: int, index: int, colours: list[list[int]], *, title: str = ""
    ) -> bytes:
        """Create a SpektraThemeConfigMessage (authors + saves a static theme).

        Args:
            index: Theme index (0-15).
            colours: Palette colours, each a list of per-channel values (0-255).
            title: Theme name (<= 28 chars).
        """
        colour_msgs = [
            pb.SpektraColourConfigMessage(channel_value=list(c)) for c in colours
        ]
        theme = pb.SpektraThemeConfigMessage(
            index=index, title=title[:SPEKTRA_TITLE_MAX_LEN], colours=colour_msgs
        )
        message = pb.EdidioMessage(
            message_id=message_id, spektra_theme=theme
        ).SerializeToString()
        length = len(message)
        return bytes([0xCD, (length >> 8) & 0xFF, length & 0xFF]) + message

    @staticmethod
    def create_alarm_message(
        message_id: int,
        index: int,
        *,
        enabled: bool = True,
        start_time: int = 0,
        start_date: int = 0,
        end_time: int = 0,
        end_date: int = 0,
        start_trigger: dict | None = None,
        end_trigger: dict | None = None,
        astro_start: int = 0,
        astro_end: int = 0,
        repeat: int = 0,
        repeat_day_bitmask: int = 0,
        repeat_month_bitmask: int = 0,
        yearly: bool = False,
        start_offset_is_before: bool = False,
        end_offset_is_before: bool = False,
    ) -> bytes:
        """Create an AlarmMessage (authors a schedule).

        Args:
            index: Alarm index (0-8).
            start_time / end_time: Packed time (see :meth:`pack_time`).
            start_date / end_date: Packed date (see :meth:`pack_date`); 0 for a
                repeating time-only alarm.
            start_trigger / end_trigger: dicts with keys ``type`` (a
                ``TriggerType``, e.g. ``SPEKTRA_START_SEQ``), ``zone``,
                ``line_mask``, ``target_index``, ``value``, ``query_index``.
            repeat: An ``AlarmRepeatType`` (NO_REPEAT/DAILY/WORK_DAY/WEEKLY/MONTHLY).
            astro_start / astro_end: ``AlarmAstroType`` (NO_ASTRO/SUNRISE/SUNSET).
        """
        alarm = pb.AlarmMessage(
            index=index,
            enabled=enabled,
            start_time=pb.TimeClockMessage(date=start_date, time=start_time),
            end_time=pb.TimeClockMessage(date=end_date, time=end_time),
            start_trigger=_build_trigger(start_trigger),
            end_trigger=_build_trigger(end_trigger),
            astro_start=astro_start,
            astro_end=astro_end,
            repeat=repeat,
            repeat_day_bitmask=repeat_day_bitmask,
            repeat_month_bitmask=repeat_month_bitmask,
            yearly=yearly,
            start_offset_is_before=start_offset_is_before,
            end_offset_is_before=end_offset_is_before,
        )
        message = pb.EdidioMessage(
            message_id=message_id, alarm=alarm
        ).SerializeToString()
        length = len(message)
        return bytes([0xCD, (length >> 8) & 0xFF, length & 0xFF]) + message

    @staticmethod
    def create_spektra_calendar_message(
        message_id: int,
        target_type: int,
        index: int,
        days: list[bool],
        *,
        is_override: bool = False,
    ) -> bytes:
        """Create a SpektraCalendarMessage (assigns a sequence/theme to days).

        Args:
            target_type: ``SpektraTargetType.SEQUENCE`` or ``THEME``.
            index: the sequence/theme index to run on the selected days.
            days: a list of booleans, one per day-of-year (up to 366). Each True
                day runs the given sequence/theme; re-sending the same index/type
                for an already-set day toggles it off (matches the app).
            is_override: mark this as an override assignment.
        """
        cal = pb.SpektraCalendarMessage(
            type=target_type, index=index, days=list(days), isOverride=is_override
        )
        message = pb.EdidioMessage(
            message_id=message_id, spektra_calendar=cal
        ).SerializeToString()
        length = len(message)
        return bytes([0xCD, (length >> 8) & 0xFF, length & 0xFF]) + message

    # --- Read / query builders (0.4.0) --------------------------------------
    @staticmethod
    def create_spektra_read_message(message_id: int, target_type: int, index: int) -> bytes:
        """Create a SpektraReadMessage to read back a zone setting / sequence /
        theme. ``target_type`` is a ``SpektraTargetType`` (SETTINGS/SEQUENCE/THEME)."""
        read = pb.SpektraReadMessage(type=target_type, index=index)
        message = pb.EdidioMessage(
            message_id=message_id, spektra_read=read
        ).SerializeToString()
        length = len(message)
        return bytes([0xCD, (length >> 8) & 0xFF, length & 0xFF]) + message

    @staticmethod
    def create_read_device_message(
        message_id: int, read_type: int, index: int = 0, secondary_index: int = 0, profile: int = 0
    ) -> bytes:
        """Create a ReadDeviceMessage (e.g. read alarms with ReadType.ALARMS, or
        DMX/DALI cache with ReadType.POLL_DATA)."""
        read = pb.ReadDeviceMessage(
            profile=profile, type=read_type, index=index, secondary_index=secondary_index
        )
        message = pb.EdidioMessage(
            message_id=message_id, read_device=read
        ).SerializeToString()
        length = len(message)
        return bytes([0xCD, (length >> 8) & 0xFF, length & 0xFF]) + message

    @staticmethod
    def create_diagnostic_message(
        message_id: int, diag_type: int, line: int = 0, page: int = 0
    ) -> bytes:
        """Create a DiagnosticMessage (e.g. DIAGNOSTIC_SYSTEM_INFO for capability
        counts, or DMX_LEVEL_CACHE to read a DMX universe's cached levels)."""
        diag = pb.DiagnosticMessage(type=diag_type, line=line, page=page)
        message = pb.EdidioMessage(
            message_id=message_id, diag_message=diag
        ).SerializeToString()
        length = len(message)
        return bytes([0xCD, (length >> 8) & 0xFF, length & 0xFF]) + message

    # --- Admin: device time get/set -----------------------------------------
    @staticmethod
    def create_get_device_time_message(message_id: int) -> bytes:
        """Create an AdminMessage(GET, DEVICE_TIME) to read the controller's clock.
        The reply's ``admin_message.device_time.time`` holds the packed date/time."""
        admin = pb.AdminMessage(
            command=pb.AdminCommandType.GET, target=pb.AdminPropertyType.DEVICE_TIME)
        message = pb.EdidioMessage(
            message_id=message_id, admin_message=admin
        ).SerializeToString()
        length = len(message)
        return bytes([0xCD, (length >> 8) & 0xFF, length & 0xFF]) + message

    @staticmethod
    def create_set_device_time_message(
        message_id: int, *, year: int, month: int, day: int, weekday: int,
        hour: int, minute: int, second: int = 0,
    ) -> bytes:
        """Create an AdminMessage(SET, DEVICE_TIME) to set the controller's clock.
        Year is 0-99 (2000+), weekday 1=Mon..7=Sun (wall-clock, local time)."""
        admin = pb.AdminMessage(
            command=pb.AdminCommandType.SET, target=pb.AdminPropertyType.DEVICE_TIME,
            device_time=pb.UpdateTimeMessage(time=pb.TimeClockMessage(
                date=EdidioClient.pack_date(year, month, day, weekday),
                time=EdidioClient.pack_time(hour, minute, second))))
        message = pb.EdidioMessage(
            message_id=message_id, admin_message=admin
        ).SerializeToString()
        length = len(message)
        return bytes([0xCD, (length >> 8) & 0xFF, length & 0xFF]) + message

    # --- Request/response helper (0.4.0) ------------------------------------
    async def _receive_framed(self) -> bytes:
        """Read one 0xCD-framed message payload, resynchronising on the 0xCD
        start byte.

        The device interleaves unsolicited traffic (keep-alive ``0xFF 0xF6``
        frames, live-data pushes) with request replies, so we skip any bytes
        that are not the start of a frame instead of hard-failing on them.
        """
        if not self.connected:
            raise EDIDIOConnectionError("Not connected to eDIDIO device for receiving.")
        assert self._reader is not None

        try:
            # Resync: read one byte at a time until we hit a frame start (0xCD).
            for _ in range(MAX_RESYNC_BYTES):
                first = await asyncio.wait_for(
                    self._reader.readexactly(1), timeout=self._timeout
                )
                if first[0] == 0xCD:
                    break
            else:
                raise EDIDIOInvalidMessageError(
                    "No 0xCD frame start found while resynchronising."
                )
            size = await asyncio.wait_for(
                self._reader.readexactly(2), timeout=self._timeout
            )
            length = (size[0] << 8) | size[1]
            if length <= 0:
                raise EDIDIOInvalidMessageError(f"Invalid message length: {length}")
            return await asyncio.wait_for(
                self._reader.readexactly(length), timeout=self._timeout
            )
        except TimeoutError as e:
            raise EDIDIOTimeoutError(f"Receive operation timed out: {e}") from e
        except asyncio.IncompleteReadError as e:
            self._connected = False
            raise EDIDIOCommunicationError(f"Incomplete read, connection lost: {e}") from e

    async def request(self, message: bytes, *, max_frames: int = 12) -> "pb.EdidioMessage":
        """Send a framed message and decode the reply as an EdidioMessage.

        Resynchronises on the 0xCD frame start and matches the reply to the
        request's ``message_id``, skipping unsolicited frames (keep-alives,
        live-data pushes) the device may interleave. Returns the parsed
        ``EdidioMessage`` (which may carry an ``ack`` or a read payload such as
        ``spektra_settings`` / ``level_cache_response`` / ``diag_*``). Use
        ``msg.WhichOneof('payload')`` to inspect it.
        """
        sent = pb.EdidioMessage()
        sent.ParseFromString(message[3:])
        want_id = sent.message_id

        await self.send_protobuf_message(message)
        reply = pb.EdidioMessage()
        for _ in range(max_frames):
            payload = await self._receive_framed()
            reply = pb.EdidioMessage()
            reply.ParseFromString(payload)
            if reply.message_id == want_id:
                return reply
            # different id -> unsolicited push; keep reading for our reply
        return reply

    # --- Additional Public Methods for Light Control ---
    async def set_dali_group_arc_level(
        self, message_id: int, line_mask: int, group: int, arc_level: int
    ) -> None:
        """Send an arc level to a whole DALI group. ``arc_level`` clamped to 0-254.

        Encoded as a standard DALI_ARC_LEVEL to the group address (64 + group).
        """
        safe_arc_level = min(max(0, arc_level), DALI_ARC_LEVEL_MAX)
        msg = self.create_dali_message(
            message_id=message_id,
            line_mask=line_mask,
            address=DALI_GROUP_ADDRESS_BASE + group,
            custom_command=pb.CustomDALICommandType.DALI_ARC_LEVEL,
            arg=[safe_arc_level],
        )
        await self.send_protobuf_message(msg)

    async def send_dali_command(
        self,
        message_id: int,
        line_mask: int,
        address: int,
        command: int,
        arg: int = 0,
    ) -> None:
        """Send a standard DALI command (a ``DALICommandType`` value).

        Examples: ``DALICommandType.DALI_OFF``, ``DALI_MAX_LEVEL``,
        ``DALI_FADE_UP``. ``arg`` carries a command argument where applicable.
        """
        msg = self.create_dali_message(
            message_id=message_id,
            line_mask=line_mask,
            address=address,
            command=command,
            arg=[arg],
        )
        await self.send_protobuf_message(msg)

    async def recall_dali_scene(
        self, message_id: int, line_mask: int, scene: int
    ) -> None:
        """Recall a stored DALI scene across all fittings on the line(s).

        Encoded as the standard DALI "GO TO SCENE X" command (0x10 + scene)
        broadcast to address 80.
        """
        msg = self.create_dali_message(
            message_id=message_id,
            line_mask=line_mask,
            address=DALI_BROADCAST_ADDRESS,
            command=DALI_GO_TO_SCENE_COMMAND_BASE + scene,
        )
        await self.send_protobuf_message(msg)

    async def recall_dali_scene_on_group(
        self, message_id: int, line_mask: int, group: int, scene: int
    ) -> None:
        """Recall a stored DALI scene on a specific group.

        Encoded as the standard DALI "GO TO SCENE X" command (0x10 + scene) sent to
        the group address (64 + group).
        """
        msg = self.create_dali_message(
            message_id=message_id,
            line_mask=line_mask,
            address=DALI_GROUP_ADDRESS_BASE + group,
            command=DALI_GO_TO_SCENE_COMMAND_BASE + scene,
        )
        await self.send_protobuf_message(msg)

    async def send_spektra_control(
        self,
        message_id: int,
        spektra_type: int,
        zone: int,
        index: int,
        action: int,
    ) -> None:
        """Start/stop/pause a SpektraPlus sequence, theme, or static scene."""
        msg = self.create_spektra_control_message(
            message_id, spektra_type, zone, index, action
        )
        await self.send_protobuf_message(msg)

    async def send_spektra_stop(
        self, message_id: int, zone: int, line_mask: int = 0xFF
    ) -> None:
        """Stop SpektraPlus playback on a zone and turn the output off."""
        msg = self.create_spektra_stop_message(message_id, zone, line_mask)
        await self.send_protobuf_message(msg)

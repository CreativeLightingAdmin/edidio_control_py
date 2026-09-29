"""Live event stream: receive the controller's pushed events (0.5.0).

The rest of :class:`EdidioClient` is request/response. The controller can also
*push* events — DALI bus frames, inputs, sensors, Spektra playback, commands — to a
client that subscribes. :class:`EventStream` owns a **dedicated connection** for
that, with a background reader that decodes each push into a compact dict and hands
it to your callback (and a rolling buffer for pollers).

Two firmware generations are supported:

* **v2** (firmware >= 1.4.0): ``EventStreamMessage`` (tag 77), subscribe with a
  category bitmask; the device pushes a typed envelope. Verified live on 1.5.7.
* **legacy** (older firmware): ``EventMessage`` (tag 34) with ``REGISTER`` and an
  ``EventFilter``.

The stream survives connection loss: it reconnects with backoff and resubscribes.

Example::

    async def on_event(ev):
        print(ev["kind"], ev)

    stream = EventStream("192.168.1.50", on_event=on_event)
    await stream.start(["dali", "inputs"])
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import datetime
from typing import Awaitable, Callable, Iterable

from . import eDS10_ProtocolBuffer_pb2 as pb
from ._v2 import eDS10_v2_pb2 as v2
from .exceptions import EDIDIOConnectionError, EDIDIOTimeoutError

_LOGGER = logging.getLogger(__name__)

EventCallback = Callable[[dict], Awaitable[None]]

# --- v2 (EventStreamMessage, tag 77) -----------------------------------------

# Log category bit positions (SpektraPlus eventStreamV2.ts EVENT_CATEGORIES), plus
# aliases so legacy category names (dali_command, triggers, …) map sensibly.
CATEGORY_BITS = {
    "sys": 0, "trigger": 1, "triggers": 1, "inputs": 2, "input": 2,
    "dali": 3, "dali_command": 3, "dali_arc": 3, "dali_24_frame": 3,
    "sensors": 4, "sensor": 4, "schedule": 5, "lists": 6, "list": 6,
    "config": 7, "network": 8, "sd": 9, "event": 10,
}
# Structured events (InputEvent/SensorEvent/SpektraEvent/…) arrive under bit 10 —
# always included so we never miss structured pushes.
_EVENT_BIT = 1 << 10
ALL_CATEGORIES_MASK = 0x7FF
DEFAULT_LEVEL_THRESHOLD = 2  # 0=ERR,1=WARN,2=INFO
DEFAULT_CATEGORIES = ("inputs", "sensors", "triggers", "dali")

# DaliFrame direction / frame_type / status (mirrors SpektraPlus daliConstants.ts).
# TX and RX use different frame_type namespaces:
#   TX (direction 0): 0=8-bit, 1=16-bit, 2=24-bit; status 0=SUCCESS, else an
#       outcome code (240 NO_ECHO, 241 BUSY, 242 TIMEOUT, 243 ERROR, 244 PARTIAL,
#       245 COLLISION, 246 ATTEMPT; older firmware used 1-7 for the same names).
#       A failed transmission is reported as ATTEMPT then e.g. TIMEOUT per retry.
#   RX (direction 1): 3=8-bit, 4=16-bit, 5=24-bit; 2/6/254/255 are receive errors.
DALI_DIR_TX = 0
DALI_DIR_RX = 1
DALI_DIR_META = 0xFF
DALI_FRAME_8 = 0
DALI_FRAME_16 = 1
DALI_FRAME_24 = 2
DALI_RX_FRAME_16 = 4
DALI_TX_SUCCESS = 0
DALI_TX_STATUS_NAMES = {
    0: "SUCCESS", 240: "NO_ECHO", 241: "BUSY", 242: "TIMEOUT", 243: "ERROR",
    244: "PARTIAL", 245: "COLLISION", 246: "ATTEMPT",
    1: "NO_ECHO", 2: "BUSY", 3: "TIMEOUT", 4: "ERROR", 5: "PARTIAL",
    6: "COLLISION", 7: "ATTEMPT",
}


def is_16bit_frame(frame_type: int, direction: int = DALI_DIR_TX) -> bool:
    """True for a 16-bit DALI forward frame (address byte + data byte)."""
    if direction == DALI_DIR_RX:
        return frame_type == DALI_RX_FRAME_16
    return direction == DALI_DIR_TX and frame_type == DALI_FRAME_16


def dali_frame_ok(direction: int, frame_type: int, status: int) -> bool:
    """True if the frame really happened on the bus: a TX that completed with
    SUCCESS, or a successfully received RX frame (not an attempt/timeout/error)."""
    if direction == DALI_DIR_TX:
        return status == DALI_TX_SUCCESS
    if direction == DALI_DIR_RX:
        return frame_type in (3, 4, 5)
    return False


# A few common 16-bit DALI command frames, for readable bus decoding.
_DALI_CMD_NAMES = {0x00: "OFF", 0x01: "FADE_UP", 0x02: "FADE_DOWN",
                   0x05: "MAX_LEVEL", 0x06: "MIN_LEVEL"}
# Special commands (address byte 101xxxxx / 110xxxxx), IEC 62386-102.
_DALI_SPECIAL = {0xA1: "TERMINATE", 0xA3: "DTR0", 0xA5: "INITIALISE", 0xA7: "RANDOMISE",
                 0xA9: "COMPARE", 0xAB: "WITHDRAW", 0xAD: "PING", 0xB1: "SEARCHADDRH",
                 0xB3: "SEARCHADDRM", 0xB5: "SEARCHADDRL", 0xB7: "PROGRAM SHORT ADDRESS",
                 0xB9: "VERIFY SHORT ADDRESS", 0xBB: "QUERY SHORT ADDRESS",
                 0xC1: "ENABLE DEVICE TYPE", 0xC3: "DTR1", 0xC5: "DTR2"}


def _frame(body: bytes) -> bytes:
    return bytes([0xCD, (len(body) >> 8) & 0xFF, len(body) & 0xFF]) + body


def categories_to_mask(categories: Iterable[str] | None) -> int:
    """Turn friendly category names into a v2 bitmask. Always includes the
    structured-event category; empty/unknown input falls back to everything."""
    mask = 0
    for c in categories or []:
        bit = CATEGORY_BITS.get(c)
        if bit is not None:
            mask |= 1 << bit
    if mask == 0:
        return ALL_CATEGORIES_MASK
    return mask | _EVENT_BIT


def build_subscribe(category_mask: int = ALL_CATEGORIES_MASK,
                    level_threshold: int = DEFAULT_LEVEL_THRESHOLD,
                    message_id: int = 1) -> bytes:
    return _frame(v2.EdidioMessage(
        message_id=message_id,
        event_stream=v2.EventStreamMessage(
            subscribe=True, category_mask=category_mask,
            level_threshold=level_threshold)).SerializeToString())


def build_unsubscribe(message_id: int = 2) -> bytes:
    return _frame(v2.EdidioMessage(
        message_id=message_id,
        event_stream=v2.EventStreamMessage(subscribe=False)).SerializeToString())


def describe_dali_frame(frame: int, frame_type: int, direction: int = DALI_DIR_TX) -> str:
    """Best-effort human description of a raw DALI frame value."""
    if is_16bit_frame(frame_type, direction):
        addr, data = (frame >> 8) & 0xFF, frame & 0xFF
        if addr == 0xFF:
            return f"broadcast {_DALI_CMD_NAMES.get(data, hex(data))}"
        if addr == 0xFE:
            return f"broadcast DAPC level {data}"
        if 0xA0 <= addr <= 0xCB:                 # special commands
            return f"{_DALI_SPECIAL.get(addr, f'special 0x{addr:02X}')} {data}"
        if addr & 0xE0 == 0x80:                  # 100GGGGS: group address
            group = (addr >> 1) & 0x0F
            if addr & 0x01:
                return f"group {group} cmd {_DALI_CMD_NAMES.get(data, hex(data))}"
            return f"group {group} arc {data}"
        if addr & 0x01:
            return f"addr {(addr >> 1) & 0x3F} cmd {_DALI_CMD_NAMES.get(data, hex(data))}"
        return f"addr {(addr >> 1) & 0x3F} arc {data}"
    return f"frame 0x{frame:06X}"


def _trigger_info(t) -> dict:
    return {"type": t.trigger_type, "target": t.target_index,
            "value": t.value, "line_mask": t.line_mask}


def parse_v2(data: bytes):
    """Parse framed body bytes into a v2 EdidioMessage (or None)."""
    msg = v2.EdidioMessage()
    try:
        msg.ParseFromString(data)
    except Exception:  # noqa: BLE001
        return None
    return msg


def is_event_stream(msg) -> bool:
    return msg is not None and msg.WhichOneof("payload") == "event_stream"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def decode_v2(msg) -> dict | None:
    """Decode a v2 EdidioMessage(event_stream) into a compact dict, or None for the
    subscribe ACK / non-events."""
    es = msg.event_stream
    if es.ack:
        return None
    base = {"seq": es.sequence, "category": es.category, "level": es.level,
            "at": _now()}
    entry = es.WhichOneof("entry")
    if entry == "dali":
        d = es.dali
        base.update(kind="dali", line=d.line, direction=d.direction,
                    frame_type=d.frame_type, status=d.status, frame=d.frame,
                    ok=dali_frame_ok(d.direction, d.frame_type, d.status),
                    decoded=describe_dali_frame(d.frame, d.frame_type, d.direction))
        if d.direction == DALI_DIR_TX:
            base["status_name"] = DALI_TX_STATUS_NAMES.get(d.status, f"STATUS_{d.status}")
    elif entry == "input":
        i = es.input
        base.update(kind="input", index=i.input_index, source=i.source,
                    press=i.press_type, action=_trigger_info(i.action),
                    dali_line=i.dali_line, dali_address=i.dali_address)
    elif entry == "sensor":
        s = es.sensor
        base.update(kind="sensor", index=s.sensor_index, motion=s.motion_state,
                    light=s.light_state, lux=s.lux_value,
                    dali_line=s.dali_line, dali_address=s.dali_address)
    elif entry == "spektra":
        s = es.spektra
        base.update(kind="spektra", zone=s.zone, action=s.action,
                    target=s.target, index=s.index)
    elif entry == "command":
        cmd = es.command
        base.update(kind="command", source=cmd.source,
                    action=_trigger_info(cmd.action), zone=cmd.zone)
    elif entry == "net":
        base.update(kind="net", event=es.net.event)
    elif entry == "text":
        base.update(kind="text", text=es.text)
    else:
        base.update(kind=entry or "unknown")
    return base


# --- legacy (EventMessage REGISTER, tag 34) ----------------------------------

# EventFilter fields, exposed as friendly category names.
LEGACY_CATEGORY_FIELDS = {
    "inputs": "input",
    "dali_arc": "dali_arc_level",
    "dali_command": "dali_command",
    "sensors": "dali_sensor",
    "dali_inputs": "dali_input",
    "dmx_changed": "dmx_stream_changed",
    "dali_24_frame": "dali_24_frame",
    "triggers": "trigger_message",
}
# "dali" is the v2 umbrella; on legacy firmware it means arc + command frames.
_LEGACY_ALIASES = {"dali": ("dali_arc", "dali_command")}

_MOTION_STATES = {v.number: v.name for v in pb.DALIMotionSensorStates.DESCRIPTOR.values}
_TRIGGER_TYPES = {v.number: v.name for v in pb.TriggerType.DESCRIPTOR.values}


def build_legacy_register(message_id: int, categories: Iterable[str]) -> bytes:
    """Frame an EventMessage(REGISTER) with an EventFilter for the categories."""
    kwargs = {}
    for cat in categories:
        for name in _LEGACY_ALIASES.get(cat, (cat,)):
            field = LEGACY_CATEGORY_FIELDS.get(name)
            if field:
                kwargs[field] = True
    filt = pb.EventFilter(**kwargs)
    body = pb.EdidioMessage(
        message_id=message_id,
        event=pb.EventMessage(event=pb.EventType.REGISTER, filter=filt),
    ).SerializeToString()
    return _frame(body)


def decode_legacy(ev) -> dict | None:
    """Turn a legacy EventMessage into a compact dict."""
    which = ev.WhichOneof("event_data")
    base = {"event": pb.EventType.Name(ev.event), "at": _now()}
    if which == "sensor":
        s = ev.sensor
        base.update(kind="sensor", index=s.index, line=s.line, address=s.address,
                    motion=_MOTION_STATES.get(s.motion_state, s.motion_state),
                    lux_level=s.lux_level)
    elif which == "trigger":
        t = ev.trigger
        base.update(kind="trigger", type=_TRIGGER_TYPES.get(t.type, t.type),
                    source=t.source, zone=t.zone, line_mask=t.line_mask,
                    target=t.target_address, value=t.value)
    elif which == "inputs":
        base.update(kind="input", input_mask=ev.inputs.input_mask,
                    inputs=list(ev.inputs.inputs))
    elif which == "dali_24_input":
        d = ev.dali_24_input
        base.update(kind="dali_input", index=d.index, line=d.line,
                    address=d.address, type=d.type, arg=d.arg)
    elif which == "dali_24_frame":
        d = ev.dali_24_frame
        base.update(kind="dali_frame", line=d.line, frame=d.frame)
    elif which == "payload":
        base.update(kind="payload")
    else:
        base.update(kind=which or "unknown")
    return base


def decode_frame(data: bytes, *, use_v2: bool) -> dict | None:
    """Decode one framed body into an event dict, or None if it isn't an event."""
    if use_v2:
        msg = parse_v2(data)
        return decode_v2(msg) if is_event_stream(msg) else None
    msg = pb.EdidioMessage()
    try:
        msg.ParseFromString(data)
    except Exception:  # noqa: BLE001
        return None
    if msg.WhichOneof("payload") != "event":
        return None
    return decode_legacy(msg.event)


# --- the stream --------------------------------------------------------------

class EventStream:
    """A dedicated connection that subscribes to and delivers device events.

    ``on_event`` (optional, async) is called for every decoded event; a failing
    callback is logged and never kills the reader. Events are also kept in a rolling
    buffer, readable with :meth:`recent`.
    """

    def __init__(self, host: str, port: int = 23, use_tls: bool = False,
                 *, buffer_size: int = 200, client_factory=None,
                 on_event: EventCallback | None = None, use_v2: bool = True,
                 reconnect_min: float = 1.0, reconnect_max: float = 10.0):
        from . import EdidioClient  # local: avoid a circular import at load time

        self._factory = client_factory or (
            lambda: EdidioClient(host, port, use_tls=use_tls))
        self._client = None
        self._task: asyncio.Task | None = None
        self._buffer: deque = deque(maxlen=buffer_size)
        self._seq = 0
        self._categories: tuple = ()
        self._running = False
        self._on_event = on_event
        self._use_v2 = use_v2
        self._reconnect_min = reconnect_min
        self._reconnect_max = reconnect_max
        self.reconnects = 0

    @property
    def running(self) -> bool:
        return self._running

    @property
    def connected(self) -> bool:
        """True while subscribed on a live connection (False while reconnecting)."""
        return (self._running and self._client is not None
                and bool(getattr(self._client, "connected", False)))

    @property
    def categories(self) -> tuple:
        return self._categories

    @property
    def last_seq(self) -> int:
        return self._seq

    @property
    def on_event(self) -> EventCallback | None:
        return self._on_event

    @on_event.setter
    def on_event(self, callback: EventCallback | None) -> None:
        self._on_event = callback

    async def _subscribe(self) -> None:
        self._client = self._factory()
        await self._client.connect()
        if self._use_v2:
            mask = categories_to_mask(self._categories)
            await self._client.send_protobuf_message(build_subscribe(mask))
        else:
            await self._client.send_protobuf_message(
                build_legacy_register(1, self._categories))

    async def start(self, categories: Iterable[str] = DEFAULT_CATEGORIES) -> None:
        """Connect, subscribe and start the background reader. Raises if the first
        connect fails; later drops are handled by reconnecting."""
        if self._running:
            await self.stop()
        self._categories = tuple(categories)
        await self._subscribe()
        self._running = True
        self._task = asyncio.ensure_future(self._read_loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._task = None
        await self._close_client(unsubscribe=True)

    async def _close_client(self, *, unsubscribe: bool) -> None:
        if self._client is None:
            return
        try:
            if unsubscribe and self._use_v2 and getattr(self._client, "connected", True):
                await self._client.send_protobuf_message(build_unsubscribe())
            await self._client.disconnect()
        except Exception:  # noqa: BLE001
            pass
        self._client = None

    async def _reconnect(self) -> None:
        """Reconnect + resubscribe with exponential backoff until it works."""
        delay = self._reconnect_min
        while self._running:
            await self._close_client(unsubscribe=False)
            try:
                await self._subscribe()
                self.reconnects += 1
                _LOGGER.info("Event stream resubscribed (reconnect #%d)", self.reconnects)
                return
            except EDIDIOConnectionError as err:
                _LOGGER.warning("Event stream reconnect failed (%s); retry in %.0fs", err, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, self._reconnect_max)

    async def _read_loop(self) -> None:
        while self._running:
            try:
                data = await self._client._receive_framed()  # resync-capable framing
            except asyncio.CancelledError:
                break
            except EDIDIOTimeoutError:
                continue                           # idle bus — nothing pushed
            except Exception as err:  # noqa: BLE001
                if not self._running:
                    break
                if self._client is None or not self._client.connected:
                    _LOGGER.warning("Event stream connection lost: %s", err)
                    await self._reconnect()
                else:
                    _LOGGER.debug("event read error: %s", err)
                    await asyncio.sleep(0.2)
                continue
            decoded = decode_frame(data, use_v2=self._use_v2)
            if decoded is not None:
                await self._deliver(decoded)

    async def _deliver(self, decoded: dict) -> None:
        self._seq += 1
        decoded["seq"] = self._seq     # our own monotonic index (device's resets on reconnect)
        self._buffer.append(decoded)
        if self._on_event is not None:
            try:
                await self._on_event(decoded)
            except Exception as err:  # noqa: BLE001 — a hook must not kill the reader
                _LOGGER.warning("on_event callback error: %s", err)

    def recent(self, since_seq: int = 0, limit: int = 50) -> list:
        """Buffered events with seq > since_seq (most recent, up to limit)."""
        items = [e for e in self._buffer if e["seq"] > since_seq]
        return items[-limit:]

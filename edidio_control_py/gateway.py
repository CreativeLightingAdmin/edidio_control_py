"""Shared gateway plumbing: a thread-safe intent dispatcher + live state feed (0.5.0).

Every bridge (MQTT, KNX, Modbus, OSC, MIDI, HomeKit, game-state, ambient data…)
translates its own protocol into a small, protocol-neutral **intent** dict and
submits it here. :class:`EdidioDispatcher` owns a persistent :class:`EdidioClient`
(keep-alive + reconnect), drains a queue on a background worker, and executes each
intent — so callbacks from any thread (paho-mqtt, pymodbus RTU, mido) are safe.

Intent vocabulary (``kind`` + fields; ``line`` is 1-based):

* ``dali_level``        line, address, level
* ``dali_group_level``  line, group, level
* ``dali_scene``        line, scene, [group]
* ``dali_command``      line, address, command
* ``spektra``           type (sequence/theme/static), zone, index, action (start/stop/pause)
* ``spektra_stop``      zone
* ``dmx_color``         line, rgb, [fixtures]

**State feedback.** Pass ``on_state`` (async ``(DaliChange, touched) -> None``) and
the dispatcher also opens an :class:`~edidio_control_py.events.EventStream`, decodes
DALI bus frames with :func:`~edidio_control_py.state.dali_change`, tracks levels in
:attr:`levels`, and calls you for every change — including ones made by wall
panels, schedules or other apps. ``on_event`` receives every raw event dict.
Feedback needs firmware >= 1.4.0; if the stream can't start, commands still work.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable

from . import EdidioClient, SpektraActionType, SpektraTargetType
from .events import EventStream
from .exceptions import EDIDIOConnectionError
from .state import DaliChange, LevelTracker, dali_change

_LOGGER = logging.getLogger(__name__)

StateCallback = Callable[[DaliChange, list], Awaitable[None]]


def line_mask(line: int) -> int:
    """1-based DALI/DMX line number -> eDIDIO line bitmask."""
    return 1 << (line - 1)


_SPEKTRA_TARGET = {
    "SEQUENCE": SpektraTargetType.SEQUENCE,
    "THEME": SpektraTargetType.THEME,
    "STATIC": SpektraTargetType.STATIC,
}
_SPEKTRA_ACTION = {
    "START": SpektraActionType.START,
    "STOP": SpektraActionType.STOP,
    "PAUSE": SpektraActionType.PAUSE,
}


class EdidioDispatcher:
    """Queues eDIDIO intents and executes them against one controller."""

    def __init__(self, host: str, port: int = 23, *, use_tls: bool = False,
                 timeout: float = 5.0, client: EdidioClient | None = None,
                 on_state: StateCallback | None = None,
                 on_event: Callable[[dict], Awaitable[None]] | None = None,
                 event_categories=("dali", "inputs", "sensors", "triggers"),
                 groups: dict | None = None, event_stream: EventStream | None = None):
        self._client = client or EdidioClient(host, port, timeout=timeout, use_tls=use_tls)
        self._queue: asyncio.Queue[dict] = asyncio.Queue()
        self._worker: asyncio.Task | None = None
        self._message_id = 0
        self._loop: asyncio.AbstractEventLoop | None = None
        self._on_state = on_state
        self._on_event = on_event
        self._event_categories = tuple(event_categories)
        self.levels = LevelTracker(groups)
        self._events: EventStream | None = None
        if on_state is not None or on_event is not None:
            self._events = event_stream or EventStream(host, port, use_tls)
            self._events.on_event = self._handle_event

    @property
    def events(self) -> EventStream | None:
        return self._events

    async def start(self) -> None:
        """Connect (best-effort), start the worker and, if requested, the event feed."""
        self._loop = asyncio.get_running_loop()
        try:
            await self._client.connect()
        except EDIDIOConnectionError as err:
            # Non-fatal: the client reconnects on the next send.
            _LOGGER.warning("Initial controller connect failed (will retry on demand): %s", err)
        self._worker = asyncio.create_task(self._run(), name="edidio-dispatcher")
        if self._events is not None:
            try:
                await self._events.start(self._event_categories)
                _LOGGER.info("State feedback enabled (event stream subscribed)")
            except EDIDIOConnectionError as err:
                _LOGGER.warning("Event stream unavailable; running command-only: %s", err)

    async def stop(self) -> None:
        if self._events is not None:
            await self._events.stop()
        if self._worker:
            self._worker.cancel()
            try:
                await self._worker
            except asyncio.CancelledError:
                pass
            self._worker = None
        await self._client.disconnect()

    def submit(self, intent: dict) -> None:
        """Enqueue an intent from any thread."""
        if self._loop is None:
            _LOGGER.error("Dispatcher not started; dropping intent %s", intent)
            return
        self._loop.call_soon_threadsafe(self._queue.put_nowait, intent)

    def _next_id(self) -> int:
        self._message_id = (self._message_id + 1) & 0xFFFFFF
        return self._message_id

    async def _handle_event(self, event: dict) -> None:
        if self._on_event is not None:
            await self._on_event(event)
        change = dali_change(event)
        if change is None:
            return
        touched = self.levels.apply(change)
        if touched and self._on_state is not None:   # [] = TX/RX-echo duplicate
            await self._on_state(change, touched)

    async def _run(self) -> None:
        _LOGGER.info("Dispatcher worker started")
        while True:
            intent = await self._queue.get()
            try:
                await self.execute(intent)
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 - one bad command must not kill the worker
                _LOGGER.error("Failed to execute intent %s: %s", intent, err)
            finally:
                self._queue.task_done()

    async def execute(self, intent: dict) -> None:
        """Execute one intent immediately (the worker calls this; so can tests)."""
        kind = intent.get("kind")
        mid = self._next_id()
        c = self._client

        if kind == "dali_level":
            await c.set_dali_arc_level(mid, line_mask(intent["line"]), intent["address"], intent["level"])
        elif kind == "dali_group_level":
            await c.set_dali_group_arc_level(mid, line_mask(intent["line"]), intent["group"], intent["level"])
        elif kind == "dali_scene":
            if intent.get("group") is None:
                await c.recall_dali_scene(mid, line_mask(intent["line"]), intent["scene"])
            else:
                await c.recall_dali_scene_on_group(
                    mid, line_mask(intent["line"]), intent["group"], intent["scene"])
        elif kind == "dali_command":
            await c.send_dali_command(mid, line_mask(intent["line"]), intent["address"], intent["command"])
        elif kind == "spektra":
            # type/action are matched case-insensitively (configs use lowercase).
            await c.send_spektra_control(
                mid, _SPEKTRA_TARGET[str(intent["type"]).upper()], intent["zone"],
                intent["index"], _SPEKTRA_ACTION[str(intent["action"]).upper()])
        elif kind == "spektra_stop":
            await c.send_spektra_stop(mid, intent["zone"])
        elif kind == "dmx_color":
            # One RGB triplet + a repeat count: the controller expands it across the
            # universe instead of us sending an explicit per-channel level list.
            rgb = list(intent["rgb"])
            fixtures = intent.get("fixtures") or (512 // len(rgb))
            frame = EdidioClient.create_dmx_message(mid, 0xFF, line_mask(intent["line"]), 1, fixtures, rgb)
            await c.send_protobuf_message(frame)
        else:
            _LOGGER.error("Unknown intent kind: %s", kind)
            return

        _LOGGER.info("Dispatched %s", intent)

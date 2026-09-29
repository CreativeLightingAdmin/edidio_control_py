"""Turn live DALI bus traffic into light state (0.5.0).

The event stream (:mod:`edidio_control_py.events`) reports every 16-bit DALI
forward frame on the bus — whether it came from this client, a wall panel, a
schedule, SpektraPlus or another master. :func:`dali_change` decodes one of those
frames into a *level change*, and :class:`LevelTracker` folds changes into a
per-target level table, so a gateway can publish **real** device state instead of
echoing the commands it sent.

Targets use the eDIDIO address convention: short address 0-63, group ``64 + g``,
broadcast ``80``. Lines are 1-based (the device reports 0-based).

Requires firmware >= 1.4.0 (Event Stream v2, which carries raw DALI frames).
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from . import DALI_ARC_LEVEL_MAX, DALI_BROADCAST_ADDRESS, DALI_GROUP_ADDRESS_BASE
from .events import DALI_DIR_TX, dali_frame_ok, is_16bit_frame

_META_LINE = 0xFF
_DAPC_MASK = 0xFF          # "no change" arc level
_CMD_OFF = 0x00
_CMD_RECALL_MAX = 0x05
_CMD_RECALL_MIN = 0x06
_CMD_SCENE_BASE = 0x10     # GO TO SCENE 0-15 = 0x10-0x1F


@dataclass(frozen=True)
class DaliChange:
    """One decoded level-affecting DALI frame.

    ``level`` is the new arc level (0-254) when the frame states it; ``None`` when
    the result depends on device config (RECALL MIN, GO TO SCENE). ``scene`` is set
    for scene recalls.
    """

    line: int          # 1-based
    address: int       # eDIDIO address: 0-63 short, 64+g group, 80 broadcast
    level: int | None
    scene: int | None = None
    command: str = "arc"

    @property
    def target(self) -> str:
        if self.address == DALI_BROADCAST_ADDRESS:
            return "broadcast"
        if self.address >= DALI_GROUP_ADDRESS_BASE:
            return "group"
        return "address"


def _edidio_address(addr_byte: int) -> int | None:
    if addr_byte & 0x80 == 0:                    # 0AAAAAAS short address
        return (addr_byte >> 1) & 0x3F
    if addr_byte & 0xE0 == 0x80:                 # 100GGGGS group
        return DALI_GROUP_ADDRESS_BASE + ((addr_byte >> 1) & 0x0F)
    if addr_byte >= 0xFC:                        # broadcast (+ unaddressed)
        return DALI_BROADCAST_ADDRESS
    return None                                  # special commands (101xxxxx, 110xxxxx)


def dali_change(event: dict) -> DaliChange | None:
    """Decode a ``kind == "dali"`` event dict into a :class:`DaliChange`, or None if
    the frame doesn't change light output (queries, config, backward frames…) or
    didn't actually happen (a TX attempt that timed out / collided / had no bus
    power — only SUCCESS transmissions and received frames count)."""
    if event.get("kind") != "dali" or event.get("line") == _META_LINE:
        return None
    direction = event.get("direction", DALI_DIR_TX)
    frame_type = event.get("frame_type", -1)
    if not is_16bit_frame(frame_type, direction):
        return None
    if not dali_frame_ok(direction, frame_type, event.get("status", 0)):
        return None
    frame = event.get("frame", 0)
    addr_byte, data = (frame >> 8) & 0xFF, frame & 0xFF
    address = _edidio_address(addr_byte)
    if address is None:
        return None
    line = int(event.get("line", 0)) + 1

    if addr_byte & 0x01 == 0:                    # S=0: direct arc power
        if data == _DAPC_MASK:
            return None
        return DaliChange(line, address, min(data, DALI_ARC_LEVEL_MAX))
    if data == _CMD_OFF:
        return DaliChange(line, address, 0, command="off")
    if data == _CMD_RECALL_MAX:
        return DaliChange(line, address, DALI_ARC_LEVEL_MAX, command="recall_max")
    if data == _CMD_RECALL_MIN:
        return DaliChange(line, address, None, command="recall_min")
    if _CMD_SCENE_BASE <= data < _CMD_SCENE_BASE + 16:
        return DaliChange(line, address, None, scene=data - _CMD_SCENE_BASE,
                          command="scene")
    return None


class LevelTracker:
    """Folds :class:`DaliChange` values into a ``(line, address) -> level`` table.

    Group membership is not visible on the bus, so pass ``groups`` —
    ``{(line, group): [short addresses]}`` — if you want group frames to update
    member addresses too. Broadcast frames update every known target on the line.

    The controller reports each of its own transmissions twice — the TX SUCCESS and
    the RX echo it hears back on the bus (verified on fw 1.6.2) — so an identical
    change repeated within ``dedupe_window`` seconds is dropped (``apply`` returns
    ``[]``) instead of being published twice.
    """

    def __init__(self, groups: dict | None = None, *, dedupe_window: float = 0.3,
                 clock=time.monotonic):
        self._levels: dict[tuple[int, int], int | None] = {}
        self._groups = {k: list(v) for k, v in (groups or {}).items()}
        self._dedupe_window = dedupe_window
        self._clock = clock
        self._last: tuple[DaliChange, float] | None = None

    def level(self, line: int, address: int) -> int | None:
        return self._levels.get((line, address))

    def snapshot(self) -> dict:
        return dict(self._levels)

    def apply(self, change: DaliChange) -> list[tuple[int, int, int | None]]:
        """Record the change; return every ``(line, address, level)`` it touched
        (``[]`` for a duplicate of the previous change — see class docstring)."""
        now = self._clock()
        if (self._last is not None and self._last[0] == change
                and now - self._last[1] <= self._dedupe_window):
            return []
        self._last = (change, now)
        line, addr = change.line, change.address
        targets = [addr]
        if change.target == "group":
            targets += self._groups.get((line, addr - DALI_GROUP_ADDRESS_BASE), [])
        elif change.target == "broadcast":
            targets += [a for (ln, a) in self._levels if ln == line and a != addr]
        touched = []
        for a in dict.fromkeys(targets):         # de-dupe, keep order
            self._levels[(line, a)] = change.level
            touched.append((line, a, change.level))
        return touched

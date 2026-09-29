"""Tests for 0.5.0: event stream, DALI state decoding, shared gateway dispatcher.
No hardware or network required — fake clients feed frames into the read loop.
"""

import asyncio

import edidio_control_py.eDS10_ProtocolBuffer_pb2 as pb
from edidio_control_py import events
from edidio_control_py._v2 import eDS10_v2_pb2 as pb2
from edidio_control_py.exceptions import EDIDIOCommunicationError, EDIDIOTimeoutError
from edidio_control_py.gateway import EdidioDispatcher, line_mask
from edidio_control_py.state import DaliChange, LevelTracker, dali_change


def _dali_body(frame, line=0, frame_type=1, direction=0, status=0):
    return pb2.EdidioMessage(event_stream=pb2.EventStreamMessage(
        sequence=1, category=3,
        dali=pb2.DaliFrame(line=line, frame_type=frame_type, frame=frame,
                           direction=direction, status=status))).SerializeToString()


def _dali_event(frame, line=0, frame_type=1, direction=0, status=0):
    return events.decode_frame(_dali_body(frame, line, frame_type, direction, status), use_v2=True)


# --- events ------------------------------------------------------------------

def test_categories_mask_always_has_event_bit():
    m = events.categories_to_mask(["dali", "inputs"])
    assert m & (1 << 3) and m & (1 << 2) and m & (1 << 10)
    assert events.categories_to_mask([]) == events.ALL_CATEGORIES_MASK


def test_subscribe_roundtrip():
    m = pb2.EdidioMessage()
    m.ParseFromString(events.build_subscribe(0x7FF, 2)[3:])
    assert m.event_stream.subscribe and m.event_stream.category_mask == 0x7FF


def test_legacy_register_maps_dali_umbrella():
    m = pb.EdidioMessage()
    m.ParseFromString(events.build_legacy_register(1, ["dali", "inputs"])[3:])
    f = m.event.filter
    assert f.dali_arc_level and f.dali_command and f.input and not f.dali_sensor


def test_describe_frames():
    assert events.describe_dali_frame(0xFF05, 1) == "broadcast MAX_LEVEL"
    assert events.describe_dali_frame(0xFF06, 4, direction=1) == "broadcast MIN_LEVEL"  # RX 16-bit
    assert events.describe_dali_frame(0x0AC8, 1) == "addr 5 arc 200"
    assert events.describe_dali_frame(0x8680, 1) == "group 3 arc 128"
    assert events.describe_dali_frame(0xAD00, 1) == "PING 0"        # seen live on fw 1.6.2
    assert events.describe_dali_frame(0xFF05, 4) == "frame 0x00FF05"  # TX type 4 isn't 16-bit


def test_decoded_tx_status():
    ok = _dali_event(0x00C8)
    assert ok["ok"] is True and ok["status_name"] == "SUCCESS"
    timeout = _dali_event(0x00C8, status=242)                  # no bus power, fw 1.6.2
    assert timeout["ok"] is False and timeout["status_name"] == "TIMEOUT"
    assert _dali_event(0x00C8, status=246)["status_name"] == "ATTEMPT"


def test_decode_ack_is_none():
    body = pb2.EdidioMessage(event_stream=pb2.EventStreamMessage(ack=True)).SerializeToString()
    assert events.decode_frame(body, use_v2=True) is None


# --- state -------------------------------------------------------------------

def test_dali_change_short_address_arc():
    c = dali_change(_dali_event(0x0AC8, line=1))
    assert c == DaliChange(line=2, address=5, level=200)
    assert c.target == "address"


def test_dali_change_group_and_broadcast_commands():
    g = dali_change(_dali_event(0x8680))                 # group 3 DAPC 128
    assert (g.address, g.level, g.target) == (67, 128, "group")
    off = dali_change(_dali_event(0xFF00))
    assert (off.address, off.level, off.command, off.target) == (80, 0, "off", "broadcast")
    mx = dali_change(_dali_event(0xFF05, frame_type=4, direction=1))  # RX 16-bit frame
    assert mx.level == 254


def test_dali_change_scene_and_ignored_frames():
    s = dali_change(_dali_event(0x0B13))                 # addr 5 GO TO SCENE 3
    assert s.scene == 3 and s.level is None
    assert dali_change(_dali_event(0x0AFF)) is None      # DAPC MASK: no change
    assert dali_change(_dali_event(0x0B90)) is None      # query, not a level change
    assert dali_change(_dali_event(0x45, frame_type=0)) is None  # 8-bit backward frame
    assert dali_change({"kind": "input"}) is None


def test_live_sequence_success_and_rx_echo_deduplicated():
    # fw 1.6.2 with bus power, "addr 0 arc 200": TX ATTEMPT, TX SUCCESS, RX echo.
    attempt = _dali_event(0x00C8, status=246)
    success = _dali_event(0x00C8)
    echo = _dali_event(0x00C8, frame_type=4, direction=1, status=16)
    assert dali_change(attempt) is None
    assert dali_change(success) == dali_change(echo) == DaliChange(1, 0, 200)
    clock = [100.0]
    t = LevelTracker(clock=lambda: clock[0])
    assert t.apply(dali_change(success)) == [(1, 0, 200)]
    clock[0] += 0.02
    assert t.apply(dali_change(echo)) == []                 # echo of our own TX
    clock[0] += 1.0
    assert t.apply(dali_change(echo)) == [(1, 0, 200)]      # later repeat counts


def test_dali_change_ignores_failed_transmissions():
    # fw 1.6.2 with no DALI bus power: each command arrives as ATTEMPT then TIMEOUT x2.
    for status in (246, 242, 240, 245, 3):
        assert dali_change(_dali_event(0x00C8, status=status)) is None
    assert dali_change(_dali_event(0x00C8)).level == 200         # SUCCESS
    assert dali_change(_dali_event(0x00C8, frame_type=1, direction=1)) is None  # RX type 1 = not 16-bit
    assert dali_change(_dali_event(0x00C8, frame_type=255, direction=1)) is None  # RX error
    assert dali_change(_dali_event(0xAD00)) is None               # PING


def test_level_tracker_group_members_and_broadcast():
    t = LevelTracker(groups={(1, 3): [5, 6]})
    touched = t.apply(DaliChange(1, 67, 100))
    assert touched == [(1, 67, 100), (1, 5, 100), (1, 6, 100)]
    t.apply(DaliChange(1, 80, 0))
    assert t.level(1, 5) == 0 and t.level(1, 67) == 0 and t.level(1, 80) == 0


# --- stream + reconnect --------------------------------------------------------

class FakeClient:
    def __init__(self, items):
        self.items = list(items)
        self.sent = []
        self.connected = False

    async def connect(self):
        self.connected = True

    async def disconnect(self):
        self.connected = False

    async def send_protobuf_message(self, frame):
        self.sent.append(frame)

    async def _receive_framed(self):
        if self.items:
            item = self.items.pop(0)
            if isinstance(item, Exception):
                if isinstance(item, EDIDIOCommunicationError) and not isinstance(item, EDIDIOTimeoutError):
                    self.connected = False
                raise item
            return item
        await asyncio.sleep(3600)


async def _wait(pred):
    for _ in range(200):
        if pred():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("condition not met")


def test_stream_delivers_skips_timeouts_and_resubscribes():
    first = FakeClient([_dali_body(0x0AC8), EDIDIOTimeoutError("idle"),
                        EDIDIOCommunicationError("lost")])
    second = FakeClient([_dali_body(0xFF00)])
    clients = iter([first, second])
    got = []

    async def on_event(ev):
        got.append(ev)

    async def run():
        s = events.EventStream("x", client_factory=lambda: next(clients),
                               on_event=on_event, reconnect_min=0.001)
        await s.start(["dali"])
        await _wait(lambda: len(got) == 2)
        assert s.reconnects == 1
        assert len(second.sent) == 1                     # resubscribed on the new link
        assert [e["seq"] for e in s.recent()] == [1, 2]
        await s.stop()

    asyncio.run(run())


# --- gateway -----------------------------------------------------------------

class RecordingClient:
    def __init__(self):
        self.calls = []

    async def connect(self):
        pass

    async def disconnect(self):
        pass

    def __getattr__(self, name):
        async def record(*args):
            self.calls.append((name, args))
        return record


def test_dispatcher_executes_intent_vocabulary():
    c = RecordingClient()
    d = EdidioDispatcher("x", client=c)

    async def run():
        await d.execute({"kind": "dali_level", "line": 2, "address": 5, "level": 200})
        await d.execute({"kind": "dali_scene", "line": 1, "scene": 3, "group": 4})
        await d.execute({"kind": "spektra", "type": "sequence", "zone": 0,
                         "index": 7, "action": "start"})
        await d.execute({"kind": "dmx_color", "line": 1, "rgb": [255, 0, 0]})

    asyncio.run(run())
    names = [n for n, _ in c.calls]
    assert names == ["set_dali_arc_level", "recall_dali_scene_on_group",
                     "send_spektra_control", "send_protobuf_message"]
    assert c.calls[0][1][1:] == (line_mask(2), 5, 200)


def test_dispatcher_state_feedback():
    changes = []

    async def on_state(change, touched):
        changes.append((change, touched))

    stream_client = FakeClient([_dali_body(0x0AC8)])
    stream = events.EventStream("x", client_factory=lambda: stream_client)
    d = EdidioDispatcher("x", client=RecordingClient(), on_state=on_state, event_stream=stream)

    async def run():
        await d.start()
        await _wait(lambda: changes)
        await d.stop()

    asyncio.run(run())
    change, touched = changes[0]
    assert change.address == 5 and change.level == 200
    assert touched == [(1, 5, 200)] and d.levels.level(1, 5) == 200


def test_connect_timeout_is_edidio_timeout_error(monkeypatch):
    # asyncio.wait_for raises asyncio.TimeoutError, a different class from the
    # builtin TimeoutError before Python 3.11 — seen live as an empty
    # "unexpected error" during a cable-pull reconnect.
    import pytest

    from edidio_control_py import EdidioClient

    async def hang(*args, **kwargs):
        await asyncio.sleep(10)

    monkeypatch.setattr(asyncio, "open_connection", hang)

    async def run():
        with pytest.raises(EDIDIOTimeoutError, match="timed out"):
            await EdidioClient("10.0.0.1", 23, timeout=0.05).connect()

    asyncio.run(run())

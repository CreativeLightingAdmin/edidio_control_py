"""Round-trip tests for the 0.4.0 authoring + read builders.

Each builder frame is decoded back through protobuf to assert the wire content
(fields + 0xCD framing), mirroring tests/test_messages.py.
"""

import edidio_control_py as e
import edidio_control_py.eDS10_ProtocolBuffer_pb2 as pb
from edidio_control_py import EdidioClient


def decode(frame):
    assert frame[0] == 0xCD, "missing 0xCD header"
    length = (frame[1] << 8) | frame[2]
    assert length == len(frame) - 3, "length prefix does not match payload"
    msg = pb.EdidioMessage()
    msg.ParseFromString(frame[3:])
    return msg


# --- time packing (must match firmware bit layout) ---

def test_pack_time():
    # 17:30:00 -> (17<<16)|(30<<8)|0
    assert EdidioClient.pack_time(17, 30, 0) == (17 << 16) | (30 << 8)
    assert EdidioClient.pack_time(23, 59, 59) == (23 << 16) | (59 << 8) | 59


def test_pack_date():
    # year 25, month 12, date 31, weekday 3
    assert EdidioClient.pack_date(25, 12, 31, 3) == (25 << 24) | (12 << 16) | (31 << 8) | 3
    assert EdidioClient.pack_date() == 0


# --- sequence authoring ---

def test_sequence_rotate_rgb():
    frame = EdidioClient.create_spektra_sequence_message(
        1, index=5, seq_type=4,  # ROTATE
        colours=[[255, 0, 0], [255, 255, 0], [0, 255, 0]],
        transition=e.SpektraTransitionType.BLEND,
        time_per_step=500, time_per_step_unit=0, title="RYG Rotate",
    )
    m = decode(frame)
    seq = m.spektra_sequence
    assert seq.index == 5
    assert seq.type == 4
    assert seq.title == "RYG Rotate"
    assert seq.time_per_step == 500
    assert [list(c.channel_value) for c in seq.colours] == [[255, 0, 0], [255, 255, 0], [0, 255, 0]]


def test_sequence_title_truncated():
    frame = EdidioClient.create_spektra_sequence_message(
        1, index=0, seq_type=0, colours=[[0, 0, 0]], title="x" * 40
    )
    assert len(decode(frame).spektra_sequence.title) == 28


# --- theme authoring ---

def test_theme():
    frame = EdidioClient.create_spektra_theme_message(
        2, index=3, colours=[[255, 0, 0], [0, 0, 255]], title="Patriotic"
    )
    theme = decode(frame).spektra_theme
    assert theme.index == 3
    assert theme.title == "Patriotic"
    assert [list(c.channel_value) for c in theme.colours] == [[255, 0, 0], [0, 0, 255]]


# --- alarm / schedule authoring ---

def test_alarm_5pm_daily_start_sequence():
    frame = EdidioClient.create_alarm_message(
        7, index=2,
        enabled=True,
        start_time=EdidioClient.pack_time(17, 0, 0),
        repeat=e.AlarmRepeatType.ALARM_REPEAT_DAILY,
        start_trigger={"type": e.TriggerType.SPEKTRA_START_SEQ, "zone": 1, "target_index": 5},
    )
    alarm = decode(frame).alarm
    assert alarm.index == 2
    assert alarm.enabled is True
    assert alarm.repeat == e.AlarmRepeatType.ALARM_REPEAT_DAILY
    assert alarm.start_time.time == (17 << 16)
    assert alarm.start_trigger.type == e.TriggerType.SPEKTRA_START_SEQ
    assert alarm.start_trigger.zone == 1
    assert alarm.start_trigger.target_index == 5


def test_alarm_sunset():
    frame = EdidioClient.create_alarm_message(
        1, index=0,
        astro_start=e.AlarmAstroType.ALARM_SUNSET,
        start_offset_is_before=True,
        start_trigger={"type": e.TriggerType.SPEKTRA_THEME, "zone": 0, "target_index": 1},
    )
    alarm = decode(frame).alarm
    assert alarm.astro_start == e.AlarmAstroType.ALARM_SUNSET
    assert alarm.start_offset_is_before is True


# --- calendar authoring ---

def test_calendar_assign_days():
    days = [False] * 366
    days[0] = True   # Jan 1
    days[100] = True
    frame = EdidioClient.create_spektra_calendar_message(
        3, e.SpektraTargetType.SEQUENCE, index=5, days=days, is_override=True)
    cal = decode(frame).spektra_calendar
    assert cal.type == e.SpektraTargetType.SEQUENCE
    assert cal.index == 5
    assert cal.isOverride is True
    assert list(cal.days)[0] is True
    assert list(cal.days)[100] is True
    assert list(cal.days)[1] is False


# --- read / query builders ---

def test_spektra_read():
    frame = EdidioClient.create_spektra_read_message(1, e.SpektraTargetType.SETTINGS, 3)
    read = decode(frame).spektra_read
    assert read.type == e.SpektraTargetType.SETTINGS
    assert read.index == 3


def test_read_device_alarms():
    frame = EdidioClient.create_read_device_message(1, e.ReadType.ALARMS, index=0)
    rd = decode(frame).read_device
    assert rd.type == e.ReadType.ALARMS


def test_diagnostic_system_info():
    frame = EdidioClient.create_diagnostic_message(1, e.DiagnosticMessageType.DIAGNOSTIC_SYSTEM_INFO)
    diag = decode(frame).diag_message
    assert diag.type == e.DiagnosticMessageType.DIAGNOSTIC_SYSTEM_INFO


def test_diagnostic_dmx_cache():
    frame = EdidioClient.create_diagnostic_message(
        1, e.DiagnosticMessageType.DMX_LEVEL_CACHE, line=1, page=0
    )
    diag = decode(frame).diag_message
    assert diag.type == e.DiagnosticMessageType.DMX_LEVEL_CACHE
    assert diag.line == 1


# --- enum re-exports present ---

def test_authoring_enums_exported():
    assert e.SpektraTransitionType.SNAP == pb.SpektraTransitionType.SNAP
    assert e.AlarmRepeatType.ALARM_REPEAT_DAILY == pb.AlarmRepeatType.ALARM_REPEAT_DAILY
    assert e.TriggerType.SPEKTRA_START_SEQ == pb.TriggerType.SPEKTRA_START_SEQ
    assert e.AckMessageType.SUCCESS == 5

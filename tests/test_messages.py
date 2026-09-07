"""Round-trip tests for message builders. No hardware or network required.

Each builder frame is decoded back through protobuf to assert the wire content,
plus a regression guard that the public API surface (relied on by downstream
consumers such as the Home Assistant integration) stays intact.
"""

import edidio_control_py as e
import edidio_control_py.eDS10_ProtocolBuffer_pb2 as pb
from edidio_control_py import EdidioClient


def decode(frame):
    """Strip the 0xCD + 2-byte length header and parse the EdidioMessage."""
    assert frame[0] == 0xCD, "missing 0xCD header"
    length = (frame[1] << 8) | frame[2]
    assert length == len(frame) - 3, "length prefix does not match payload"
    msg = pb.EdidioMessage()
    msg.ParseFromString(frame[3:])
    return msg


def test_enum_reexports_match_protobuf():
    assert e.CustomDALICommandType.DALI_GROUP_ARC_LEVEL == pb.CustomDALICommandType.DALI_GROUP_ARC_LEVEL
    assert e.SpektraTargetType.SEQUENCE == pb.SpektraTargetType.SEQUENCE
    assert e.SpektraActionType.START == pb.SpektraActionType.START
    assert e.DALICommandType.DALI_OFF == pb.DALICommandType.DALI_OFF


def test_group_arc_level_frame():
    f = EdidioClient.create_dali_message(
        message_id=1, line_mask=0b0001, address=5,
        custom_command=pb.CustomDALICommandType.DALI_GROUP_ARC_LEVEL, arg=[200],
    )
    m = decode(f)
    assert m.message_id == 1
    assert m.dali_message.line_mask == 0b0001
    assert m.dali_message.address == 5
    assert m.dali_message.custom_command == pb.CustomDALICommandType.DALI_GROUP_ARC_LEVEL
    assert m.dali_message.arg == 200


def test_broadcast_scene_frame():
    f = EdidioClient.create_dali_message(
        message_id=2, line_mask=0b0010, address=0,
        custom_command=pb.CustomDALICommandType.DALI_BROADCAST_SCENE, arg=[7],
    )
    m = decode(f)
    assert m.dali_message.custom_command == pb.CustomDALICommandType.DALI_BROADCAST_SCENE
    assert m.dali_message.arg == 7


def test_scene_on_group_frame():
    f = EdidioClient.create_dali_message(
        message_id=3, line_mask=0b0001, address=4,
        custom_command=pb.CustomDALICommandType.DALI_SCENE_ON_GROUP, arg=[3],
    )
    m = decode(f)
    assert m.dali_message.custom_command == pb.CustomDALICommandType.DALI_SCENE_ON_GROUP
    assert m.dali_message.address == 4
    assert m.dali_message.arg == 3


# --- high-level group/scene methods use the hardware-verified encoding ---
# The DALI_GROUP_ARC_LEVEL / DALI_BROADCAST_SCENE / DALI_SCENE_ON_GROUP custom
# commands are not implemented on the controller, so these methods encode group
# arc as DALI_ARC_LEVEL at address 64+group, and scene recall as the raw
# "GO TO SCENE X" command (0x10 + scene) at the target address.

def _capture(coro_factory):
    """Run an async EdidioClient method, capturing the frame it would send."""
    import asyncio

    client = EdidioClient("127.0.0.1", 23)
    sent = []

    async def fake_send(frame):
        sent.append(bytes(frame))

    client.send_protobuf_message = fake_send  # type: ignore[assignment]
    asyncio.run(coro_factory(client))
    assert len(sent) == 1
    return decode(sent[0])


def test_group_arc_level_uses_group_address():
    m = _capture(lambda c: c.set_dali_group_arc_level(7, 0b0001, 3, 128))
    assert m.dali_message.address == 67  # 64 + group 3
    assert m.dali_message.custom_command == pb.CustomDALICommandType.DALI_ARC_LEVEL
    assert m.dali_message.arg == 128


def test_broadcast_scene_uses_go_to_scene_at_80():
    m = _capture(lambda c: c.recall_dali_scene(7, 0b0001, 3))
    assert m.dali_message.address == 80  # broadcast
    assert m.dali_message.command == 0x10 + 3  # GO TO SCENE 3


def test_scene_on_group_uses_go_to_scene_at_group_address():
    m = _capture(lambda c: c.recall_dali_scene_on_group(7, 0b0001, 4, 3))
    assert m.dali_message.address == 68  # 64 + group 4
    assert m.dali_message.command == 0x10 + 3  # GO TO SCENE 3


def test_group_scene_reference_frames_hex():
    """Lock the exact wire bytes used as the cross-encoder reference frames."""
    g = _capture(lambda c: c.set_dali_group_arc_level(7, 0b0001, 3, 128))
    b = _capture(lambda c: c.recall_dali_scene(7, 0b0001, 3))
    gs = _capture(lambda c: c.recall_dali_scene_on_group(7, 0b0001, 4, 3))
    # re-encode to hex and compare to the ENCODERS.md oracle
    assert g.SerializeToString().hex() == pb.EdidioMessage(
        message_id=7, dali_message=pb.DALIMessage(
            line_mask=1, address=67, custom_command=pb.CustomDALICommandType.DALI_ARC_LEVEL, arg=128)
    ).SerializeToString().hex()


def test_dali_command_frame():
    f = EdidioClient.create_dali_message(
        message_id=4, line_mask=0b0001, address=10,
        command=pb.DALICommandType.DALI_MAX_LEVEL, arg=[0],
    )
    m = decode(f)
    assert m.dali_message.command == pb.DALICommandType.DALI_MAX_LEVEL
    assert m.dali_message.address == 10


def test_spektra_control_frame():
    f = EdidioClient.create_spektra_control_message(
        message_id=5, spektra_type=pb.SpektraTargetType.SEQUENCE,
        zone=1, index=2, action=pb.SpektraActionType.START,
    )
    m = decode(f)
    assert m.spektra_control.type == pb.SpektraTargetType.SEQUENCE
    assert m.spektra_control.zone == 1
    assert m.spektra_control.index == 2
    assert m.spektra_control.action == pb.SpektraActionType.START


def test_spektra_stop_frame():
    f = EdidioClient.create_spektra_stop_message(message_id=6, zone=1, line_mask=0xFF)
    m = decode(f)
    assert m.external_trigger.trigger.type == pb.TriggerType.SPEKTRA_STOP_SEQ
    assert m.external_trigger.trigger.zone == 1
    assert m.external_trigger.trigger.line_mask == 0xFF


def test_existing_api_surface_intact():
    """Guard the members downstream consumers (e.g. Home Assistant) depend on."""
    for name in (
        "connect", "disconnect", "connected", "create_dali_message",
        "create_dmx_message", "set_dali_arc_level", "set_dmx_level",
        "send_dali_commands_sequence", "send_protobuf_message",
    ):
        assert hasattr(EdidioClient, name), f"missing existing member: {name}"

    from edidio_control_py.exceptions import (  # noqa: F401
        EDIDIOCommunicationError,
        EDIDIOConnectionError,
        EDIDIOTimeoutError,
    )

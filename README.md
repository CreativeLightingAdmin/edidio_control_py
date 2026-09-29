# eDIDIO Control Python Library

A Python library for communicating with and controlling the Control Freak eDIDIO S10 lighting controller.

## Features

- Asynchronous TCP and TLS communication with eDIDIO controllers
- Support for DMX and DALI (including DALI DT8 CCT) message creation
- High-level helpers for DALI group levels, standard commands, and scene recall
- SpektraPlus control (start/stop/pause sequences, themes, and static scenes)
- **Authoring (0.4.0):** create/save SpektraPlus sequences and themes, and
  schedules/alarms (`create_spektra_sequence_message`,
  `create_spektra_theme_message`, `create_alarm_message`), plus read/query
  builders (`create_spektra_read_message`, `create_read_device_message`,
  `create_diagnostic_message`) and a `request()` helper that decodes the reply
- **Live events + state (0.5.0):** subscribe to the controller's pushed event
  stream (`edidio_control_py.events`), turn DALI bus traffic into light levels
  (`edidio_control_py.state`), and a shared thread-safe gateway dispatcher with an
  optional state feed (`edidio_control_py.gateway`)
- Connection management including keep-alive and auto-reconnect
- Async context manager support

## Installation

```bash
pip install edidio_control_py
```

## Quick Start

```python
import asyncio
from edidio_control_py import EdidioClient

async def main():
    async with EdidioClient("192.168.1.10", 23) as client:
        # Set a DALI device to 50% brightness (arc level 127)
        await client.set_dali_arc_level(
            message_id=1, line_mask=1, address=0, arc_level=127
        )

asyncio.run(main())
```

## Connecting

### Plain TCP (port 23)

```python
client = EdidioClient("192.168.1.10", 23)
await client.connect()
```

### TLS (port 443)

```python
client = EdidioClient("192.168.1.10", 443, use_tls=True)
await client.connect()
```

By default, TLS uses the system CA bundle for certificate verification. If your device uses a self-signed certificate, pass a custom SSL context:

```python
import ssl

ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

client = EdidioClient("192.168.1.10", 443, use_tls=True, ssl_context=ctx)
await client.connect()
```

### Async Context Manager

The client supports `async with`, which automatically connects and disconnects:

```python
async with EdidioClient("192.168.1.10", 443, use_tls=True) as client:
    await client.set_dali_arc_level(message_id=1, line_mask=1, address=0, arc_level=254)
```

## API Reference

### `EdidioClient(host, port, timeout=5.0, *, use_tls=False, ssl_context=None)`

| Parameter | Type | Description |
|-----------|------|-------------|
| `host` | `str` | IP address or hostname of the eDIDIO device |
| `port` | `int` | Port number — typically `23` (TCP) or `443` (TLS) |
| `timeout` | `float` | Timeout for network operations in seconds (default: `5.0`) |
| `use_tls` | `bool` | Enable TLS (default: `False`) |
| `ssl_context` | `ssl.SSLContext \| None` | Custom SSL context for TLS connections |

### Methods

#### `await client.connect()`
Establishes a TCP/TLS connection and starts the keep-alive task.

#### `await client.disconnect()`
Closes the connection and cancels the keep-alive task.

#### `await client.set_dali_arc_level(message_id, line_mask, address, arc_level)`
Sets a DALI device brightness. `arc_level` is clamped to `0–254`.

#### `await client.set_dmx_level(message_id, zone, universe_mask, channel, level, fade_time_by_10ms=0)`
Sends a DMX level command. `level` is a list of channel values.

#### `await client.set_dali_group_arc_level(message_id, line_mask, group, arc_level)`
Sets a whole DALI group's brightness. `arc_level` is clamped to `0–254`.

#### `await client.send_dali_command(message_id, line_mask, address, command, arg=0)`
Sends a standard DALI command to an address. `command` is a `DALICommandType` value (e.g. `DALICommandType.DALI_OFF`, `DALI_MAX_LEVEL`, `DALI_FADE_UP`); `arg` carries a command argument where applicable.

#### `await client.recall_dali_scene(message_id, line_mask, scene)`
Recalls a stored DALI scene across all fittings on the given line(s).

#### `await client.recall_dali_scene_on_group(message_id, line_mask, group, scene)`
Recalls a stored DALI scene on a specific group.

#### `await client.send_spektra_control(message_id, spektra_type, zone, index, action)`
Starts/stops/pauses a SpektraPlus sequence, theme, or static scene on a zone. `spektra_type` is a `SpektraTargetType` value (`SEQUENCE`, `THEME`, `STATIC`); `action` is a `SpektraActionType` value (`START`, `STOP`, `PAUSE`).

#### `await client.send_spektra_stop(message_id, zone, line_mask=0xFF)`
Stops SpektraPlus playback on a zone **and** turns the output off (matching how the SpektraPlus app stops playback).

#### `await client.send_dali_commands_sequence(commands)`
Sends a list of pre-built DALI protobuf byte messages with a 50ms gap between each.

#### `await client.send_protobuf_message(message)`
Sends a raw framed protobuf message (prefixed with `0xCD` + 2-byte length).

#### `await client.receive_protobuf_response()`
Reads and returns the next protobuf payload from the device, stripping the frame header.

#### `EdidioClient.create_dali_message(message_id, line_mask, address, *, ...)`
Static method. Builds and frames a DALI protobuf message. Exactly one action field must be provided (e.g. `command`, `custom_command`, `query`, `type8`, etc.).

#### `EdidioClient.create_dmx_message(message_id, zone, universe_mask, channel, repeat, level, fade_time_by_10ms=0)`
Static method. Builds and frames a DMX protobuf message.

#### `EdidioClient.create_spektra_control_message(message_id, spektra_type, zone, index, action)`
Static method. Builds and frames a SpektraPlus control message.

#### `EdidioClient.create_spektra_stop_message(message_id, zone, line_mask=0xFF)`
Static method. Builds and frames a SpektraPlus stop external-trigger message.

### Enums

For convenience the protocol enums are re-exported from the package, so you don't need to reach into the generated protobuf module:

```python
from edidio_control_py import (
    DALICommandType,
    CustomDALICommandType,
    SpektraTargetType,
    SpektraActionType,
)

await client.send_dali_command(1, line_mask=1, address=0, command=DALICommandType.DALI_MAX_LEVEL)
await client.send_spektra_control(2, SpektraTargetType.SEQUENCE, zone=1, index=0, action=SpektraActionType.START)
```

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `client.connected` | `bool` | `True` if currently connected |
| `client.host` | `str` | Host address |
| `client.port` | `int` | Port number |
| `client.use_tls` | `bool` | `True` if TLS is enabled |

## Live events & state (0.5.0)

The controller can **push** events — every DALI frame on the bus, input presses,
sensor changes, SpektraPlus playback, commands — to a subscribed client. This is
how integrations report the *real* light state, including changes made by wall
panels, schedules, SpektraPlus or other apps.

Event Stream v2 needs **firmware ≥ 1.4.0**; older firmware can use the legacy
`EventMessage` path (`EventStream(..., use_v2=False)`, no DALI-level decoding).

### `EventStream` — a dedicated push connection

```python
from edidio_control_py.events import EventStream

async def on_event(ev: dict):
    print(ev["kind"], ev)          # dali / input / sensor / spektra / command / ...

stream = EventStream("192.168.1.50", on_event=on_event)
await stream.start(["dali", "inputs", "sensors"])
...
stream.recent(since_seq=0)          # rolling buffer, for pollers
await stream.stop()
```

It uses its own connection (never interferes with request/response), skips idle
timeouts, and on connection loss **reconnects with backoff and resubscribes**
(`stream.reconnects`, `stream.connected`).

### `state` — DALI frames → levels

```python
from edidio_control_py.state import LevelTracker, dali_change

tracker = LevelTracker(groups={(1, 3): [5, 6]})   # optional group membership
change = dali_change(ev)          # DaliChange(line, address, level, scene, command) or None
if change:
    touched = tracker.apply(change)   # [(line, address, level), ...]
```

Addresses use the eDIDIO convention (0–63 short, `64 + g` group, `80` broadcast);
lines are 1-based. `level` is `None` when the frame doesn't state one (RECALL MIN,
GO TO SCENE). Broadcast frames update every known target on the line.

Only frames that really happened count: a TX frame (direction 0) must have status
`SUCCESS` (0) — ATTEMPT/TIMEOUT/COLLISION etc. are ignored, so a command sent with
no bus power never shows as a level — and RX frames (direction 1, which use their
own `frame_type` numbering: 4 = 16-bit) must be successfully received. The
controller reports its own transmissions twice (TX SUCCESS + RX echo);
`LevelTracker` drops the repeat. Decoded DALI events carry `ok` and, for TX,
`status_name`. Verified on firmware 1.6.2.

### `gateway.EdidioDispatcher` — shared by every Python bridge

```python
from edidio_control_py.gateway import EdidioDispatcher

async def on_state(change, touched):
    for line, address, level in touched:
        publish(line, address, level)

d = EdidioDispatcher("192.168.1.50", on_state=on_state)
await d.start()
d.submit({"kind": "dali_level", "line": 1, "address": 5, "level": 200})  # any thread
```

Intents: `dali_level`, `dali_group_level`, `dali_scene`, `dali_command`,
`spektra`, `spektra_stop`, `dmx_color` (see the module docstring). With
`on_state`/`on_event` it also runs an `EventStream` and keeps `d.levels` current;
if the stream can't start, commands still work.

## Exceptions

All exceptions are defined in `edidio_control_py.exceptions`.

| Exception | Description |
|-----------|-------------|
| `EDIDIOConnectionError` | Base exception for connection failures |
| `EDIDIOCommunicationError` | Error during send/receive (subclass of `EDIDIOConnectionError`) |
| `EDIDIOTimeoutError` | Operation timed out (subclass of `EDIDIOCommunicationError`) |
| `EDIDIOInvalidMessageError` | Received a malformed message (subclass of `EDIDIOCommunicationError`) |

```python
from edidio_control_py.exceptions import EDIDIOConnectionError, EDIDIOTimeoutError

try:
    await client.connect()
except EDIDIOTimeoutError:
    print("Connection timed out")
except EDIDIOConnectionError as e:
    print(f"Connection failed: {e}")
```

## Requirements

- Python >= 3.9
- `protobuf >= 5.29.2` (the generated modules check the runtime version at import)

## License

MIT

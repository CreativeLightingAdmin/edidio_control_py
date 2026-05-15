# eDIDIO Control Python Library

A Python library for communicating with and controlling the Control Freak eDIDIO S10 lighting controller.

## Features

- Asynchronous TCP and TLS communication with eDIDIO controllers
- Support for DMX and DALI (including DALI DT8 CCT) message creation
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

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `client.connected` | `bool` | `True` if currently connected |
| `client.host` | `str` | Host address |
| `client.port` | `int` | Port number |
| `client.use_tls` | `bool` | `True` if TLS is enabled |

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
- `protobuf >= 3.0`

## License

MIT

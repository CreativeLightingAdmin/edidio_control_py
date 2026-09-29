"""Namespaced protobuf module for the current eDIDIO proto (package ``edidiov2``).

Compiled separately from the engine's ``eDS10_ProtocolBuffer_pb2`` so its
descriptors don't collide in the shared protobuf pool. Regenerate with::

    protoc --python_out=edidio_control_py/_v2 --proto_path=edidio_control_py/_v2 eDS10_v2.proto
"""

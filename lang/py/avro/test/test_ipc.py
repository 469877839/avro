#!/usr/bin/env python3

##
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Dual-end error contract tests for Avro IPC.

The responder and requestor each use their own protocol declaration, so the
handshake exchanges the server protocol and every call response is resolved
from the remote (writer) error union into the local (reader) error union.
For each scenario the tests assert both the exact wire payload (the union
branch index and its body, located after the response metadata and the error
flag) and the result the caller observes. A declared error, a system string
error and a decode failure therefore stay distinguishable instead of being
collapsed into one generic error.
"""

import io
import json
import unittest

import avro.errors
import avro.io
import avro.ipc
import avro.protocol

# The responder protocol declares a BizError record. Each message exercises
# one error-contract scenario.
SERVER_PROTOCOL_JSON = json.dumps(
    {
        "protocol": "ErrContractServer",
        "namespace": "ex.contract",
        "types": [
            {
                "name": "BizError",
                "type": "error",
                "fields": [
                    {"name": "code", "type": "int"},
                    {"name": "message", "type": "string"},
                ],
            }
        ],
        "messages": {
            "echo": {"request": [{"name": "x", "type": "string"}], "response": "string"},
            "declared_error": {"request": [], "response": "null", "errors": ["BizError"]},
            "undeclared_error": {"request": [], "response": "null"},
            "bad_error_object": {"request": [], "response": "null", "errors": ["BizError"]},
            "bad_response": {"request": [{"name": "x", "type": "int"}], "response": "string"},
            "empty_errors": {"request": [], "response": "null"},
        },
    }
)


def _client_protocol_json(name, types, drop_declared_errors=False):
    """Build a client protocol with the same messages but a chosen error view."""
    messages = {
        "echo": {"request": [{"name": "x", "type": "string"}], "response": "string"},
        "declared_error": {"request": [], "response": "null", "errors": ["BizError"]},
        "undeclared_error": {"request": [], "response": "null"},
        "bad_error_object": {"request": [], "response": "null", "errors": ["BizError"]},
        "bad_response": {"request": [{"name": "x", "type": "int"}], "response": "string"},
        "empty_errors": {"request": [], "response": "null"},
    }
    if drop_declared_errors:
        for body in messages.values():
            body.pop("errors", None)
    return json.dumps({"protocol": name, "namespace": "ex.contract", "types": types, "messages": messages})


BIZ_ERROR_TYPE = [
    {
        "name": "BizError",
        "type": "error",
        "fields": [
            {"name": "code", "type": "int"},
            {"name": "message", "type": "string"},
        ],
    }
]

# The local error union is identical to the remote one.
FULL_CLIENT_PROTOCOL_JSON = _client_protocol_json("ErrContractClientFull", BIZ_ERROR_TYPE)

# The local protocol declares no error branch, so the effective reader union
# for "declared_error" is only ["string"].
NARROW_CLIENT_PROTOCOL_JSON = _client_protocol_json("ErrContractClientNarrow", [], drop_declared_errors=True)

# The local protocol has a same-named error record with fewer fields.
DRIFTED_CLIENT_PROTOCOL_JSON = _client_protocol_json(
    "ErrContractClientDrifted",
    [{"name": "BizError", "type": "error", "fields": [{"name": "code", "type": "int"}]}],
)


class InMemoryTransceiver:
    """A transceiver that sends calls straight to a responder in-process."""

    def __init__(self, responder):
        self.responder = responder
        self.remote_name = ("in-memory", id(responder))

    def transceive(self, request):
        return self.responder.respond(request)


class ContractResponder(avro.ipc.Responder):
    """A responder whose behavior is supplied by each test."""

    def __init__(self, protocol, behavior):
        super().__init__(protocol)
        self.behavior = behavior
        self.invocations = []

    def invoke(self, message, request):
        self.invocations.append(message.name)
        return self.behavior(message.name, request)


def default_behavior(message_name, request):
    if message_name == "echo":
        return f"ok:{request['x']}"
    if message_name == "declared_error":
        raise avro.errors.AvroRemoteException({"code": 7, "message": "declared"})
    if message_name == "undeclared_error":
        raise RuntimeError("undeclared boom")
    if message_name == "bad_error_object":
        # AvroRemoteException carrying a value that fits no declared branch.
        raise avro.errors.AvroRemoteException(42)
    if message_name == "bad_response":
        # A response datum that does not match the declared response schema.
        return 123
    if message_name == "empty_errors":
        raise avro.errors.AvroRemoteException("plain system string")
    raise AssertionError(f"unknown message: {message_name}")


class WireView:
    """The decoded envelope of a call response plus its raw error payload."""

    def __init__(self, match, metadata, error_flag, handshake_end, payload_start, payload):
        self.match = match
        self.metadata = metadata
        self.error_flag = error_flag
        self.handshake_end = handshake_end
        self.payload_start = payload_start
        self.payload = payload


def encode_call_request(requestor, message_name, request_datum=None):
    buffer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(buffer)
    requestor.write_handshake_request(encoder)
    requestor.write_call_request(message_name, request_datum or {}, encoder)
    return buffer.getvalue()


def split_response(response):
    """Decode handshake, metadata and error flag, returning the raw payload.

    Layout: handshake response, response metadata (an empty map here), the
    one-byte error flag, then the response or error body.
    """
    decoder = avro.io.BinaryDecoder(io.BytesIO(response))
    handshake = avro.ipc.HANDSHAKE_REQUESTOR_READER.read(decoder)
    handshake_end = decoder.reader.tell()
    metadata = avro.ipc.META_READER.read(decoder)
    error_flag = decoder.read_boolean()
    payload_start = decoder.reader.tell()
    return WireView(handshake.get("match"), metadata, error_flag, handshake_end, payload_start, response[payload_start:])


def read_call_response(requestor, response, message_name):
    decoder = avro.io.BinaryDecoder(io.BytesIO(response))
    # A BOTH handshake does not re-send the protocol, mirror the state that
    # write_handshake_request would have established before the round trip.
    requestor.remote_protocol = requestor.local_protocol
    requestor.read_handshake_response(decoder)
    return requestor.read_call_response(message_name, decoder)


class TestIPC(unittest.TestCase):
    def test_server_with_path(self):
        client_with_custom_path = avro.ipc.HTTPTransceiver("apache.org", 80, "/service/article")
        self.assertEqual("/service/article", client_with_custom_path.req_resource)

        client_with_default_path = avro.ipc.HTTPTransceiver("apache.org", 80)
        self.assertEqual("/", client_with_default_path.req_resource)


class TestIPCErrorContract(unittest.TestCase):
    """Assert the wire contract and the caller-visible outcome per scenario."""

    def setUp(self):
        self.server_protocol = avro.protocol.parse(SERVER_PROTOCOL_JSON)
        self.responder = ContractResponder(self.server_protocol, default_behavior)
        self.transceiver = InMemoryTransceiver(self.responder)

    def requestor(self, protocol_json):
        return avro.ipc.Requestor(avro.protocol.parse(protocol_json), self.transceiver)

    def raw_response(self, message_name, request_datum=None):
        """Ask the responder directly and return its raw response bytes."""
        # Use the server protocol on both ends so the handshake resolves in one
        # round trip; the wire bytes are produced by the responder regardless.
        requestor = self.requestor(SERVER_PROTOCOL_JSON)
        return self.responder.respond(encode_call_request(requestor, message_name, request_datum))

    def assert_response_shape(self, wire):
        self.assertEqual("BOTH", wire.match)
        self.assertEqual({}, wire.metadata)
        self.assertEqual(4, wire.handshake_end)

    # --- success and declared error ------------------------------------

    def test_success_response_wire_and_value(self):
        wire = split_response(self.raw_response("echo", {"x": "hi"}))
        self.assert_response_shape(wire)
        self.assertFalse(wire.error_flag)
        # UTF-8 "ok:hi" (5 bytes) with its length prefix on the response branch.
        self.assertEqual("0a6f6b3a6869", wire.payload.hex())
        requestor = self.requestor(FULL_CLIENT_PROTOCOL_JSON)
        self.assertEqual("ok:hi", requestor.request("echo", {"x": "hi"}))

    def test_declared_error_is_written_as_declared_branch(self):
        wire = split_response(self.raw_response("declared_error"))
        self.assert_response_shape(wire)
        self.assertTrue(wire.error_flag)
        # Branch index 1 (the BizError record) followed by code=7, message="declared".
        self.assertEqual("020e106465636c61726564", wire.payload.hex())

    def test_declared_error_reaches_caller_as_record_value(self):
        requestor = self.requestor(FULL_CLIENT_PROTOCOL_JSON)
        with self.assertRaises(avro.errors.AvroRemoteException) as raised:
            requestor.request("declared_error", {})
        self.assertEqual(({"code": 7, "message": "declared"},), raised.exception.args)

    def test_declared_error_with_drifted_reader_resolves_fields(self):
        requestor = self.requestor(DRIFTED_CLIENT_PROTOCOL_JSON)
        with self.assertRaises(avro.errors.AvroRemoteException) as raised:
            requestor.request("declared_error", {})
        # The reader's record only declares "code"; "message" is skipped.
        self.assertEqual(({"code": 7},), raised.exception.args)

    def test_declared_error_with_missing_reader_branch_is_a_decode_failure(self):
        requestor = self.requestor(NARROW_CLIENT_PROTOCOL_JSON)
        with self.assertRaises(avro.errors.SchemaResolutionException):
            requestor.request("declared_error", {})
        # The remote branch was genuinely on the wire even though the local
        # protocol cannot read it.
        self.assertEqual("020e106465636c61726564", split_response(self.raw_response("declared_error")).payload.hex())

    # --- system string errors ------------------------------------------

    def test_undeclared_exception_becomes_system_string_error(self):
        wire = split_response(self.raw_response("undeclared_error"))
        self.assert_response_shape(wire)
        self.assertTrue(wire.error_flag)
        # Branch index 0 (string) followed by "undeclared boom".
        self.assertEqual("001e756e6465636c6172656420626f6f6d", wire.payload.hex())

    def test_undeclared_exception_reaches_caller_as_string(self):
        requestor = self.requestor(FULL_CLIENT_PROTOCOL_JSON)
        with self.assertRaises(avro.errors.AvroRemoteException) as raised:
            requestor.request("undeclared_error", {})
        self.assertEqual(("undeclared boom",), raised.exception.args)

    def test_error_object_outside_schema_becomes_system_string_error(self):
        wire = split_response(self.raw_response("bad_error_object"))
        self.assert_response_shape(wire)
        self.assertTrue(wire.error_flag)
        # 42 does not fit the BizError branch, so it falls back to branch 0
        # carrying the string form of the exception.
        self.assertEqual("00043432", wire.payload.hex())

    def test_empty_error_declaration_has_effective_string_union(self):
        wire = split_response(self.raw_response("empty_errors"))
        self.assert_response_shape(wire)
        self.assertTrue(wire.error_flag)
        # No declared errors: the effective union is ["string"], so the only
        # branch (index 0) carries the message.
        self.assertEqual("0026706c61696e2073797374656d20737472696e67", wire.payload.hex())
        requestor = self.requestor(FULL_CLIENT_PROTOCOL_JSON)
        with self.assertRaises(avro.errors.AvroRemoteException) as raised:
            requestor.request("empty_errors", {})
        self.assertEqual(("plain system string",), raised.exception.args)

    # --- failures while producing the response -------------------------

    def test_bad_response_is_reported_as_system_error_after_handshake(self):
        wire = split_response(self.raw_response("bad_response", {"x": 1}))
        self.assert_response_shape(wire)
        self.assertTrue(wire.error_flag)
        self.assertTrue(wire.payload.startswith(b"\x00"), "payload must start with the string branch index")

        requestor = self.requestor(FULL_CLIENT_PROTOCOL_JSON)
        with self.assertRaises(avro.errors.AvroRemoteException) as raised:
            requestor.request("bad_response", {"x": 1})
        self.assertIn("is not an example of the schema", raised.exception.args[0])

    # --- tampered remote error payloads --------------------------------

    def test_truncated_success_response_is_a_decode_failure_not_remote_error(self):
        # A well-formed envelope with an error flag of false, but a string body
        # whose declared length runs past the end of the message. The caller
        # must see a decoding exception rather than a remote error.
        response = bytearray(self.raw_response("echo", {"x": "hi"}))
        payload_start = split_response(bytes(response)).payload_start
        response[payload_start:] = b"\x0a\x6f"  # claims 5 bytes, only one left
        requestor = self.requestor(FULL_CLIENT_PROTOCOL_JSON)
        with self.assertRaises(avro.errors.InvalidAvroBinaryEncoding):
            read_call_response(requestor, bytes(response), "echo")

    def test_tampered_error_branch_index_is_a_decode_failure(self):
        response = bytearray(self.raw_response("declared_error"))
        payload_start = split_response(bytes(response)).payload_start
        response[payload_start:] = b"\xc6\x01"  # zig-zag long 99: no such union branch
        requestor = self.requestor(FULL_CLIENT_PROTOCOL_JSON)
        with self.assertRaises(avro.errors.SchemaResolutionException):
            read_call_response(requestor, bytes(response), "declared_error")

    def test_truncated_error_body_is_a_decode_failure(self):
        response = bytearray(self.raw_response("declared_error"))
        payload_start = split_response(bytes(response)).payload_start
        response[payload_start:] = b"\x02"  # record branch with no record bytes
        requestor = self.requestor(FULL_CLIENT_PROTOCOL_JSON)
        with self.assertRaises(avro.errors.InvalidAvroBinaryEncoding):
            read_call_response(requestor, bytes(response), "declared_error")

    # --- the connection stays usable after every outcome ---------------

    def test_caller_results_and_subsequent_calls(self):
        requestor = self.requestor(FULL_CLIENT_PROTOCOL_JSON)
        self.assertEqual("ok:first", requestor.request("echo", {"x": "first"}))

        with self.assertRaises(avro.errors.AvroRemoteException) as declared:
            requestor.request("declared_error", {})
        self.assertEqual({"code": 7, "message": "declared"}, declared.exception.args[0])

        with self.assertRaises(avro.errors.AvroRemoteException) as system:
            requestor.request("undeclared_error", {})
        self.assertEqual("undeclared boom", system.exception.args[0])

        with self.assertRaises(avro.errors.AvroRemoteException):
            requestor.request("bad_response", {"x": 1})

        self.assertEqual("ok:last", requestor.request("echo", {"x": "last"}))
        # Only the three successful or handler-level calls reached invoke; the
        # bad_response failure happened while serializing the result.
        self.assertEqual(["echo", "declared_error", "undeclared_error", "bad_response", "echo"], self.responder.invocations)


if __name__ == "__main__":  # pragma: no coverage
    unittest.main()

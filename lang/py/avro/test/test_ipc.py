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

"""Tests for the hardened framed IPC transport and real IPC round trips."""

import io
import json
import unittest

import avro.errors
import avro.ipc
import avro.protocol

BUFFER_SIZE = avro.ipc.BUFFER_SIZE
HEADER = avro.ipc.BIG_ENDIAN_INT_STRUCT


def frame(payload):
    """Encode one frame: a big-endian length followed by its payload."""
    return HEADER.pack(len(payload)) + payload


def framed(*payloads):
    """Encode a message: frames followed by the zero-length terminator."""
    return b"".join(frame(p) for p in payloads) + HEADER.pack(0)


class ShortReadChannel:
    """A read channel that returns at most ``chunk`` bytes per call."""

    def __init__(self, data, chunk=1):
        self._buffer = io.BytesIO(data)
        self.chunk = chunk
        self.read_calls = 0

    def read(self, n=-1):
        self.read_calls += 1
        if n is None or n < 0:
            n = self.chunk
        return self._buffer.read(min(n, self.chunk))

    @property
    def position(self):
        return self._buffer.tell()


class ShortWriteChannel:
    """A write channel that accepts at most ``chunk`` bytes per call.

    It returns the number of bytes it accepted, never more, mimicking a
    socket-like writer that can accept only a small slice at a time.
    """

    def __init__(self, chunk=1):
        self.chunk = chunk
        self._sink = io.BytesIO()
        self.write_calls = 0

    def write(self, data):
        self.write_calls += 1
        accepted = data[: self.chunk]
        self._sink.write(accepted)
        return len(accepted)

    def getvalue(self):
        return self._sink.getvalue()


class StuckWriteChannel:
    """A write channel that reports zero bytes accepted without progress."""

    def write(self, data):
        return 0


CALC_PROTOCOL_JSON = json.dumps(
    {
        "namespace": "example.proto",
        "protocol": "Calc",
        "messages": {
            "add": {
                "request": [{"name": "a", "type": "int"}, {"name": "b", "type": "int"}],
                "response": "int",
            },
            "echo": {
                "request": [{"name": "text", "type": "string"}],
                "response": "string",
            },
            "fail": {"request": [], "response": "string"},
        },
    }
)
CALC_PROTOCOL = avro.protocol.parse(CALC_PROTOCOL_JSON)


class CalcResponder(avro.ipc.Responder):
    def __init__(self):
        super().__init__(CALC_PROTOCOL)

    def invoke(self, message, request):
        if message.name == "add":
            return request["a"] + request["b"]
        if message.name == "echo":
            return request["text"]
        if message.name == "fail":
            raise avro.errors.AvroRemoteException("business failure")
        raise avro.errors.AvroException(f"Unknown message: {message.name}")


class InProcessTransceiver:
    """Carry IPC bytes through the hardened framed transport in-process.

    Both directions run over channel doubles that move one or two bytes per
    call, so both the reader's short-read handling and the writer's
    short-write handling are exercised on real request/response bytes.
    """

    _instance_counter = 0

    def __init__(self, responder, chunk=1, max_frame_size=avro.ipc.MAX_FRAME_SIZE, max_message_size=avro.ipc.MAX_MESSAGE_SIZE):
        self.responder = responder
        self.chunk = chunk
        self.max_frame_size = max_frame_size
        self.max_message_size = max_message_size
        # A stable, never-reused remote name keeps the module-level
        # protocol/handshake cache from leaking state between tests.
        InProcessTransceiver._instance_counter += 1
        self.remote_name = f"in-process-{InProcessTransceiver._instance_counter}"
        self.requests = []

    def transceive(self, request):
        request_bytes = self._encode(request)
        self.requests.append(request_bytes)
        call_request = self._decode(request_bytes)
        response_bytes = self.responder.respond(call_request)
        return self._decode(self._encode(response_bytes))

    def _encode(self, message):
        writer = avro.ipc.FramedWriter(ShortWriteChannel(self.chunk))
        writer.write_framed_message(message)
        return writer.writer.getvalue()

    def _decode(self, data):
        reader = avro.ipc.FramedReader(
            ShortReadChannel(data, self.chunk),
            max_frame_size=self.max_frame_size,
            max_message_size=self.max_message_size,
        )
        message = reader.read_framed_message()
        assert message is not None, "expected a framed message, got EOF"
        return message


class TestFramedTransport(unittest.TestCase):
    """Unit tests for FramedReader and FramedWriter."""

    def test_round_trip_multiple_sizes_through_short_channels(self):
        sizes = [0, 1, BUFFER_SIZE - 1, BUFFER_SIZE, BUFFER_SIZE + 1, 2 * BUFFER_SIZE + 3]
        messages = [bytes([i % 251]) * size for i, size in enumerate(sizes)]
        writer = avro.ipc.FramedWriter(ShortWriteChannel(chunk=1))
        for message in messages:
            writer.write_framed_message(message)
        reader = avro.ipc.FramedReader(ShortReadChannel(writer.writer.getvalue(), chunk=1))
        read = []
        for _ in messages:
            message = reader.read_framed_message()
            self.assertIsNotNone(message)
            read.append(message)
        self.assertEqual(messages, read)
        self.assertIsNone(reader.read_framed_message())

    def test_empty_message_is_distinct_from_clean_eof(self):
        empty = avro.ipc.FramedReader(ShortReadChannel(framed())).read_framed_message()
        self.assertEqual(b"", empty)
        self.assertIsNone(avro.ipc.FramedReader(ShortReadChannel(b"")).read_framed_message())

    def test_consecutive_messages_and_empty_in_the_middle(self):
        stream = framed(b"first") + framed() + framed(b"third")
        reader = avro.ipc.FramedReader(ShortReadChannel(stream, chunk=1))
        self.assertEqual(b"first", reader.read_framed_message())
        self.assertEqual(b"", reader.read_framed_message())
        self.assertEqual(b"third", reader.read_framed_message())
        self.assertIsNone(reader.read_framed_message())

    def test_truncated_length_header(self):
        for header in (b"\x00", b"\x00\x00", b"\x00\x00\x01"):
            with self.assertRaises(avro.errors.ConnectionClosedException):
                avro.ipc.FramedReader(ShortReadChannel(header, chunk=1)).read_framed_message()

    def test_truncated_payload(self):
        stream = HEADER.pack(10) + b"abcd"
        reader = avro.ipc.FramedReader(ShortReadChannel(stream, chunk=1))
        with self.assertRaises(avro.errors.ConnectionClosedException):
            reader.read_framed_message()

    def test_eof_between_frames_is_truncation_not_eof(self):
        stream = frame(b"abc")
        reader = avro.ipc.FramedReader(ShortReadChannel(stream, chunk=1))
        with self.assertRaises(avro.errors.ConnectionClosedException):
            reader.read_framed_message()

    def test_frame_limit_rejects_declared_length_before_payload(self):
        stream = HEADER.pack(1 << 30) + b"payload"
        channel = ShortReadChannel(stream, chunk=1)
        reader = avro.ipc.FramedReader(channel, max_frame_size=1024)
        with self.assertRaises(avro.errors.AvroFrameSizeException):
            reader.read_framed_message()
        # Only the four length bytes were consumed; the payload was never touched.
        self.assertEqual(4, channel.position)

    def test_message_limit_rejects_accumulated_length(self):
        stream = framed(b"abcdef", b"ghijkl")
        channel = ShortReadChannel(stream, chunk=1)
        reader = avro.ipc.FramedReader(channel, max_message_size=10)
        with self.assertRaises(avro.errors.AvroMessageSizeException):
            reader.read_framed_message()
        # The first frame and the second length prefix were read; the second
        # frame's payload was never touched.
        self.assertEqual(4 + 6 + 4, channel.position)

    def test_limits_can_be_disabled(self):
        payload = b"x" * (BUFFER_SIZE + 1)
        reader = avro.ipc.FramedReader(ShortReadChannel(framed(payload), chunk=1), max_frame_size=None, max_message_size=None)
        self.assertEqual(payload, reader.read_framed_message())

    def test_reader_is_broken_after_failure(self):
        # A first frame is followed by an oversized length prefix, whose bytes
        # belong to that frame's payload rather than a new length. A broken
        # reader must not reinterpret them as the start of another message.
        stream = frame(b"abcd") + HEADER.pack(1 << 20) + framed(b"not-a-length")
        channel = ShortReadChannel(stream, chunk=1)
        reader = avro.ipc.FramedReader(channel, max_frame_size=1024)
        with self.assertRaises(avro.errors.AvroFrameSizeException):
            reader.read_framed_message()
        with self.assertRaises(avro.errors.ConnectionClosedException):
            reader.read_framed_message()

    def test_short_write_channel_receives_every_byte(self):
        message = b"0123456789" * 10
        writer = avro.ipc.FramedWriter(ShortWriteChannel(chunk=2))
        writer.write_framed_message(message)
        self.assertEqual(framed(message), writer.writer.getvalue())

    def test_writer_rejects_zero_progress(self):
        writer = avro.ipc.FramedWriter(StuckWriteChannel())
        with self.assertRaises(avro.errors.ConnectionClosedException):
            writer.write_framed_message(b"data")


class TestRequestorResponderOverHardenedTransport(unittest.TestCase):
    """Drive a real handshake and call through the hardened framed transport."""

    def setUp(self):
        self.transceiver = InProcessTransceiver(CalcResponder(), chunk=1)
        self.requestor = avro.ipc.Requestor(CALC_PROTOCOL, self.transceiver)

    def test_handshake_then_call(self):
        self.assertEqual(5, self.requestor.request("add", {"a": 2, "b": 3}))
        # Identical protocols and a responder that knows its own hash complete
        # the handshake in a single round trip.
        self.assertEqual(1, len(self.transceiver.requests))
        self.assertFalse(self.requestor.send_protocol)
        self.assertEqual(CALC_PROTOCOL, self.requestor.remote_protocol)

    def test_handshake_retries_with_client_protocol_on_none_match(self):
        # A client-side protocol with a different hash is initially unknown to
        # the responder, so the first handshake is NONE and must be retried
        # with the client protocol attached.
        other_protocol = avro.protocol.parse(
            CALC_PROTOCOL_JSON.replace('"Calc"', '"CalcClient"', 1)
        )
        requestor = avro.ipc.Requestor(other_protocol, InProcessTransceiver(CalcResponder(), chunk=1))
        self.assertEqual(5, requestor.request("add", {"a": 2, "b": 3}))
        self.assertEqual(2, len(requestor.transceiver.requests))
        self.assertEqual(CALC_PROTOCOL.md5, requestor.remote_hash)

    def test_multiple_calls_over_two_byte_channel(self):
        self.transceiver.chunk = 2
        self.assertEqual(1, self.requestor.request("add", {"a": 0, "b": 1}))
        text = "echo-" * 40
        self.assertEqual(text, self.requestor.request("echo", {"text": text}))
        self.assertEqual("echo-" * 40, self.requestor.request("echo", {"text": text}))

    def test_business_error_is_reported_as_remote_error(self):
        with self.assertRaises(avro.errors.AvroRemoteException) as caught:
            self.requestor.request("fail", {})
        self.assertIn("business failure", str(caught.exception))

    def test_business_error_is_distinct_from_limits_and_truncation(self):
        self.assertNotIsInstance(
            self._remote_error_for_fail(),
            (avro.errors.AvroFrameSizeException, avro.errors.AvroMessageSizeException, avro.errors.ConnectionClosedException),
        )

    def _remote_error_for_fail(self):
        requestor = avro.ipc.Requestor(CALC_PROTOCOL, InProcessTransceiver(CalcResponder(), chunk=1))
        with self.assertRaises(avro.errors.AvroRemoteException) as caught:
            requestor.request("fail", {})
        return caught.exception

    def test_client_message_limit_is_its_own_failure(self):
        transceiver = InProcessTransceiver(CalcResponder(), chunk=1, max_message_size=64)
        requestor = avro.ipc.Requestor(CALC_PROTOCOL, transceiver)
        with self.assertRaises(avro.errors.AvroMessageSizeException):
            requestor.request("echo", {"text": "x" * 200})

    def test_truncated_response_is_its_own_failure(self):
        class TruncatingTransceiver(InProcessTransceiver):
            def transceive(self, request):
                request_bytes = self._encode(request)
                call_request = self._decode(request_bytes)
                response_bytes = self.responder.respond(call_request)
                wire = self._encode(response_bytes)
                # Cut the response on the wire: its final length prefix is
                # only partially present, so the reader must raise truncation.
                return avro.ipc.FramedReader(ShortReadChannel(wire[:-2], chunk=1)).read_framed_message()

        requestor = avro.ipc.Requestor(CALC_PROTOCOL, TruncatingTransceiver(CalcResponder(), chunk=1))
        with self.assertRaises(avro.errors.ConnectionClosedException):
            requestor.request("add", {"a": 1, "b": 2})


if __name__ == "__main__":  # pragma: no coverage
    unittest.main()


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

"""Tests for in-process and HTTP-style IPC transports."""

import io
import json
import unittest

import avro.errors
import avro.ipc
import avro.protocol


PROTOCOL = avro.protocol.parse(
    json.dumps(
        {
            "protocol": "OneWayIPC",
            "messages": {
                "ack": {"request": [], "response": "null", "one-way": True},
                "hello": {"request": [], "response": "string"},
                "fails": {"request": [], "response": "null"},
                "missing": {"request": [], "response": "null"},
            },
        }
    )
)
TWO_WAY_PROTOCOL = avro.protocol.parse(
    json.dumps({"protocol": "TwoWayIPC", "messages": {"ack": {"request": [], "response": "null"}}})
)
UNSUPPORTED_PROTOCOL = avro.protocol.parse(
    json.dumps({"protocol": "UnsupportedIPC", "messages": {"unknown": {"request": [], "response": "null"}}})
)


def framed_messages(payload):
    stream = io.BytesIO(payload)
    reader = avro.ipc.FramedReader(stream)
    return reader.read_framed_message()


def frame_payload(payload):
    buffer = io.BytesIO()
    avro.ipc.FramedWriter(buffer).write_framed_message(payload)
    return buffer.getvalue()


class FakeStatefulTransceiver(avro.ipc.Transceiver):
    def __init__(self, responder):
        self.responder = responder
        self.remote_name = "stateful"
        self.requests = []
        self.writes = []
        self.reads = []
        self.responses = []
        self._is_connected = False
        self._remote_protocol = None

    @property
    def is_connected(self):
        return self._is_connected

    def set_remote(self, remote_protocol):
        self._remote_protocol = remote_protocol
        self._is_connected = True

    def get_remote(self):
        return self._remote_protocol

    def transceive(self, request):
        self.requests.append(("transceive", request))
        self.reads.append(request)
        return self.responder.respond(request, self)

    def write_framed_message(self, message):
        self.requests.append(("write", message))
        self.writes.append(message)
        self.responses.append(self.responder.respond(message, self))


class TestResponder(avro.ipc.Responder):
    def __init__(self):
        super().__init__(PROTOCOL)
        self.invocations = []

    def invoke(self, local_message, request):
        self.invocations.append(local_message.name)
        if local_message.name == "fails":
            raise RuntimeError("server failure")
        if local_message.name == "hello":
            return "hello"
        return None


class FailingWriteTransceiver(avro.ipc.Transceiver):
    remote_name = "failing-write"

    def transceive(self, request):
        raise OSError("transport unavailable")

    def write_framed_message(self, message):
        raise OSError("transport unavailable")


class FailingStatefulWriteTransceiver(avro.ipc.Transceiver):
    remote_name = "failing-stateful-write"

    @property
    def is_connected(self):
        return True

    def get_remote(self):
        return PROTOCOL

    def transceive(self, request):
        raise AssertionError("established one-way calls must not read a response")

    def write_framed_message(self, message):
        raise OSError("transport unavailable")


class FakeHTTPResponse:
    def __init__(self, body):
        self._body = io.BytesIO(body)
        self.reads = []

    def read(self, size=-1):
        data = self._body.read(size)
        self.reads.append(data)
        return data

    def close(self):
        self._body.close()


class FakeHTTPConnection:
    def __init__(self, host, port, responder):
        self.host = host
        self.port = port
        self.responder = responder
        self.sock = type("Socket", (), {"getsockname": lambda self: (host, port)})()
        self.requests = []
        self.getresponse_calls = 0
        self.last_response = None
        self.closed = False

    def connect(self):
        pass

    def request(self, method, path, body, headers):
        self.requests.append((method, path, body, headers))
        self.response_body = frame_payload(self.responder.respond(framed_messages(body)))

    def getresponse(self):
        self.getresponse_calls += 1
        self.last_response = FakeHTTPResponse(self.response_body)
        return self.last_response

    def close(self):
        self.closed = True


class TestIPC(unittest.TestCase):
    def setUp(self):
        self.responder = TestResponder()
        self.transceiver = FakeStatefulTransceiver(self.responder)
        self.requestor = avro.ipc.Requestor(PROTOCOL, self.transceiver)

    def test_one_way_first_call_completes_handshake(self):
        self.assertIsNone(self.requestor.request("ack", {}))

        self.assertEqual(["transceive"], [operation for operation, _ in self.transceiver.requests])
        self.assertEqual([], self.transceiver.writes)
        self.assertEqual(1, len(self.transceiver.reads))
        self.assertTrue(self.transceiver.is_connected)
        self.assertEqual(["ack"], self.responder.invocations)

    def test_one_way_established_session_only_writes(self):
        self.assertIsNone(self.requestor.request("ack", {}))
        first_request = self.transceiver.requests[0][1]

        self.assertIsNone(self.requestor.request("ack", {}))

        self.assertEqual(["transceive", "write"], [operation for operation, _ in self.transceiver.requests])
        self.assertEqual(1, len(self.transceiver.reads))
        self.assertEqual(1, len(self.transceiver.writes))
        self.assertNotEqual(first_request, self.transceiver.writes[0])
        self.assertEqual(["ack", "ack"], self.responder.invocations)

    def test_normal_two_way_call_remains_request_response(self):
        self.assertIsNone(self.requestor.request("ack", {}))

        self.assertEqual("hello", self.requestor.request("hello", {}))
        self.assertEqual(["transceive", "transceive"], [operation for operation, _ in self.transceiver.requests])
        self.assertEqual(2, len(self.transceiver.reads))
        self.assertEqual(["ack", "hello"], self.responder.invocations)

    def test_one_way_server_exception_during_handshake_is_reported(self):
        self.responder.invoke = lambda message, request: (_ for _ in ()).throw(RuntimeError("server failure"))

        with self.assertRaises(avro.errors.AvroRemoteException):
            self.requestor.request("ack", {})

    def test_one_way_server_exception_is_only_visible_to_server_after_handshake(self):
        self.assertIsNone(self.requestor.request("ack", {}))
        first_request = self.transceiver.requests[0][1]
        self.responder.invoke = lambda message, request: (_ for _ in ()).throw(RuntimeError("server failure"))
        self.transceiver.writes.clear()

        self.assertIsNone(self.requestor.request("ack", {}))

        self.assertEqual([first_request], self.transceiver.reads)
        self.assertEqual(1, len(self.transceiver.writes))
        self.assertEqual([None], self.transceiver.responses)

    def test_invalid_protocol_message_returns_system_error(self):
        self.assertIsNone(self.requestor.request("ack", {}))
        requestor = avro.ipc.Requestor(UNSUPPORTED_PROTOCOL, self.transceiver)
        requestor.remote_protocol = self.transceiver.get_remote()

        with self.assertRaises(avro.errors.AvroException) as ctx:
            requestor.request("unknown", {})

        self.assertIn("Unknown remote message", str(ctx.exception))

    def test_transport_failure_is_visible_to_caller(self):
        requestor = avro.ipc.Requestor(PROTOCOL, FailingWriteTransceiver())
        with self.assertRaises(OSError):
            requestor.request("ack", {})

    def test_established_write_failure_is_visible_to_caller(self):
        requestor = avro.ipc.Requestor(PROTOCOL, FailingStatefulWriteTransceiver())
        with self.assertRaises(OSError):
            requestor.request("ack", {})

    def test_connected_one_way_protocol_mismatch_is_rejected_before_write(self):
        self.assertIsNone(self.requestor.request("ack", {}))
        self.transceiver.writes.clear()
        requestor = avro.ipc.Requestor(TWO_WAY_PROTOCOL, self.transceiver)
        requestor.remote_protocol = self.transceiver.get_remote()

        with self.assertRaises(avro.errors.AvroException):
            requestor.request("ack", {})

        self.assertEqual([], self.transceiver.writes)

    def test_http_one_way_consumes_transport_response_and_releases_connection(self):
        server_responder = TestResponder()
        connection = FakeHTTPConnection("example.test", 8080, server_responder)
        transceiver = avro.ipc.HTTPTransceiver("example.test", 8080, conn=connection)
        requestor = avro.ipc.Requestor(PROTOCOL, transceiver)

        self.assertIsNone(requestor.request("ack", {}))
        self.assertEqual(1, len(connection.requests))
        self.assertEqual(1, connection.getresponse_calls)
        self.assertTrue(connection.last_response.reads)
        self.assertEqual(b"", connection.last_response.reads[-1])
        self.assertFalse(transceiver.is_connected)

        self.assertIsNone(requestor.request("ack", {}))

        self.assertEqual(2, len(connection.requests))
        self.assertEqual(2, connection.getresponse_calls)
        self.assertEqual(b"", connection.last_response.reads[-1])
        self.assertEqual(["ack", "ack"], server_responder.invocations)

    def test_http_two_way_call_remains_request_response(self):
        server_responder = TestResponder()
        connection = FakeHTTPConnection("example.test", 8081, server_responder)
        transceiver = avro.ipc.HTTPTransceiver("example.test", 8081, conn=connection)

        result = avro.ipc.Requestor(PROTOCOL, transceiver).request("hello", {})

        self.assertEqual("hello", result)
        self.assertEqual(1, len(connection.requests))
        self.assertEqual(1, connection.getresponse_calls)

    def test_http_one_way_server_failure_is_transport_visible(self):
        server_responder = TestResponder()
        server_responder.invoke = lambda message, request: (_ for _ in ()).throw(RuntimeError("server failure"))
        connection = FakeHTTPConnection("example.test", 8082, server_responder)
        transceiver = avro.ipc.HTTPTransceiver("example.test", 8082, conn=connection)

        with self.assertRaises(avro.errors.AvroRemoteException):
            avro.ipc.Requestor(PROTOCOL, transceiver).request("ack", {})

        self.assertEqual(1, connection.getresponse_calls)
        self.assertEqual(b"", connection.last_response.reads[-1])

    def test_http_path_constructor_without_network(self):
        connection = FakeHTTPConnection("example.test", 8080, TestResponder())
        transceiver = avro.ipc.HTTPTransceiver("example.test", 8080, "/service/article", conn=connection)

        self.assertEqual("/service/article", transceiver.req_resource)
        self.assertEqual(("example.test", 8080), transceiver.remote_name)


if __name__ == "__main__":  # pragma: no coverage
    unittest.main()

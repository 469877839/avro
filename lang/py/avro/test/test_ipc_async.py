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

"""Tests for the asyncio-based IPC requestor."""

import asyncio
import io
import unittest

import avro.errors
import avro.ipc
import avro.protocol

TEST_PROTOCOL_JSON = """{
  "namespace": "org.apache.avro.test",
  "protocol": "AsyncTest",
  "types": [
    {"name": "TestError", "type": "error", "fields": [{"name": "message", "type": "string"}]}
  ],
  "messages": {
    "hello": {
      "request": [{"name": "greeting", "type": "string"}],
      "response": "string"
    },
    "add": {
      "request": [{"name": "arg1", "type": "int"}, {"name": "arg2", "type": "int"}],
      "response": "int"
    },
    "error": {
      "request": [],
      "response": "null",
      "errors": ["TestError"]
    },
    "ack": {
      "request": [],
      "response": "null",
      "one-way": true
    }
  }
}"""

TEST_PROTOCOL = avro.protocol.parse(TEST_PROTOCOL_JSON)

# A server-side variant: identical message shapes, different protocol text,
# so its MD5 hash differs and the initial handshake must negotiate.
SERVER_PROTOCOL = avro.protocol.parse(
    TEST_PROTOCOL_JSON.replace('"protocol": "AsyncTest"', '"protocol": "AsyncTestServer"')
)


class EchoResponder(avro.ipc.Responder):
    """In-memory responder implementing the test protocol."""

    def __init__(self, protocol):
        super().__init__(protocol)
        self.acks = []

    def invoke(self, local_message, request):
        if local_message.name == "hello":
            return f"Hello, {request['greeting']}!"
        if local_message.name == "add":
            return request["arg1"] + request["arg2"]
        if local_message.name == "error":
            raise avro.errors.AvroRemoteException("it always fails")
        if local_message.name == "ack":
            self.acks.append(request)
            return None
        raise avro.errors.AvroException(f"Unexpected message: {local_message.name}")


class MemoryTransceiver(avro.ipc.AsyncTransceiver):
    """An in-memory async transceiver with hooks for barriers and failures.

    ``send`` and ``receive`` are awaitable and never block the event loop.
    """

    _next_id = 0

    def __init__(self, responder):
        self._responder = responder
        # Unique per instance: handshake caches are keyed by remote_name.
        MemoryTransceiver._next_id += 1
        self._remote_name = f"memory-{MemoryTransceiver._next_id}"
        self.sent = []
        self.received = []
        self.send_count = 0
        self.receive_count = 0
        self.aclosed = False
        # test hooks
        self.pause_send = False
        self.pause_receive = False
        self.fail_on_receive = None
        self.send_started = asyncio.Event()
        self.receive_started = asyncio.Event()
        self.release_send = asyncio.Event()
        self.release_receive = asyncio.Event()

    @property
    def remote_name(self):
        return self._remote_name

    async def send(self, request):
        self.send_count += 1
        self.send_started.set()
        if self.pause_send:
            await self.release_send.wait()
        self.sent.append(request)
        await asyncio.sleep(0)

    async def receive(self):
        self.receive_count += 1
        self.receive_started.set()
        if self.pause_receive:
            await self.release_receive.wait()
        if self.fail_on_receive is not None:
            raise self.fail_on_receive
        await asyncio.sleep(0)
        response = self._responder.respond(self.sent.pop(0))
        self.received.append(response)
        return response

    async def aclose(self):
        self.aclosed = True


def no_pending_tasks(test_case):
    """Assert that no tasks other than the current one remain."""
    current = asyncio.current_task()
    pending = [task for task in asyncio.all_tasks() if task is not current and not task.done()]
    test_case.assertEqual([], pending)


class TestAsyncRequestor(unittest.IsolatedAsyncioTestCase):
    async def test_basic_call(self):
        transceiver = MemoryTransceiver(EchoResponder(TEST_PROTOCOL))
        requestor = avro.ipc.AsyncRequestor(TEST_PROTOCOL, transceiver)
        self.assertEqual("Hello, world!", await requestor.request("hello", {"greeting": "world"}))
        self.assertEqual(7, await requestor.request("add", {"arg1": 3, "arg2": 4}))
        self.assertFalse(requestor.closed)
        no_pending_tasks(self)

    async def test_remote_error_keeps_connection(self):
        transceiver = MemoryTransceiver(EchoResponder(TEST_PROTOCOL))
        requestor = avro.ipc.AsyncRequestor(TEST_PROTOCOL, transceiver)
        with self.assertRaises(avro.errors.AvroRemoteException):
            await requestor.request("error", {})
        # The response was fully consumed, so the connection stays usable.
        self.assertFalse(requestor.closed)
        self.assertEqual(5, await requestor.request("add", {"arg1": 2, "arg2": 3}))
        no_pending_tasks(self)

    async def test_one_way(self):
        responder = EchoResponder(TEST_PROTOCOL)
        transceiver = MemoryTransceiver(responder)
        requestor = avro.ipc.AsyncRequestor(TEST_PROTOCOL, transceiver)
        self.assertIsNone(await requestor.request("ack", {}))
        self.assertEqual([{}], responder.acks)
        self.assertFalse(requestor.closed)
        # A regular call still works afterwards on the same connection.
        self.assertEqual("Hello, x!", await requestor.request("hello", {"greeting": "x"}))
        no_pending_tasks(self)

    async def test_one_way_with_handshake_negotiation(self):
        responder = EchoResponder(SERVER_PROTOCOL)
        transceiver = MemoryTransceiver(responder)
        requestor = avro.ipc.AsyncRequestor(TEST_PROTOCOL, transceiver)
        self.assertIsNone(await requestor.request("ack", {}))
        self.assertEqual([{}], responder.acks)
        # NONE handshake response plus the matched one-way exchange: both
        # responses contain only handshake bytes, never a call response.
        self.assertEqual(2, transceiver.receive_count)
        decoder = avro.io.BinaryDecoder(io.BytesIO(transceiver.received[-1]))
        self.assertTrue(requestor.read_handshake_response(decoder))
        # No call response bytes remain after the handshake.
        self.assertEqual(b"", decoder.reader.read())
        self.assertFalse(requestor.closed)
        no_pending_tasks(self)

    async def test_concurrent_calls_share_first_handshake(self):
        transceiver = MemoryTransceiver(EchoResponder(SERVER_PROTOCOL))
        transceiver.pause_send = True
        requestor = avro.ipc.AsyncRequestor(TEST_PROTOCOL, transceiver)
        first = asyncio.ensure_future(requestor.request("hello", {"greeting": "a"}))
        second = asyncio.ensure_future(requestor.request("add", {"arg1": 40, "arg2": 2}))
        # Barrier: the second call must be queued behind the in-flight call
        # (whose handshake negotiation owns the connection).
        await transceiver.send_started.wait()
        await asyncio.sleep(0)
        self.assertFalse(second.done())
        self.assertEqual(1, transceiver.send_count)
        transceiver.release_send.set()
        hello, added = await asyncio.gather(first, second)
        self.assertEqual("Hello, a!", hello)
        self.assertEqual(42, added)
        # First call: handshake NONE + resubmit (2 exchanges); the second call
        # reuses the negotiated protocol, so it needs only one exchange.
        self.assertEqual(3, transceiver.send_count)
        self.assertFalse(requestor.closed)
        no_pending_tasks(self)

    async def test_concurrent_calls_get_matching_responses(self):
        transceiver = MemoryTransceiver(EchoResponder(TEST_PROTOCOL))
        requestor = avro.ipc.AsyncRequestor(TEST_PROTOCOL, transceiver)
        calls = [
            requestor.request("add", {"arg1": 0, "arg2": 100}),
            requestor.request("hello", {"greeting": "a"}),
            requestor.request("add", {"arg1": 2, "arg2": 200}),
            requestor.request("hello", {"greeting": "b"}),
        ]
        results = await asyncio.gather(*calls)
        self.assertEqual([100, "Hello, a!", 202, "Hello, b!"], results)
        no_pending_tasks(self)

    async def test_concurrent_calls_do_not_block_event_loop(self):
        transceiver = MemoryTransceiver(EchoResponder(TEST_PROTOCOL))
        transceiver.pause_send = True
        requestor = avro.ipc.AsyncRequestor(TEST_PROTOCOL, transceiver)
        call = asyncio.ensure_future(requestor.request("hello", {"greeting": "slow"}))
        await transceiver.send_started.wait()
        # The event loop must stay responsive while the call is in flight.
        ticks = 0
        for _ in range(10):
            await asyncio.sleep(0)
            ticks += 1
        self.assertEqual(10, ticks)
        self.assertFalse(call.done())
        transceiver.release_send.set()
        self.assertEqual("Hello, slow!", await call)
        no_pending_tasks(self)

    async def test_queued_call_cancellation_keeps_connection(self):
        transceiver = MemoryTransceiver(EchoResponder(TEST_PROTOCOL))
        transceiver.pause_send = True
        requestor = avro.ipc.AsyncRequestor(TEST_PROTOCOL, transceiver)
        first = asyncio.ensure_future(requestor.request("hello", {"greeting": "first"}))
        await transceiver.send_started.wait()
        second = asyncio.ensure_future(requestor.request("add", {"arg1": 1, "arg2": 1}))
        await asyncio.sleep(0)
        second.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await second
        # The waiter was cancelled before sending anything: connection usable.
        self.assertFalse(requestor.closed)
        transceiver.release_send.set()
        self.assertEqual("Hello, first!", await first)
        self.assertEqual(2, await requestor.request("add", {"arg1": 1, "arg2": 1}))
        no_pending_tasks(self)

    async def test_in_flight_cancellation_closes_connection(self):
        transceiver = MemoryTransceiver(EchoResponder(TEST_PROTOCOL))
        transceiver.pause_receive = True
        requestor = avro.ipc.AsyncRequestor(TEST_PROTOCOL, transceiver)
        call = asyncio.ensure_future(requestor.request("hello", {"greeting": "doomed"}))
        await transceiver.receive_started.wait()
        call.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await call
        # The request may already have been executed remotely, so the
        # connection can no longer be trusted for further calls.
        self.assertTrue(requestor.closed)
        self.assertTrue(transceiver.aclosed)
        transceiver.release_receive.set()
        with self.assertRaises(avro.errors.ConnectionClosedException):
            await requestor.request("add", {"arg1": 1, "arg2": 1})
        no_pending_tasks(self)

    async def test_transport_failure_closes_connection(self):
        transceiver = MemoryTransceiver(EchoResponder(TEST_PROTOCOL))
        transceiver.fail_on_receive = avro.errors.ConnectionClosedException("boom")
        requestor = avro.ipc.AsyncRequestor(TEST_PROTOCOL, transceiver)
        with self.assertRaises(avro.errors.ConnectionClosedException):
            await requestor.request("hello", {"greeting": "x"})
        self.assertTrue(requestor.closed)
        self.assertTrue(transceiver.aclosed)
        with self.assertRaises(avro.errors.ConnectionClosedException):
            await requestor.request("hello", {"greeting": "y"})
        no_pending_tasks(self)

    async def test_send_failure_closes_connection(self):
        transceiver = MemoryTransceiver(EchoResponder(TEST_PROTOCOL))
        requestor = avro.ipc.AsyncRequestor(TEST_PROTOCOL, transceiver)

        async def failing_send(request):
            transceiver.send_count += 1
            raise OSError("write failed mid-frame")

        transceiver.send = failing_send
        with self.assertRaises(OSError):
            await requestor.request("hello", {"greeting": "x"})
        self.assertTrue(requestor.closed)
        self.assertTrue(transceiver.aclosed)
        with self.assertRaises(avro.errors.ConnectionClosedException):
            await requestor.request("hello", {"greeting": "y"})
        no_pending_tasks(self)


if __name__ == "__main__":  # pragma: no coverage
    unittest.main()

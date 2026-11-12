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
IPC tests, including in-memory transports used to exercise the handshake
protocol without opening real sockets.
"""

import io
import json
import threading
import unittest

import avro.errors
import avro.io
import avro.ipc
import avro.protocol


def _protocol(name, extra_message=False):
    """Build a small protocol whose ping response identifies its owner."""
    messages = {"ping": {"request": [], "response": "string"}}
    if extra_message:
        messages["echo"] = {"request": [{"name": "text", "type": "string"}], "response": "string"}
    return avro.protocol.parse(
        json.dumps(
            {
                "protocol": name,
                "namespace": "example.test",
                "messages": messages,
            }
        )
    )


PROTOCOL_A = _protocol("ProtoA")
PROTOCOL_A_V2 = _protocol("ProtoAv2", extra_message=True)
PROTOCOL_B = _protocol("ProtoB")


def _read_handshake_request(raw_call_request):
    decoder = avro.io.BinaryDecoder(io.BytesIO(raw_call_request))
    return avro.ipc.HANDSHAKE_RESPONDER_READER.read(decoder)


def _read_handshake_response(raw_call_response):
    decoder = avro.io.BinaryDecoder(io.BytesIO(raw_call_response))
    return avro.ipc.HANDSHAKE_REQUESTOR_READER.read(decoder)


class MemoryTransceiver:
    """In-memory transport that forwards framed calls to a Responder."""

    def __init__(self, responder, remote_name):
        self.responder = responder
        self.remote_name = remote_name
        self.call_requests = []
        self.call_responses = []

    def transceive(self, request):
        self.call_requests.append(request)
        response = self.responder.respond(request)
        self.call_responses.append(response)
        return response


class PingResponder(avro.ipc.Responder):
    """Responder that answers ping with a fixed value."""

    def __init__(self, protocol, response_value):
        super().__init__(protocol)
        self.response_value = response_value

    def invoke(self, local_message, request):
        return self.response_value


class TestHandshakeCache(unittest.TestCase):
    def test_only_verified_entries_are_stored(self):
        cache = avro.ipc.HandshakeCache()
        self.assertIsNone(cache.get("endpoint"))
        # A protocol whose hash is inconsistent with the supplied hash is rejected.
        with self.assertRaises(avro.errors.AvroException):
            cache.set("endpoint", PROTOCOL_A, PROTOCOL_B.md5)
        self.assertIsNone(cache.get("endpoint"))
        self.assertEqual(0, len(cache))

    def test_invalidate_and_clear(self):
        cache = avro.ipc.HandshakeCache()
        cache.set("a", PROTOCOL_A, PROTOCOL_A.md5)
        cache.set("b", PROTOCOL_B, PROTOCOL_B.md5)
        self.assertEqual(2, len(cache))
        self.assertTrue(cache.invalidate("a"))
        self.assertFalse(cache.invalidate("a"))
        self.assertEqual(1, len(cache))
        cache.clear()
        self.assertEqual(0, len(cache))

    def test_entries_are_immutable_verified_results(self):
        cache = avro.ipc.HandshakeCache()
        entry = cache.set("endpoint", PROTOCOL_A, PROTOCOL_A.md5)
        self.assertIs(entry.protocol, PROTOCOL_A)
        self.assertEqual(PROTOCOL_A.md5, entry.hash)


class TestHandshakeIsolation(unittest.TestCase):
    """
    Two endpoints share the same transceiver.remote_name but speak different
    protocols. Interleaved calls must never mix their handshake state.
    """

    def setUp(self):
        self.server_a = PingResponder(PROTOCOL_A, "from-A")
        self.server_b = PingResponder(PROTOCOL_B, "from-B")
        self.transceiver_a = MemoryTransceiver(self.server_a, "same-remote-name")
        self.transceiver_b = MemoryTransceiver(self.server_b, "same-remote-name")
        # No shared cache: each requestor is isolated by default.
        self.requestor_a = avro.ipc.Requestor(PROTOCOL_A, self.transceiver_a)
        self.requestor_b = avro.ipc.Requestor(PROTOCOL_B, self.transceiver_b)

    def test_interleaved_calls_use_correct_protocol(self):
        self.assertEqual(self.requestor_a.endpoint_id, self.requestor_b.endpoint_id)
        self.assertIsNot(self.requestor_a.handshake_cache, self.requestor_b.handshake_cache)

        for _ in range(3):
            self.assertEqual(self.requestor_a.request("ping", {}), "from-A")
            self.assertEqual(self.requestor_b.request("ping", {}), "from-B")

        for raw in self.transceiver_a.call_requests:
            handshake = _read_handshake_request(raw)
            self.assertEqual(handshake["clientHash"], PROTOCOL_A.md5)
            self.assertEqual(handshake["serverHash"], PROTOCOL_A.md5)
            self.assertIsNone(handshake["clientProtocol"])
        for raw in self.transceiver_b.call_requests:
            handshake = _read_handshake_request(raw)
            self.assertEqual(handshake["clientHash"], PROTOCOL_B.md5)
            self.assertEqual(handshake["serverHash"], PROTOCOL_B.md5)
            self.assertIsNone(handshake["clientProtocol"])

        self.assertEqual(1, len(self.requestor_a.handshake_cache))
        self.assertEqual(1, len(self.requestor_b.handshake_cache))
        self.assertEqual(PROTOCOL_A.md5, self.requestor_a.handshake_cache.get("same-remote-name").hash)
        self.assertEqual(PROTOCOL_B.md5, self.requestor_b.handshake_cache.get("same-remote-name").hash)

    def test_second_call_is_a_cache_hit(self):
        self.requestor_a.request("ping", {})
        self.requestor_a.request("ping", {})
        first = _read_handshake_request(self.transceiver_a.call_requests[0])
        second = _read_handshake_request(self.transceiver_a.call_requests[1])
        # Both requests send only hashes; no full protocol text is ever sent.
        self.assertEqual(first["serverHash"], second["serverHash"])
        self.assertIsNone(second["clientProtocol"])

    def test_server_protocol_update_only_affects_that_endpoint(self):
        self.requestor_a.request("ping", {})
        self.requestor_b.request("ping", {})

        # Endpoint A updates its protocol and, like a restarted server, has
        # forgotten the client protocol it learned earlier.
        self.transceiver_a.responder = PingResponder(PROTOCOL_A_V2, "from-A-v2")

        # Endpoint B keeps its cached protocol and still decodes correctly.
        self.assertEqual(self.requestor_b.request("ping", {}), "from-B")

        update_start = len(self.transceiver_a.call_requests)
        self.assertEqual(self.requestor_a.request("ping", {}), "from-A-v2")
        update_requests = self.transceiver_a.call_requests[update_start:]
        update_responses = self.transceiver_a.call_responses[update_start:]

        # First exchange: the client offers the stale hash, the server says NONE.
        first_request = _read_handshake_request(update_requests[0])
        first_response = _read_handshake_response(update_responses[0])
        self.assertEqual(first_request["serverHash"], PROTOCOL_A.md5)
        self.assertIsNone(first_request["clientProtocol"])
        self.assertEqual("NONE", first_response["match"])

        # Second exchange: the client resends with its protocol text and the
        # accepted protocol. It never blindly sends the full protocol.
        second_request = _read_handshake_request(update_requests[1])
        second_response = _read_handshake_response(update_responses[1])
        self.assertEqual(second_request["serverHash"], PROTOCOL_A_V2.md5)
        self.assertIsNotNone(second_request["clientProtocol"])
        self.assertEqual("BOTH", second_response["match"])

        # The cache now holds endpoint A's new protocol, while endpoint B is untouched.
        self.assertEqual(PROTOCOL_A_V2.md5, self.requestor_a.handshake_cache.get("same-remote-name").hash)
        self.assertEqual(PROTOCOL_B.md5, self.requestor_b.handshake_cache.get("same-remote-name").hash)

        # Subsequent calls negotiate with hashes only and decode with the new protocol.
        update_start = len(self.transceiver_a.call_requests)
        self.assertEqual(self.requestor_a.request("ping", {}), "from-A-v2")
        handshake = _read_handshake_request(self.transceiver_a.call_requests[update_start])
        self.assertEqual(handshake["serverHash"], PROTOCOL_A_V2.md5)
        self.assertIsNone(handshake["clientProtocol"])

    def test_explicit_invalidation_forces_renegotiation(self):
        self.requestor_a.request("ping", {})
        self.assertEqual(PROTOCOL_A.md5, self.requestor_a.handshake_cache.get("same-remote-name").hash)

        self.requestor_a.invalidate_handshake_cache()
        self.assertFalse(self.requestor_a.handshake_cache.get("same-remote-name"))
        self.assertIsNone(self.requestor_a.remote_protocol)

        self.assertEqual(self.requestor_a.request("ping", {}), "from-A")
        self.assertEqual(PROTOCOL_A.md5, self.requestor_a.handshake_cache.get("same-remote-name").hash)


class TestHandshakeNegotiation(unittest.TestCase):
    def setUp(self):
        self.server = PingResponder(PROTOCOL_B, "from-B")
        self.transceiver = MemoryTransceiver(self.server, "endpoint")
        self.requestor = avro.ipc.Requestor(PROTOCOL_A, self.transceiver)

    def test_first_contact_different_protocols_goes_none_then_both(self):
        self.assertEqual(self.requestor.request("ping", {}), "from-B")
        first_request = _read_handshake_request(self.transceiver.call_requests[0])
        first_response = _read_handshake_response(self.transceiver.call_responses[0])
        self.assertIsNone(first_request["clientProtocol"])
        self.assertEqual("NONE", first_response["match"])

        second_request = _read_handshake_request(self.transceiver.call_requests[1])
        second_response = _read_handshake_response(self.transceiver.call_responses[1])
        self.assertIsNotNone(second_request["clientProtocol"])
        self.assertEqual(PROTOCOL_B.md5, second_request["serverHash"])
        self.assertEqual("BOTH", second_response["match"])

        # The full protocol text is only sent during negotiation, never again.
        for _ in range(4):
            self.assertEqual(self.requestor.request("ping", {}), "from-B")
        for raw in self.transceiver.call_requests[2:]:
            self.assertIsNone(_read_handshake_request(raw)["clientProtocol"])
        self.assertEqual(PROTOCOL_B.md5, self.requestor.handshake_cache.get("endpoint").hash)

    def test_client_match_completes_in_a_single_exchange(self):
        # Teach the server the client's protocol, so the stale server hash
        # produces CLIENT rather than NONE.
        self.server.set_protocol_cache(PROTOCOL_A.md5, PROTOCOL_A)
        exchanges = len(self.transceiver.call_requests)
        self.assertEqual(self.requestor.request("ping", {}), "from-B")
        self.assertEqual(1, len(self.transceiver.call_requests) - exchanges)
        response = _read_handshake_response(self.transceiver.call_responses[-1])
        self.assertEqual("CLIENT", response["match"])
        self.assertEqual(PROTOCOL_B.md5, self.requestor.handshake_cache.get("endpoint").hash)
        self.assertFalse(self.requestor.send_protocol)


class TestSharedHandshakeCache(unittest.TestCase):
    def test_shared_cache_uses_caller_provided_endpoint_identity(self):
        cache = avro.ipc.HandshakeCache()
        transceiver_a = MemoryTransceiver(PingResponder(PROTOCOL_A, "from-A"), "collision")
        transceiver_b = MemoryTransceiver(PingResponder(PROTOCOL_B, "from-B"), "collision")
        requestor_a = avro.ipc.Requestor(PROTOCOL_A, transceiver_a, cache, endpoint_id="service-a")
        requestor_b = avro.ipc.Requestor(PROTOCOL_B, transceiver_b, cache, endpoint_id="service-b")
        # cache_key is retained as an alias for endpoint_id.
        self.assertEqual("service-a", requestor_a.endpoint_id)
        self.assertEqual("service-b", requestor_b.endpoint_id)

        self.assertEqual(requestor_a.request("ping", {}), "from-A")
        self.assertEqual(requestor_b.request("ping", {}), "from-B")
        self.assertEqual(2, len(cache))
        self.assertEqual(PROTOCOL_A.md5, cache.get("service-a").hash)
        self.assertEqual(PROTOCOL_B.md5, cache.get("service-b").hash)

    def test_shared_cache_across_concurrent_requestors(self):
        cache = avro.ipc.HandshakeCache()
        responder = PingResponder(PROTOCOL_A, "from-A")
        failures = []

        def worker():
            try:
                for _ in range(25):
                    requestor = avro.ipc.Requestor(PROTOCOL_A, MemoryTransceiver(responder, "shared-name"), cache, endpoint_id="service-a")
                    self.assertEqual(requestor.request("ping", {}), "from-A")
            except Exception as e:  # pragma: no cover - only on failure
                failures.append(e)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual([], failures)
        self.assertEqual(1, len(cache))
        self.assertEqual(PROTOCOL_A.md5, cache.get("service-a").hash)


class _BrokenHandshakeResponder(avro.ipc.Responder):
    """Responder that returns a malformed successful handshake."""

    def __init__(self, protocol, server_protocol, server_hash):
        super().__init__(protocol)
        self.server_protocol = server_protocol
        self.server_hash = server_hash

    def process_handshake(self, decoder, encoder):
        avro.ipc.HANDSHAKE_RESPONDER_READER.read(decoder)
        response = {"match": "CLIENT", "serverProtocol": self.server_protocol, "serverHash": self.server_hash}
        avro.ipc.HANDSHAKE_RESPONDER_WRITER.write(response, encoder)
        return self.local_protocol


class TestHandshakeFailureIsolation(unittest.TestCase):
    def test_unparseable_protocol_does_not_populate_cache(self):
        transceiver = MemoryTransceiver(_BrokenHandshakeResponder(PROTOCOL_A, "not valid json", PROTOCOL_A.md5), "endpoint")
        requestor = avro.ipc.Requestor(PROTOCOL_A, transceiver)
        with self.assertRaises(avro.errors.AvroException):
            requestor.request("ping", {})
        self.assertEqual(0, len(requestor.handshake_cache))
        self.assertIsNone(requestor.remote_protocol)

    def test_hash_mismatch_does_not_populate_cache(self):
        transceiver = MemoryTransceiver(_BrokenHandshakeResponder(PROTOCOL_A, str(PROTOCOL_A), PROTOCOL_B.md5), "endpoint")
        requestor = avro.ipc.Requestor(PROTOCOL_A, transceiver)
        with self.assertRaises(avro.errors.AvroException):
            requestor.request("ping", {})
        self.assertEqual(0, len(requestor.handshake_cache))
        self.assertIsNone(requestor.remote_protocol)

    def test_failed_handshake_does_not_erase_verified_entry(self):
        transceiver = MemoryTransceiver(PingResponder(PROTOCOL_A, "from-A"), "endpoint")
        requestor = avro.ipc.Requestor(PROTOCOL_A, transceiver)
        self.assertEqual(requestor.request("ping", {}), "from-A")
        verified = requestor.handshake_cache.get("endpoint")
        self.assertIsNotNone(verified)

        # The same endpoint now returns a broken protocol. The failure must not
        # replace or remove the previously verified entry.
        transceiver.responder = _BrokenHandshakeResponder(PROTOCOL_A, "not valid json", PROTOCOL_A.md5)
        with self.assertRaises(avro.errors.AvroException):
            requestor.request("ping", {})
        self.assertEqual(verified, requestor.handshake_cache.get("endpoint"))


class TestIPC(unittest.TestCase):
    def test_placeholder(self):
        pass

    def test_server_with_path(self):
        client_with_custom_path = avro.ipc.HTTPTransceiver("apache.org", 80, "/service/article")
        self.assertEqual("/service/article", client_with_custom_path.req_resource)

        client_with_default_path = avro.ipc.HTTPTransceiver("apache.org", 80)
        self.assertEqual("/", client_with_default_path.req_resource)


if __name__ == "__main__":  # pragma: no coverage
    unittest.main()

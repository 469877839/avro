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

"""Support for inter-process calls."""

import http.client
import io
import os
import struct
import threading
from typing import Any, Dict, Optional

import avro.errors
import avro.io
import avro.protocol
import avro.schema


def _load(name):
    dir_path = os.path.dirname(__file__)
    rsrc_path = os.path.join(dir_path, name)
    with open(rsrc_path) as f:
        return f.read()


HANDSHAKE_REQUEST_SCHEMA_JSON = _load("HandshakeRequest.avsc")
HANDSHAKE_RESPONSE_SCHEMA_JSON = _load("HandshakeResponse.avsc")
HANDSHAKE_REQUEST_SCHEMA = avro.schema.parse(HANDSHAKE_REQUEST_SCHEMA_JSON)
HANDSHAKE_RESPONSE_SCHEMA = avro.schema.parse(HANDSHAKE_RESPONSE_SCHEMA_JSON)

HANDSHAKE_REQUESTOR_WRITER = avro.io.DatumWriter(HANDSHAKE_REQUEST_SCHEMA)
HANDSHAKE_REQUESTOR_READER = avro.io.DatumReader(HANDSHAKE_RESPONSE_SCHEMA)
HANDSHAKE_RESPONDER_WRITER = avro.io.DatumWriter(HANDSHAKE_RESPONSE_SCHEMA)
HANDSHAKE_RESPONDER_READER = avro.io.DatumReader(HANDSHAKE_REQUEST_SCHEMA)

META_SCHEMA = avro.schema.parse('{"type": "map", "values": "bytes"}')
META_WRITER = avro.io.DatumWriter(META_SCHEMA)
META_READER = avro.io.DatumReader(META_SCHEMA)

SYSTEM_ERROR_SCHEMA = avro.schema.parse('["string"]')

# Deprecated module-level handshake caches, kept for backward compatibility.
# New code should use a HandshakeCache instance instead: the module-level
# dictionaries are keyed only by transceiver.remote_name, so two distinct
# endpoints that happen to share a name would silently share protocol state.
REMOTE_HASHES: Dict[Any, bytes] = {}
REMOTE_PROTOCOLS: Dict[Any, "avro.protocol.Protocol"] = {}

BIG_ENDIAN_INT_STRUCT = struct.Struct("!I")
BUFFER_HEADER_LENGTH = 4
BUFFER_SIZE = 8192


class HandshakeEntry:
    """
    A single verified handshake result for one endpoint.

    An entry is only created after a handshake has completed successfully:
    the protocol has been parsed locally and its MD5 digest matches the hash
    reported by the remote endpoint. Entries are immutable so they can be
    shared safely between concurrent requestors.
    """

    __slots__ = ("_protocol", "_hash")

    def __init__(self, protocol: "avro.protocol.Protocol", hash: bytes) -> None:
        if not isinstance(protocol, avro.protocol.Protocol):
            raise avro.errors.AvroException(f"Handshake cache entries require a Protocol, got {type(protocol).__name__}")
        if protocol.md5 != hash:
            raise avro.errors.AvroException("Handshake cache entry rejected: protocol MD5 does not match the remote hash")
        self._protocol = protocol
        self._hash = hash

    @property
    def protocol(self) -> "avro.protocol.Protocol":
        return self._protocol

    @property
    def hash(self) -> bytes:
        return self._hash

    def __eq__(self, that: object) -> bool:
        return isinstance(that, HandshakeEntry) and self.hash == that.hash and self.protocol == that.protocol

    def __repr__(self) -> str:
        return f"HandshakeEntry(protocol={self.protocol.fullname!r}, hash={self.hash.hex()!r})"


class HandshakeCache:
    """
    Thread-safe cache of verified handshake results, keyed by endpoint identity.

    The cache key is a stable endpoint identity supplied by the caller (for
    example a URL, a ``(host, port)`` pair, or a logical service name). Each
    requestor that does not share its cache is fully isolated: two endpoints
    that happen to have the same ``transceiver.remote_name`` no longer leak
    protocol state into each other.

    Only protocol information that has been verified during a completed
    handshake is stored here; a failed handshake never mutates the cache, so
    one bad exchange cannot pollute other sessions.
    """

    def __init__(self) -> None:
        self._entries: Dict[Any, HandshakeEntry] = {}
        self._lock = threading.RLock()

    def get(self, endpoint_id: Any) -> Optional[HandshakeEntry]:
        """Return the verified entry for ``endpoint_id``, or None."""
        with self._lock:
            return self._entries.get(endpoint_id)

    def set(self, endpoint_id: Any, protocol: "avro.protocol.Protocol", hash: bytes) -> HandshakeEntry:
        """
        Store a verified handshake result for ``endpoint_id``.

        The protocol's MD5 digest must equal ``hash``; otherwise the entry is
        rejected and the cache is left untouched.
        """
        entry = HandshakeEntry(protocol, hash)
        with self._lock:
            self._entries[endpoint_id] = entry
        return entry

    def invalidate(self, endpoint_id: Any) -> bool:
        """Drop the entry for ``endpoint_id``. Returns True if one existed."""
        with self._lock:
            return self._entries.pop(endpoint_id, None) is not None

    def clear(self) -> None:
        """Drop every cached handshake result."""
        with self._lock:
            self._entries.clear()

    def __contains__(self, endpoint_id: Any) -> bool:
        with self._lock:
            return endpoint_id in self._entries

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)


#
# Base IPC Classes (Requestor/Responder)
#


class BaseRequestor:
    """Base class for the client side of a protocol interaction."""

    def __init__(self, local_protocol, transceiver, handshake_cache=None, endpoint_id=None, *, cache_key=None):
        """
        :param local_protocol: the protocol the client speaks.
        :param transceiver: the transport used to reach the server.
        :param handshake_cache: optional HandshakeCache holding verified remote
            protocols. When omitted, a private cache is created so this
            requestor is isolated from every other endpoint, even ones whose
            transceivers report the same remote_name. Pass a shared cache to
            reuse handshakes across requestors.
        :param endpoint_id: stable identity of the remote endpoint used as the
            cache key (for example a URL or a (host, port) pair). Defaults to
            ``transceiver.remote_name`` for backward compatibility. Callers
            sharing a cache between endpoints must supply distinct identities.
        :param cache_key: deprecated alias for endpoint_id.
        """
        if cache_key is not None:
            if endpoint_id is not None:
                raise avro.errors.AvroException("Specify only one of endpoint_id and cache_key")
            endpoint_id = cache_key
        self._local_protocol = local_protocol
        self._transceiver = transceiver
        self._handshake_cache = handshake_cache if handshake_cache is not None else HandshakeCache()
        self._endpoint_id = endpoint_id
        self._remote_protocol = None
        self._remote_hash = None
        self._send_protocol = False

    # read-only properties
    @property
    def local_protocol(self):
        return self._local_protocol

    @property
    def transceiver(self):
        return self._transceiver

    @property
    def handshake_cache(self):
        """The cache of verified remote protocols used by this requestor."""
        return self._handshake_cache

    @property
    def endpoint_id(self):
        """Stable identity of the remote endpoint, used as the cache key."""
        if self._endpoint_id is not None:
            return self._endpoint_id
        return self.transceiver.remote_name

    # read/write properties
    @property
    def remote_protocol(self):
        return self._remote_protocol

    @remote_protocol.setter
    def remote_protocol(self, new_remote_protocol):
        # Only updates this requestor's view. The handshake cache is updated
        # exclusively with protocol information verified by a completed
        # handshake; use the cache's set()/invalidate() for explicit control.
        self._remote_protocol = new_remote_protocol

    @property
    def remote_hash(self):
        return self._remote_hash

    @remote_hash.setter
    def remote_hash(self, new_remote_hash):
        self._remote_hash = new_remote_hash

    @property
    def send_protocol(self):
        return self._send_protocol

    @send_protocol.setter
    def send_protocol(self, new_send_protocol):
        self._send_protocol = new_send_protocol

    def invalidate_handshake_cache(self):
        """
        Explicitly drop the verified remote protocol cached for this endpoint.

        The next request re-negotiates the handshake from scratch. Useful when
        the server is known to have changed its protocol.
        """
        self.handshake_cache.invalidate(self.endpoint_id)
        self._remote_protocol = None
        self._remote_hash = None

    def request(self, message_name, request_datum):
        """
        Writes a request message and reads a response or error message.
        """
        # build handshake and call request
        buffer_writer = io.BytesIO()
        buffer_encoder = avro.io.BinaryEncoder(buffer_writer)
        self.write_handshake_request(buffer_encoder)
        self.write_call_request(message_name, request_datum, buffer_encoder)

        # send the handshake and call request; block until call response
        call_request = buffer_writer.getvalue()
        return self.issue_request(call_request, message_name, request_datum)

    def write_handshake_request(self, encoder):
        local_hash = self.local_protocol.md5
        entry = self.handshake_cache.get(self.endpoint_id)
        if entry is not None:
            # verified remote protocol from a previous handshake
            remote_hash = entry.hash
            self._remote_protocol = entry.protocol
            self._remote_hash = entry.hash
        else:
            # never connected: guess the server speaks our protocol
            remote_hash = local_hash
            self._remote_protocol = None
            self._remote_hash = None
        request_datum = {}
        request_datum["clientHash"] = local_hash
        request_datum["serverHash"] = remote_hash
        if self.send_protocol:
            request_datum["clientProtocol"] = str(self.local_protocol)
        HANDSHAKE_REQUESTOR_WRITER.write(request_datum, encoder)

    def write_call_request(self, message_name, request_datum, encoder):
        """
        The format of a call request is:
          * request metadata, a map with values of type bytes
          * the message name, an Avro string, followed by
          * the message parameters. Parameters are serialized according to
            the message's request declaration.
        """
        # request metadata (not yet implemented)
        request_metadata = {}
        META_WRITER.write(request_metadata, encoder)

        # message name
        message = self.local_protocol.messages.get(message_name)
        if message is None:
            raise avro.errors.AvroException(f"Unknown message: {message_name}")
        encoder.write_utf8(message.name)

        # message parameters
        self.write_request(message.request, request_datum, encoder)

    def write_request(self, request_schema, request_datum, encoder):
        datum_writer = avro.io.DatumWriter(request_schema)
        datum_writer.write(request_datum, encoder)

    def _parse_verified_remote(self, handshake_response):
        """
        Parse the server's protocol text and verify that its MD5 digest
        matches the hash the server reported. Nothing is stored until both
        checks pass, so an unparseable protocol or a hash mismatch cannot
        pollute the cache or another session.
        """
        server_protocol_text = handshake_response.get("serverProtocol")
        server_hash = handshake_response.get("serverHash")
        if server_protocol_text is None or server_hash is None:
            raise avro.errors.AvroException("Handshake response is missing the server protocol or its hash")
        try:
            remote_protocol = avro.protocol.parse(server_protocol_text)
        except Exception as e:
            raise avro.errors.AvroException(f"Handshake failure: server sent an unparseable protocol: {e}")
        if remote_protocol.md5 != server_hash:
            raise avro.errors.AvroException("Handshake failure: server protocol MD5 does not match the reported server hash")
        return remote_protocol, server_hash

    def _store_verified_remote(self, remote_protocol, remote_hash):
        """Publish a verified remote protocol to both this requestor and the cache."""
        self.handshake_cache.set(self.endpoint_id, remote_protocol, remote_hash)
        self._remote_protocol = remote_protocol
        self._remote_hash = remote_hash

    def read_handshake_response(self, decoder):
        handshake_response = HANDSHAKE_REQUESTOR_READER.read(decoder)
        match = handshake_response.get("match")
        if match == "BOTH":
            if self._remote_protocol is None:
                # We guessed the server protocol was our own; the server
                # confirmed its hash, so this is now verified information.
                self._store_verified_remote(self.local_protocol, self.local_protocol.md5)
            self.send_protocol = False
            return True
        elif match == "CLIENT":
            if self.send_protocol:
                raise avro.errors.AvroException("Handshake failure.")
            remote_protocol, remote_hash = self._parse_verified_remote(handshake_response)
            self._store_verified_remote(remote_protocol, remote_hash)
            self.send_protocol = False
            return True
        elif match == "NONE":
            if self.send_protocol:
                raise avro.errors.AvroException("Handshake failure.")
            remote_protocol, remote_hash = self._parse_verified_remote(handshake_response)
            self._store_verified_remote(remote_protocol, remote_hash)
            self.send_protocol = True
            return False
        else:
            raise avro.errors.AvroException(f"Unexpected match: {match}")

    def read_call_response(self, message_name, decoder):
        """
        The format of a call response is:
          * response metadata, a map with values of type bytes
          * a one-byte error flag boolean, followed by either:
            o if the error flag is false,
              the message response, serialized per the message's response schema.
            o if the error flag is true,
              the error, serialized per the message's error union schema.
        """
        # response metadata
        META_READER.read(decoder)

        # remote response schema
        remote_message_schema = self.remote_protocol.messages.get(message_name)
        if remote_message_schema is None:
            raise avro.errors.AvroException(f"Unknown remote message: {message_name}")

        # local response schema
        local_message_schema = self.local_protocol.messages.get(message_name)
        if local_message_schema is None:
            raise avro.errors.AvroException(f"Unknown local message: {message_name}")

        # error flag
        if not decoder.read_boolean():
            writers_schema = remote_message_schema.response
            readers_schema = local_message_schema.response
            return self.read_response(writers_schema, readers_schema, decoder)
        else:
            writers_schema = remote_message_schema.errors
            readers_schema = local_message_schema.errors
            datum_reader = avro.io.DatumReader(writers_schema, readers_schema)
            raise avro.errors.AvroRemoteException(datum_reader.read(decoder))

    def read_response(self, writers_schema, readers_schema, decoder):
        datum_reader = avro.io.DatumReader(writers_schema, readers_schema)
        result = datum_reader.read(decoder)
        return result


class Requestor(BaseRequestor):
    def issue_request(self, call_request, message_name, request_datum):
        call_response = self.transceiver.transceive(call_request)

        # process the handshake and call response
        buffer_decoder = avro.io.BinaryDecoder(io.BytesIO(call_response))
        call_response_exists = self.read_handshake_response(buffer_decoder)
        if call_response_exists:
            return self.read_call_response(message_name, buffer_decoder)
        return self.request(message_name, request_datum)


class Responder:
    """Base class for the server side of a protocol interaction."""

    def __init__(self, local_protocol):
        self._local_protocol = local_protocol
        self._local_hash = self.local_protocol.md5
        self._protocol_cache = {}
        self.set_protocol_cache(self.local_hash, self.local_protocol)

    # read-only properties
    @property
    def local_protocol(self):
        return self._local_protocol

    @property
    def local_hash(self):
        return self._local_hash

    @property
    def protocol_cache(self):
        return self._protocol_cache

    # utility functions to manipulate protocol cache
    def get_protocol_cache(self, hash):
        return self.protocol_cache.get(hash)

    def set_protocol_cache(self, hash, protocol):
        self.protocol_cache[hash] = protocol

    def respond(self, call_request):
        """
        Called by a server to deserialize a request, compute and serialize
        a response or error. Compare to 'handle()' in Thrift.
        """
        buffer_reader = io.BytesIO(call_request)
        buffer_decoder = avro.io.BinaryDecoder(buffer_reader)
        buffer_writer = io.BytesIO()
        buffer_encoder = avro.io.BinaryEncoder(buffer_writer)
        error = None
        response_metadata = {}

        try:
            remote_protocol = self.process_handshake(buffer_decoder, buffer_encoder)
            # handshake failure
            if remote_protocol is None:
                return buffer_writer.getvalue()

            # read request using remote protocol
            META_READER.read(buffer_decoder)
            remote_message_name = buffer_decoder.read_utf8()

            # get remote and local request schemas so we can do
            # schema resolution (one fine day)
            remote_message = remote_protocol.messages.get(remote_message_name)
            if remote_message is None:
                fail_msg = f"Unknown remote message: {remote_message_name}"
                raise avro.errors.AvroException(fail_msg)
            local_message = self.local_protocol.messages.get(remote_message_name)
            if local_message is None:
                fail_msg = f"Unknown local message: {remote_message_name}"
                raise avro.errors.AvroException(fail_msg)
            writers_schema = remote_message.request
            readers_schema = local_message.request
            request = self.read_request(writers_schema, readers_schema, buffer_decoder)

            # perform server logic
            try:
                response = self.invoke(local_message, request)
            except avro.errors.AvroRemoteException as e:
                error = e
            except Exception as e:
                error = avro.errors.AvroRemoteException(str(e))

            # write response using local protocol
            META_WRITER.write(response_metadata, buffer_encoder)
            buffer_encoder.write_boolean(error is not None)
            if error is None:
                writers_schema = local_message.response
                self.write_response(writers_schema, response, buffer_encoder)
            else:
                writers_schema = local_message.errors
                self.write_error(writers_schema, error, buffer_encoder)
        except avro.errors.AvroException as e:
            error = avro.errors.AvroRemoteException(str(e))
            buffer_encoder = avro.io.BinaryEncoder(io.BytesIO())
            META_WRITER.write(response_metadata, buffer_encoder)
            buffer_encoder.write_boolean(True)
            self.write_error(SYSTEM_ERROR_SCHEMA, error, buffer_encoder)
        return buffer_writer.getvalue()

    def process_handshake(self, decoder, encoder):
        handshake_request = HANDSHAKE_RESPONDER_READER.read(decoder)
        handshake_response = {}

        # determine the remote protocol
        client_hash = handshake_request.get("clientHash")
        client_protocol = handshake_request.get("clientProtocol")
        remote_protocol = self.get_protocol_cache(client_hash)
        if remote_protocol is None and client_protocol is not None:
            remote_protocol = avro.protocol.parse(client_protocol)
            self.set_protocol_cache(client_hash, remote_protocol)

        # evaluate remote's guess of the local protocol
        server_hash = handshake_request.get("serverHash")
        if self.local_hash == server_hash:
            if remote_protocol is None:
                handshake_response["match"] = "NONE"
            else:
                handshake_response["match"] = "BOTH"
        else:
            if remote_protocol is None:
                handshake_response["match"] = "NONE"
            else:
                handshake_response["match"] = "CLIENT"

        if handshake_response["match"] != "BOTH":
            handshake_response["serverProtocol"] = str(self.local_protocol)
            handshake_response["serverHash"] = self.local_hash

        HANDSHAKE_RESPONDER_WRITER.write(handshake_response, encoder)
        return remote_protocol

    def invoke(self, local_message, request):
        """
        Actual work done by server: cf. handler in thrift.
        """

    def read_request(self, writers_schema, readers_schema, decoder):
        datum_reader = avro.io.DatumReader(writers_schema, readers_schema)
        return datum_reader.read(decoder)

    def write_response(self, writers_schema, response_datum, encoder):
        datum_writer = avro.io.DatumWriter(writers_schema)
        datum_writer.write(response_datum, encoder)

    def write_error(self, writers_schema, error_exception, encoder):
        datum_writer = avro.io.DatumWriter(writers_schema)
        datum_writer.write(str(error_exception), encoder)


#
# Utility classes
#


class FramedReader:
    """Wrapper around a file-like object to read framed data."""

    def __init__(self, reader):
        self._reader = reader

    # read-only properties
    @property
    def reader(self):
        return self._reader

    def read_framed_message(self):
        message = []
        while True:
            buffer = io.BytesIO()
            buffer_length = self._read_buffer_length()
            if buffer_length == 0:
                return b"".join(message)
            while buffer.tell() < buffer_length:
                chunk = self.reader.read(buffer_length - buffer.tell())
                if chunk == b"":
                    raise avro.errors.ConnectionClosedException("Reader read 0 bytes.")
                buffer.write(chunk)
            message.append(buffer.getvalue())

    def _read_buffer_length(self):
        read = self.reader.read(BUFFER_HEADER_LENGTH)
        if read == b"":
            raise avro.errors.ConnectionClosedException("Reader read 0 bytes.")
        return BIG_ENDIAN_INT_STRUCT.unpack(read)[0]


class FramedWriter:
    """Wrapper around a file-like object to write framed data."""

    def __init__(self, writer):
        self._writer = writer

    # read-only properties
    @property
    def writer(self):
        return self._writer

    def write_framed_message(self, message):
        message_length = len(message)
        total_bytes_sent = 0
        while message_length - total_bytes_sent > 0:
            if message_length - total_bytes_sent > BUFFER_SIZE:
                buffer_length = BUFFER_SIZE
            else:
                buffer_length = message_length - total_bytes_sent
            self.write_buffer(message[total_bytes_sent : (total_bytes_sent + buffer_length)])
            total_bytes_sent += buffer_length
        # A message is always terminated by a zero-length buffer.
        self.write_buffer_length(0)

    def write_buffer(self, chunk):
        buffer_length = len(chunk)
        self.write_buffer_length(buffer_length)
        self.writer.write(chunk)

    def write_buffer_length(self, n):
        self.writer.write(BIG_ENDIAN_INT_STRUCT.pack(n))


#
# Transceiver Implementations
#


class HTTPTransceiver:
    """
    A simple HTTP-based transceiver implementation.
    Useful for clients but not for servers
    """

    def __init__(self, host, port, req_resource="/"):
        self.req_resource = req_resource
        self.conn = http.client.HTTPConnection(host, port)
        self.conn.connect()
        self.remote_name = self.conn.sock.getsockname()

    def transceive(self, request):
        self.write_framed_message(request)
        result = self.read_framed_message()
        return result

    def read_framed_message(self):
        response = self.conn.getresponse()
        response_reader = FramedReader(response)
        framed_message = response_reader.read_framed_message()
        response.read()  # ensure we're ready for subsequent requests
        return framed_message

    def write_framed_message(self, message):
        req_method = "POST"
        req_headers = {"Content-Type": "avro/binary"}

        req_body_buffer = FramedWriter(io.BytesIO())
        req_body_buffer.write_framed_message(message)
        req_body = req_body_buffer.writer.getvalue()

        self.conn.request(req_method, self.req_resource, req_body, req_headers)

    def close(self):
        self.conn.close()


#
# Server Implementations (none yet)
#

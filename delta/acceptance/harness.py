#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Owned process evidence and a one-shot external response-loss proxy."""

import hashlib
import http.client
import http.server
import json
import os
import re
from pathlib import Path
import shlex
import socket
import threading
import time
import urllib.parse


def owned_directory(path):
    path = Path(path)
    path.mkdir(mode=0o700, parents=True, exist_ok=False)
    return path


def credentials(path):
    values = {}
    for line in Path(path).read_text().splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        key, value = line.split("=", 1)
        parsed = shlex.split(value)
        if len(parsed) != 1:
            raise ValueError("Invalid private credential file")
        values[key.strip()] = parsed[0]
    required = ("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD")
    if any(not values.get(key) for key in required):
        raise ValueError("Missing private credentials")
    return tuple(values[key] for key in required)


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    os.chmod(path, 0o600)


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def manifests_equal(before, after):
    return sorted(before, key=lambda row: row["key"]) == sorted(
        after, key=lambda row: row["key"]
    )


def operation(body):
    try:
        data = json.loads(body)
        encoded = data.get("properties", {}).get("delta.registration.command")
        for update in data.get("updates", []):
            if update.get("property") == "delta.registration.command":
                encoded = update.get("value")
        return json.loads(encoded).get("operation") if encoded else None
    except (ValueError, TypeError, AttributeError):
        return None


class RecordingProxy:
    """Forward once; optionally drop a completed response or an unforwarded publish.

    Events omit request/response bodies because catalog requests contain credentials.
    A blocked-before request is explicitly not sent; it is never called completed.
    """

    def __init__(self, upstream, events):
        self.upstream = urllib.parse.urlsplit(upstream)
        self.events = Path(events)
        self.lock = threading.Lock()
        self.fault = None
        self.active = 0
        self.unresolved = 0
        self.paused = False
        self.drain_proven = False
        self.drained = threading.Condition(self.lock)
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.forward()

            def do_POST(self):
                self.forward()

            def do_PUT(self):
                self.forward()

            def do_DELETE(self):
                self.forward()

            def forward(self):
                expected_length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(expected_length)
                if len(body) != expected_length:
                    self.close_connection = True
                    return
                op = operation(body)
                fault = None
                with owner.lock:
                    if owner.fault and owner.fault[0] == op:
                        _, fault, persistent = owner.fault
                        if not persistent:
                            owner.fault = None
                event = {
                    "method": self.command,
                    "path": self.path,
                    "operation": op,
                    "started_ns": time.time_ns(),
                    "completion": "unknown",
                }
                connection = http.client.HTTPConnection(
                    owner.upstream.hostname, owner.upstream.port, timeout=180
                )
                try:
                    if fault == "drop-before":
                        event.update(completion="not-forwarded", downstream="dropped")
                        return
                    headers = {
                        k: v
                        for k, v in self.headers.items()
                        if k.lower() not in ("host", "connection", "content-length")
                    }
                    connection.request(self.command, self.path, body, headers)
                    response = connection.getresponse()
                    result = response.read()
                    event.update(
                        completion="confirmed",
                        upstream_status=response.status,
                        response_sha256=hashlib.sha256(result).hexdigest(),
                        completed_ns=time.time_ns(),
                    )
                    if fault == "drop-after":
                        event["downstream"] = "dropped"
                        return
                    self.send_response(response.status)
                    for key, value in response.getheaders():
                        if key.lower() not in (
                            "transfer-encoding",
                            "connection",
                            "content-length",
                        ):
                            self.send_header(key, value)
                    self.send_header("Content-Length", str(len(result)))
                    self.end_headers()
                    self.wfile.write(result)
                    event["downstream"] = "sent"
                except (OSError, http.client.HTTPException) as error:
                    event["transport_error"] = type(error).__name__
                finally:
                    connection.close()
                    with owner.lock:
                        with owner.events.open("a") as stream:
                            stream.write(json.dumps(event, sort_keys=True) + "\n")
                        if event["completion"] == "unknown":
                            owner.unresolved += 1
                    self.close_connection = True
                    try:
                        self.connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass

            def log_message(self, *_):
                pass

        class Server(http.server.ThreadingHTTPServer):
            def process_request(self, request, address):
                with owner.drained:
                    owner.active += 1
                    owner.drained.notify_all()
                try:
                    super().process_request(request, address)
                except BaseException:
                    with owner.drained:
                        owner.active -= 1
                        owner.drained.notify_all()
                    raise

            def process_request_thread(self, request, address):
                try:
                    super().process_request_thread(request, address)
                finally:
                    with owner.drained:
                        owner.active -= 1
                        owner.drained.notify_all()

        self.server_factory = lambda port: Server(("127.0.0.1", port), Handler)
        self.server = self.server_factory(0)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.port = self.server.server_port

    def arm(self, operation_name, mode, persistent=False):
        if mode not in ("drop-before", "drop-after"):
            raise ValueError("Unknown fault mode")
        with self.lock:
            if self.fault:
                raise ValueError("Previous fault has not fired")
            self.drain_proven = False
            self.fault = operation_name, mode, persistent

    def wait_drained(self, timeout):
        self.drain_proven = False
        # First stop acceptance, including sockets not yet parsed by a handler.
        self.server.shutdown()
        self.thread.join()
        with self.drained:
            if not self.drained.wait_for(lambda: self.active == 0, timeout=timeout):
                raise TimeoutError("Original upstream requests have not drained")
        self.server.server_close()
        self.paused = True
        if self.unresolved:
            raise RuntimeError(
                "Unknown upstream completion remains unresolved after local drain"
            )
        self.drain_proven = True

    def disarm(self):
        with self.lock:
            if self.active or self.unresolved or not self.drain_proven:
                raise RuntimeError(
                    "Cannot disarm active or unresolved upstream requests"
                )
            self.fault = None
            self.drain_proven = False
        if self.paused:
            self.server = self.server_factory(self.port)
            self.thread = threading.Thread(target=self.server.serve_forever)
            self.paused = False
            self.thread.start()

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        if not self.paused:
            self.server.shutdown()
            self.thread.join()
            self.server.server_close()


def catalog_close_completed(log, catalog):
    """Recognize existing production cleanup evidence, rejecting competing retirements."""
    removed = False
    for line in log.splitlines():
        if "Removed catalog cache entry" in line:
            if removed or catalog not in line:
                raise RuntimeError(
                    "Concurrent catalog retirement makes close evidence ambiguous"
                )
            removed = True
        if removed and "Failed to close catalog" in line:
            raise RuntimeError("Catalog close failed")
        if removed and "Released ClassLoader for key" in line:
            count = re.search(r"refCount=(\d+)", line)
            if "lakehouse-delta" not in line or not count or int(count.group(1)) <= 0:
                raise RuntimeError("No surviving shared provider classloader reference")
            return True
    return False

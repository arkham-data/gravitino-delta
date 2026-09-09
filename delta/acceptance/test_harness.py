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

from contextlib import closing
import http.client
import http.server
import json
import pathlib
import socket
import sqlite3
import tempfile
import threading
import unittest

import harness
from s3_objects import OwnedObjects
from run import COLUMN_SNAPSHOT_QUERY


class TestHarness(unittest.TestCase):
    def test_column_snapshot_selects_current_live_version_after_replacement(self):
        with closing(sqlite3.connect(":memory:")) as db:
            db.executescript("""
                CREATE TABLE catalog_meta(catalog_id INT, catalog_name TEXT, deleted_at INT);
                CREATE TABLE table_meta(table_id INT, table_name TEXT, current_version INT, deleted_at INT);
                CREATE TABLE table_column_version_info(catalog_id INT, table_id INT,
                    column_name TEXT, column_id INT, table_version INT, column_op_type INT, deleted_at INT);
                INSERT INTO catalog_meta VALUES(1, 'catalog', 0);
                INSERT INTO table_meta VALUES(2, 'same', 4, 0);
                INSERT INTO table_column_version_info VALUES
                    (1,2,'id',10,2,1,0), (1,2,'id',10,4,3,0),
                    (1,2,'id',20,4,1,0), (1,2,'future',30,6,1,0),
                    (1,2,'stable',40,2,1,0);
            """)
            self.assertEqual(
                db.execute(COLUMN_SNAPSHOT_QUERY).fetchall(),
                [
                    ("catalog", "same", "id", 20, 4),
                    ("catalog", "same", "stable", 40, 2),
                ],
            )

    def test_s3_refuses_unowned_keys_before_network(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "credentials"
            path.write_text("MINIO_ROOT_USER=a\nMINIO_ROOT_PASSWORD=b\n")
            store = OwnedObjects({"credentialsFile": str(path)}, "owned")
            with self.assertRaises(ValueError):
                store.request("PUT", "owned_other/table", data=b"bad")
            with self.assertRaises(ValueError):
                OwnedObjects({}, "")

    def test_s3_rejects_deletion_and_path_traversal(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "credentials"
            path.write_text("MINIO_ROOT_USER=a\nMINIO_ROOT_PASSWORD=b\n")
            store = OwnedObjects({"credentialsFile": str(path)}, "owned")
            for method, key in [("DELETE", "owned/table"), ("PUT", "owned/../other")]:
                with self.assertRaises(ValueError):
                    store.request(method, key)

    def test_persistent_fault_blocks_every_attempt_and_drains_before_disarm(self):
        with tempfile.TemporaryDirectory() as directory:
            events = pathlib.Path(directory) / "events"
            with harness.RecordingProxy("http://127.0.0.1:9", events) as proxy:
                proxy.arm("PUBLISH", "drop-before", persistent=True)
                for _ in range(2):
                    connection = http.client.HTTPConnection("127.0.0.1", proxy.port)
                    body = json.dumps(
                        {
                            "updates": [
                                {
                                    "property": "delta.registration.command",
                                    "value": json.dumps({"operation": "PUBLISH"}),
                                }
                            ]
                        }
                    )
                    connection.request("PUT", "/table", body)
                    with self.assertRaises(http.client.RemoteDisconnected):
                        connection.getresponse()
                    connection.close()
                proxy.wait_drained(timeout=5)
                proxy.disarm()
                rows = [json.loads(line) for line in events.read_text().splitlines()]
                self.assertEqual(len(rows), 2)
                self.assertTrue(
                    all(row["completion"] == "not-forwarded" for row in rows)
                )

    def test_catalog_close_requires_ordered_observable_completion(self):
        removed = "DEBUG [cleanup] Removed catalog cache entry, identifier=owned.primary, cause=EXPLICIT\n"
        release = "DEBUG [cleanup] Released ClassLoader for key ClassLoaderKey{provider='lakehouse-delta', properties={}}, refCount=2\n"
        self.assertFalse(harness.catalog_close_completed(release, "primary"))
        self.assertFalse(harness.catalog_close_completed(removed, "primary"))
        self.assertTrue(harness.catalog_close_completed(removed + release, "primary"))
        for mismatched in (
            release.replace("[cleanup]", "[other]"),
            release.replace("properties={}", "properties={different=true}"),
        ):
            with self.subTest(release=mismatched), self.assertRaises(RuntimeError):
                harness.catalog_close_completed(removed + mismatched, "primary")
        with self.assertRaises(RuntimeError):
            harness.catalog_close_completed(
                removed + "WARN Failed to close catalog\n" + release, "primary"
            )
        with self.assertRaises(RuntimeError):
            harness.catalog_close_completed(
                removed + removed.replace("primary", "other") + release, "primary"
            )

    def test_partial_body_connection_prevents_drain_and_disarm(self):
        with tempfile.TemporaryDirectory() as directory:
            with harness.RecordingProxy(
                "http://127.0.0.1:9", pathlib.Path(directory) / "events"
            ) as proxy:
                connection = socket.create_connection(("127.0.0.1", proxy.port))
                try:
                    connection.sendall(
                        b"POST /tables HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\n{"
                    )
                    with proxy.drained:
                        self.assertTrue(
                            proxy.drained.wait_for(lambda: proxy.active > 0, timeout=3)
                        )
                    with self.assertRaises(TimeoutError):
                        proxy.wait_drained(timeout=0.05)
                    with self.assertRaises(RuntimeError):
                        proxy.disarm()
                finally:
                    connection.close()
                proxy.wait_drained(timeout=3)
                proxy.disarm()

    def test_completed_local_handler_cannot_resolve_unknown_upstream(self):
        entered = threading.Event()
        release = threading.Event()

        class Upstream(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers["Content-Length"]))
                entered.set()
                release.wait(5)
                self.close_connection = True

            def log_message(self, *_):
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                events = pathlib.Path(directory) / "events"
                with harness.RecordingProxy(
                    f"http://127.0.0.1:{server.server_port}", events
                ) as proxy:
                    connection = http.client.HTTPConnection("127.0.0.1", proxy.port)
                    connection.request("POST", "/tables", "{}")
                    self.assertTrue(entered.wait(3))
                    with self.assertRaises(TimeoutError):
                        proxy.wait_drained(timeout=0.05)
                    release.set()
                    with self.assertRaises(http.client.RemoteDisconnected):
                        connection.getresponse()
                    connection.close()
                    with proxy.drained:
                        self.assertTrue(
                            proxy.drained.wait_for(lambda: proxy.active == 0, timeout=3)
                        )
                    with self.assertRaisesRegex(RuntimeError, "Unknown upstream"):
                        proxy.wait_drained(timeout=3)
                    with self.assertRaises(RuntimeError):
                        proxy.disarm()
                    self.assertEqual(
                        json.loads(events.read_text())["completion"], "unknown"
                    )
        finally:
            release.set()
            server.shutdown()
            thread.join()
            server.server_close()

    def test_disarm_requires_successful_drain_even_after_active_request_exits(self):
        with tempfile.TemporaryDirectory() as directory:
            with harness.RecordingProxy(
                "http://127.0.0.1:9", pathlib.Path(directory) / "events"
            ) as proxy:
                proxy.arm("PUBLISH", "drop-before", persistent=True)
                with self.assertRaises(RuntimeError):
                    proxy.disarm()
                proxy.wait_drained(timeout=3)
                proxy.disarm()

    def test_output_refuses_existing_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(FileExistsError):
                harness.owned_directory(pathlib.Path(directory))

    def test_credentials_are_parsed_without_shell_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "credentials"
            path.write_text(
                'MINIO_ROOT_USER=abc\nMINIO_ROOT_PASSWORD="$(touch /invalid)"\n'
            )
            self.assertEqual(harness.credentials(path), ("abc", "$(touch /invalid)"))
            path.write_text("MINIO_ROOT_USER=abc\n")
            with self.assertRaises(ValueError):
                harness.credentials(path)

    def test_manifest_compares_content_not_only_names(self):
        self.assertFalse(
            harness.manifests_equal(
                [{"key": "a", "sha256": "one"}], [{"key": "a", "sha256": "two"}]
            )
        )
        self.assertTrue(
            harness.manifests_equal(
                [{"key": "b", "sha256": "two"}, {"key": "a", "sha256": "one"}],
                [{"key": "a", "sha256": "one"}, {"key": "b", "sha256": "two"}],
            )
        )

    def test_response_loss_records_upstream_completion_and_never_replays(self):
        calls = []

        class Upstream(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                calls.append(self.rfile.read(int(self.headers["Content-Length"])))
                self.send_response(201)
                self.end_headers()
                self.wfile.write(b'{"committed":true}')

            def log_message(self, *_):
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                events = pathlib.Path(directory) / "events.jsonl"
                with harness.RecordingProxy(
                    f"http://127.0.0.1:{server.server_port}", events
                ) as proxy:
                    proxy.arm("RESERVE", "drop-after")
                    conn = http.client.HTTPConnection("127.0.0.1", proxy.port)
                    body = json.dumps(
                        {
                            "properties": {
                                "delta.registration.command": json.dumps(
                                    {"operation": "RESERVE"}
                                )
                            }
                        }
                    )
                    conn.request(
                        "POST", "/tables", body, {"Content-Type": "application/json"}
                    )
                    with self.assertRaises(http.client.RemoteDisconnected):
                        conn.getresponse()
                    conn.close()
                    self.assertEqual(len(calls), 1)
                    record = json.loads(events.read_text().splitlines()[-1])
                    self.assertEqual(record["upstream_status"], 201)
                    self.assertEqual(record["completion"], "confirmed")
                    self.assertEqual(record["downstream"], "dropped")
                    self.assertNotIn("body", record)
        finally:
            server.shutdown()
            thread.join()
            server.server_close()


if __name__ == "__main__":
    unittest.main()

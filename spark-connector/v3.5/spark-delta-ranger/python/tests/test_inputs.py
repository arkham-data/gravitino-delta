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

"""Classic input transport validation; live tests qualify native execution."""
import importlib
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from py4j.java_gateway import JavaGateway, GatewayParameters, launch_gateway
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


class JavaMap(dict):
    def put(self, key, value):
        self[key] = value


class TestInputs(unittest.TestCase):
    def setUp(self):
        self.api = importlib.import_module("gravitino_delta")
        self.spark = object.__new__(SparkSession)
        self.spark._jsparkSession = object()
        self.spark._sc = SimpleNamespace(_jvm=MagicMock())
        self.jvm = self.spark._sc._jvm
        self.entry = self.jvm.org.apache.gravitino.spark.connector.delta.ranger.GravitinoJdbcInput
        self.entry.apiVersion.return_value = 1
        self.jvm.java.util.HashMap.side_effect = JavaMap

    def open(self):
        return self.api.GravitinoJdbcInput.open(self.spark, lifetime_seconds=1200)

    def test_open_read_copy_and_close_on_exception(self):
        options = {"dbtable": "(select 1) t", "password": "fixture"}
        with patch("gravitino_delta.inputs.DataFrame") as frame:
            with self.assertRaisesRegex(ValueError, "body"):
                with self.open() as reader:
                    self.assertIs(reader.read(options), frame.return_value)
                    options["dbtable"] = "changed"
                    self.assertEqual("(select 1) t", self.entry.open.return_value.read.call_args.args[0]["dbtable"])
                    frame.assert_called_once_with(self.entry.open.return_value.read.return_value, self.spark)
                    raise ValueError("body")
        self.entry.open.assert_called_once_with(self.spark._jsparkSession, 1200)
        self.entry.open.return_value.close.assert_called_once_with()
        reader.close()
        self.entry.open.return_value.close.assert_called_once_with()
        with self.assertRaisesRegex(RuntimeError, "closed"):
            reader.read({})

    def test_invalid_session_and_lifetime_before_jvm(self):
        for spark in [object(), type("SparkSession", (), {"__module__": "pyspark.sql.connect.session"})()]:
            with self.assertRaisesRegex(TypeError, "classic"):
                self.api.GravitinoJdbcInput.open(spark, lifetime_seconds=1200)
        for seconds in [True, 1.5, "1", None, 0, 86401]:
            with self.assertRaises((TypeError, ValueError)):
                self.api.GravitinoJdbcInput.open(self.spark, lifetime_seconds=seconds)
        self.entry.open.assert_not_called()

    def test_options_validate_before_native_read(self):
        reader = self.open()
        for options in [[], {1: "x"}, {"a": 1}, {"url": "x", "URL": "y"}]:
            with self.assertRaises((TypeError, ValueError)):
                reader.read(options)
        self.entry.open.return_value.read.assert_not_called()

    def test_missing_and_wrong_protocol(self):
        for version in [None, True, 0, 2]:
            self.entry.apiVersion.return_value = version
            with self.assertRaisesRegex(RuntimeError, "version"):
                self.open()
        self.entry.open.assert_not_called()
        path = self.jvm.org.apache.gravitino.spark.connector.delta.ranger
        path.GravitinoJdbcInput = SimpleNamespace(apiVersion=lambda: 1)
        with self.assertRaisesRegex(RuntimeError, "open API is missing"):
            self.open()
        path.GravitinoJdbcInput = SimpleNamespace()
        with self.assertRaisesRegex(RuntimeError, "apiVersion API is missing"):
            self.open()

    def test_native_failures_preserved_without_replay(self):
        error = RuntimeError("native denial")
        for method in ["apiVersion", "open"]:
            with self.subTest(method=method):
                member = getattr(self.entry, method)
                member.reset_mock()
                member.side_effect = error
                with self.assertRaises(RuntimeError) as caught:
                    self.open()
                self.assertIs(error, caught.exception)
                self.assertEqual(1, member.call_count)
                member.side_effect = None
        reader = self.open()
        for method in ["read", "close"]:
            member = getattr(self.entry.open.return_value, method)
            member.side_effect = error
            with self.assertRaises(RuntimeError) as caught:
                reader.read({}) if method == "read" else reader.close()
            self.assertIs(error, caught.exception)
            self.assertEqual(1, member.call_count)
        reader.close()
        self.assertEqual(1, self.entry.open.return_value.close.call_count)

    def test_real_gateway_missing_entry_and_method(self):
        java = str(Path(os.environ["JAVA_HOME"]) / "bin/java") if "JAVA_HOME" in os.environ else "java"
        port, process = launch_gateway(java_path=java, die_on_exit=True, return_proc=True)
        gateway = JavaGateway(gateway_parameters=GatewayParameters(port=port))
        try:
            self.spark._sc._jvm = gateway.jvm
            with self.assertRaisesRegex(RuntimeError, "apiVersion API is missing"):
                self.open()
            entry = gateway.jvm.java.lang.System
            entry.apiVersion = lambda: 1
            self.spark._sc._jvm = self.jvm
            self.jvm.org.apache.gravitino.spark.connector.delta.ranger.GravitinoJdbcInput = entry
            with self.assertRaisesRegex(RuntimeError, "open API is missing"):
                self.open()
        finally:
            gateway.shutdown()
            process.stdin.close()
            process.wait(timeout=10)
            for stream in (process.stdin, process.stdout, process.stderr):
                if stream is not None:
                    stream.close()

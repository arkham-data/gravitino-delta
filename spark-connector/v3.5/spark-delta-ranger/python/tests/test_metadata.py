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

"""Finite metadata transport, detached results, and failure semantics."""
import importlib
import json
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType


class TestMetadata(unittest.TestCase):
    def setUp(self):
        self.api = importlib.import_module("gravitino_delta")
        self.spark = object.__new__(SparkSession)
        self.spark._jsparkSession = object()
        self.spark._sc = SimpleNamespace(_jvm=MagicMock())
        self.path = self.spark._sc._jvm.org.apache.gravitino.spark.connector.delta.ranger
        self.entry = self.path.GravitinoDeltaTable
        self.entry.apiVersion.return_value = 1
        self.schema = StructType([StructField("id", LongType(), False, {"comment": "clé 雪"})])
        self.value = {"schemaJson": self.schema.json(), "properties": {"primary_key": "id"},
                      "partitionColumns": ["id"]}
        self.entry.metadataJson.return_value = json.dumps(self.value)

    def read(self, spark=None, name="a.db.t"):
        return self.api.GravitinoDeltaTable.metadata(self.spark if spark is None else spark, name)

    def test_result_is_detached_and_properties_are_immutable(self):
        self.assertTrue(hasattr(self.api.GravitinoDeltaTable, "metadata"), "metadata API is missing")
        result = self.read()
        self.assertIsInstance(result, self.api.GravitinoDeltaMetadata)
        self.assertEqual(self.schema, result.schema)
        self.assertEqual({"primary_key": "id"}, result.properties)
        self.assertEqual(("id",), result.partition_columns)
        with self.assertRaises(TypeError):
            result.properties["primary_key"] = "changed"
        result.schema.fields[0].metadata["comment"] = "changed"
        result.schema.add("other", "string")
        self.assertEqual(self.schema, self.read().schema)
        self.assertEqual({"schema", "properties", "partition_columns"}, set(vars(result)))
        self.entry.metadataJson.assert_called_with(self.spark._jsparkSession, "a.db.t")

    def test_null_alone_means_authorized_absence(self):
        self.entry.metadataJson.return_value = None
        self.assertIsNone(self.read())
        for raw in ["null", "", "false", "[]", 0, False, {}, "not-json"]:
            self.entry.metadataJson.return_value = raw
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                self.read()

    def test_malformed_metadata_rejects_without_returning_partial_result(self):
        invalid = [{}, dict(self.value, location="private"), dict(self.value, schemaJson="{}"),
                   dict(self.value, schemaJson='"long"'),
                   dict(self.value, schemaJson='{"type":"struct","fields":[null]}'),
                   dict(self.value, properties={"primary_key": 1}),
                   dict(self.value, properties={"location": "private"}),
                   dict(self.value, partitionColumns="id"), dict(self.value, partitionColumns=[1]),
                   dict(self.value, partitionColumns=["missing"]),
                   dict(self.value, partitionColumns=["id", "id"])]
        for value in invalid:
            self.entry.metadataJson.return_value = json.dumps(value)
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.read()

    def test_connect_and_invalid_names_reject_before_metadata_call(self):
        with self.assertRaisesRegex(TypeError, "Connect"):
            self.read(spark=object())
        for name, error in [(None, TypeError), (1, TypeError), ("", ValueError), ("  ", ValueError)]:
            with self.subTest(name=name), self.assertRaises(error):
                self.read(name=name)
        self.entry.metadataJson.assert_not_called()

    def test_missing_api_and_invocation_failures_remain_distinct(self):
        self.path.GravitinoDeltaTable = SimpleNamespace(apiVersion=lambda: 1)
        with self.assertRaisesRegex(RuntimeError, "metadata.*missing"):
            self.read()
        self.path.GravitinoDeltaTable = self.entry
        failure = RuntimeError("visibility denied")
        self.entry.metadataJson.side_effect = failure
        with self.assertRaises(RuntimeError) as caught:
            self.read()
        self.assertIs(caught.exception, failure)
        self.entry.metadataJson.assert_called_once()
        self.entry.apiVersion.return_value = 2
        with self.assertRaisesRegex(RuntimeError, "version"):
            self.read()


if __name__ == "__main__":
    unittest.main()

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

"""Transport-only facade tests; installed policy behavior is exercised by the live gate."""
import importlib
import os
from pathlib import Path
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from pyspark.sql import Column, DataFrame, SparkSession
from py4j.java_gateway import JavaGateway, GatewayParameters, launch_gateway
from pyspark.sql.types import StructType, StructField, LongType, StringType, ArrayType, MapType

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


class JavaMap(dict):
    def put(self, key, value):
        self[key] = value


class JavaList(list):
    def add(self, value):
        self.append(value)


class TestTables(unittest.TestCase):
    def setUp(self):
        self.api = importlib.import_module("gravitino_delta.tables")
        self.spark = object.__new__(SparkSession)
        self.spark._jsparkSession = object()
        self.spark._sc = SimpleNamespace(_jvm=MagicMock())
        self.jvm = self.spark._sc._jvm
        self.entry = self.jvm.org.apache.gravitino.spark.connector.delta.ranger.GravitinoDeltaTable
        self.entry.apiVersion.return_value = 1
        self.jtable = self.entry.forName.return_value
        self.jtable.alias.return_value = self.jtable
        self.jbuilder = self.jtable.merge.return_value
        for method in ["whenMatchedUpdate", "whenMatchedDelete", "whenNotMatchedInsert", "whenMatchedUpdateAll", "whenNotMatchedInsertAll"]:
            getattr(self.jbuilder, method).return_value = self.jbuilder
        self.jvm.java.util.HashMap.side_effect = lambda: JavaMap()
        self.source = object.__new__(DataFrame)
        self.source._jdf = object()
        self.source._session = self.spark
        self.column = object.__new__(Column)
        self.column._jc = object()
        self.table = self.api.GravitinoDeltaTable.forName(self.spark, "c.d.target")

    def test_create_uses_native_schema_codec_and_copies_arguments(self):
        self.spark._jsparkSession = MagicMock()
        self.jvm.java.util.ArrayList.side_effect = JavaList
        schema = StructType([StructField("id", LongType(), False, {"comment": "clé '雪'"}),
                             StructField("tags", ArrayType(StringType(), True)),
                             StructField("attrs", MapType(StringType(), StringType(), True))])
        partitions = ["id"]
        properties = {"primary_key": "id", "delta.enableChangeDataFeed": "true"}
        name = "`a`.`db`.`ta``ble`"
        self.assertIsNone(self.api.GravitinoDeltaTable.create(
            self.spark, name, schema, partition_by=partitions, properties=properties,
            comment="table '雪'", if_not_exists=True))
        self.spark._jsparkSession.parseDataType.assert_called_once_with(schema.json())
        args = self.entry.create.call_args.args
        self.assertEqual((self.spark._jsparkSession, name, self.spark._jsparkSession.parseDataType.return_value,
                          ["id"], dict(properties), "table '雪'", True), args)
        partitions.append("changed")
        properties["primary_key"] = "changed"
        self.assertEqual(["id"], args[3])
        self.assertEqual("id", args[4]["primary_key"])

    def test_create_validates_types_before_gateway_execution(self):
        schema = StructType([StructField("id", LongType())])
        invalid = [({"schema": "id BIGINT"}, TypeError), ({"spark": object()}, TypeError),
                   ({"partition_by": "id"}, TypeError), ({"partition_by": [1]}, TypeError),
                   ({"partition_by": [""]}, ValueError), ({"partition_by": {"id"}}, TypeError),
                   ({"properties": []}, TypeError), ({"properties": {"x": 1}}, TypeError),
                   ({"properties": {1: "x"}}, TypeError), ({"comment": 1}, TypeError),
                   ({"if_not_exists": 1}, TypeError), ({"name": ""}, ValueError)]
        for overrides, error in invalid:
            arguments = dict(spark=self.spark, name="a.db.t", schema=schema)
            arguments.update(overrides)
            with self.subTest(overrides=overrides), self.assertRaises(error):
                self.api.GravitinoDeltaTable.create(**arguments)
        self.entry.create.assert_not_called()

    def test_create_missing_api_is_separate_from_native_execution_failure(self):
        self.spark._jsparkSession = MagicMock()
        self.jvm.java.util.ArrayList.side_effect = JavaList
        schema = StructType([StructField("id", LongType())])
        for version in [0, 2, None, True]:
            self.entry.apiVersion.return_value = version
            with self.assertRaisesRegex(RuntimeError, "API"):
                self.api.GravitinoDeltaTable.create(self.spark, "a.db.t", schema)
        self.entry.apiVersion.return_value = 1
        entry_path = self.jvm.org.apache.gravitino.spark.connector.delta.ranger
        entry_path.GravitinoDeltaTable = SimpleNamespace(apiVersion=lambda: 1)
        with self.assertRaisesRegex(RuntimeError, "create.*missing"):
            self.api.GravitinoDeltaTable.create(self.spark, "a.db.t", schema)
        entry_path.GravitinoDeltaTable = self.entry
        error = RuntimeError("native committed; publication status needed")
        self.entry.create.side_effect = error
        with self.assertRaises(RuntimeError) as caught:
            self.api.GravitinoDeltaTable.create(self.spark, "a.db.t", schema)
        self.assertIs(caught.exception, error)
        self.entry.create.assert_called_once()
        self.assertEqual(([], {}, None, False), self.entry.create.call_args.args[3:])

    def test_real_py4j_missing_companion_and_missing_create_lookup(self):
        java = str(Path(os.environ["JAVA_HOME"]) / "bin" / "java") if "JAVA_HOME" in os.environ else "java"
        port, process = launch_gateway(java_path=java, die_on_exit=True, return_proc=True)
        gateway = JavaGateway(gateway_parameters=GatewayParameters(port=port))
        try:
            self.spark._sc._jvm = gateway.jvm
            schema = StructType([StructField("id", LongType())])
            with self.assertRaisesRegex(RuntimeError, "companion API is missing"):
                self.api.GravitinoDeltaTable.create(self.spark, "a.db.t", schema)
            # A real JavaClass performs the missing static method lookup in the JVM.
            # Only the protocol value is scripted; no fake JavaMember is invoked.
            entry = gateway.jvm.java.lang.System
            entry.apiVersion = lambda: 1
            self.spark._sc._jvm = self.jvm
            self.jvm.org.apache.gravitino.spark.connector.delta.ranger.GravitinoDeltaTable = entry
            with self.assertRaisesRegex(RuntimeError, "create API is missing"):
                self.api.GravitinoDeltaTable.create(self.spark, "a.db.t", schema)
        finally:
            gateway.shutdown()
            process.stdin.close()
            process.wait(timeout=10)
            for stream in (process.stdin, process.stdout, process.stderr):
                if stream is not None:
                    stream.close()

    def test_factory_alias_and_merge_preserve_actual_objects(self):
        self.entry.forName.assert_called_once_with(self.spark._jsparkSession, "c.d.target")
        alias = self.table.alias("t")
        self.assertIs(alias._spark, self.spark)
        for condition, expected in [("t.id=s.id", "t.id=s.id"), (self.column, self.column._jc)]:
            alias.merge(self.source, condition)
            self.jtable.merge.assert_called_with(self.source._jdf, expected)

    def test_all_predicate_overloads_and_mixed_assignment_values(self):
        for method, keyword in [("whenMatchedUpdate", "set"), ("whenNotMatchedInsert", "values")]:
            for condition in [None, "s.id>0", self.column]:
                with self.subTest(method=method, condition=condition):
                    builder = self.table.merge(self.source, "t.id=s.id")
                    assignments = {"`secret`": "concat(s.secret, 'quote`')", "id": self.column}
                    self.assertIs(getattr(builder, method)(condition, **{keyword: assignments}), builder)
                    expected = {"`secret`": "concat(s.secret, 'quote`')", "id": self.column._jc}
                    args = (expected,) if condition is None else ((condition._jc if isinstance(condition, Column) else condition), expected)
                    getattr(self.jbuilder, method).assert_called_with(*args)
                    assignments["id"] = "changed"
                    self.assertEqual(expected, getattr(self.jbuilder, method).call_args.args[-1])
        for method in ["whenMatchedDelete", "whenMatchedUpdateAll", "whenNotMatchedInsertAll"]:
            for condition in [None, "s.id>0", self.column]:
                builder = self.table.merge(self.source, "t.id=s.id")
                self.assertIs(getattr(builder, method)(condition), builder)
                args = () if condition is None else ((condition._jc if isinstance(condition, Column) else condition),)
                getattr(self.jbuilder, method).assert_called_with(*args)
        self.assertIsNone(builder.execute())
        self.jbuilder.execute.assert_called_once_with()

    def test_invalid_calls_do_not_mutate_clauses(self):
        builder = self.table.merge(self.source, "t.id=s.id")
        for method, keyword in [("whenMatchedUpdate", "set"), ("whenNotMatchedInsert", "values")]:
            for value, error in [(None, ValueError), ({}, ValueError), ([], TypeError), ({1: "x"}, TypeError), ({"": "x"}, ValueError), ({"a": None}, TypeError), ({"a": 1}, TypeError)]:
                with self.subTest(method=method, value=value), self.assertRaises(error):
                    getattr(builder, method)(**{keyword: value})
            with self.assertRaises(TypeError):
                getattr(builder, method)(False, **{keyword: {"id": "s.id"}})
            getattr(self.jbuilder, method).assert_not_called()
        for method in ["whenMatchedDelete", "whenMatchedUpdateAll", "whenNotMatchedInsertAll"]:
            with self.assertRaises(TypeError):getattr(builder, method)(1)
            getattr(self.jbuilder, method).assert_not_called()
        for source, condition in [(None, "x"), (self.source, None), (self.source, 1)]:
            with self.assertRaises(TypeError):self.table.merge(source, condition)
        for value, error in [(None, TypeError), ("", ValueError), ("  ", ValueError)]:
            with self.assertRaises(error):self.table.alias(value)
            with self.assertRaises(error):self.api.GravitinoDeltaTable.forName(self.spark, value)
        with self.assertRaises(TypeError):self.api.GravitinoDeltaTable.forName(object(), "c.d.t")
        connect = type("DataFrame", (), {"__module__": "pyspark.sql.connect.dataframe"})()
        with self.assertRaisesRegex(TypeError, "classic|Connect"):self.table.merge(connect, "x")

    def test_missing_or_incompatible_api_fails_before_target_factory(self):
        self.entry.forName.reset_mock()
        for version in [0, 2, None]:
            self.entry.apiVersion.return_value = version
            with self.assertRaisesRegex(RuntimeError, "API|apiVersion"):self.api.GravitinoDeltaTable.forName(self.spark, "c.d.t")
        self.entry.apiVersion.side_effect = TypeError("JavaPackage is not callable")
        with self.assertRaisesRegex(RuntimeError, "JVM|companion"):self.api.GravitinoDeltaTable.forName(self.spark, "c.d.t")
        self.entry.forName.assert_not_called()

    def test_execute_preserves_native_error_without_retry(self):
        error = RuntimeError("native denial")
        self.jbuilder.execute.side_effect = error
        with self.assertRaises(RuntimeError) as caught:self.table.merge(self.source, "x").execute()
        self.assertIs(caught.exception, error)
        self.jbuilder.execute.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()

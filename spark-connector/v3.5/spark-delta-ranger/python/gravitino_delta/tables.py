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

"""Pass actual classic DataFrames and Columns to the matching Gravitino JVM companion.

Expression strings use Spark parser semantics. Use ``lit`` or a quoted expression for
literal strings. Builders are single-use, mutable, and not thread-safe. The JVM
independently validates session, expression provenance, policy, and execution state.
"""
from collections.abc import Sequence
from dataclasses import dataclass
import json
from types import MappingProxyType
from typing import Mapping, Optional, Tuple, Union

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.types import StructType

Expression = Union[str, Column]


def _name(value: str, label: str) -> str:
    if not isinstance(value, str):
        raise TypeError(label + " must be a string")
    if not value.strip():
        raise ValueError(label + " must not be empty")
    return value


def _expression(value: Expression):
    if isinstance(value, str):
        return value
    if isinstance(value, Column):
        return value._jc
    raise TypeError("Expression must be a string or classic PySpark Column; Spark Connect is unsupported")


def _entry(spark: SparkSession):
    if not isinstance(spark, SparkSession):
        raise TypeError("A classic PySpark SparkSession is required; Spark Connect is unsupported")
    entry = spark._sc._jvm.org.apache.gravitino.spark.connector.delta.ranger.GravitinoDeltaTable
    try:
        version = entry.apiVersion()
    except Exception as error:
        raise RuntimeError("Gravitino Delta JVM companion API is missing; install the matching runtime") from error
    if type(version) is not int or version != 1:
        raise RuntimeError("Gravitino Delta JVM API version must be 1, got " + repr(version))
    return entry


_METADATA_PROPERTIES = frozenset((
    "delta.enableChangeDataFeed", "primary_key", "delta.logRetentionDuration",
    "delta.deletedFileRetentionDuration", "delta.compatibility.symlinkFormatManifest.enabled",
))


@dataclass(frozen=True)
class GravitinoDeltaMetadata:
    """Detached descriptive metadata; no read or mutation authority is retained."""

    schema: StructType
    properties: Mapping[str, str]
    partition_columns: Tuple[str, ...]


def _metadata_result(raw: str) -> GravitinoDeltaMetadata:
    try:
        if not isinstance(raw, str):
            raise ValueError()
        value = json.loads(raw)
        if not isinstance(value, dict) or set(value) != {"schemaJson", "properties", "partitionColumns"}:
            raise ValueError()
        schema_value = json.loads(value["schemaJson"])
        if not isinstance(schema_value, dict) or schema_value.get("type") != "struct":
            raise ValueError()
        schema = StructType.fromJson(schema_value)
        properties = value["properties"]
        if not isinstance(properties, dict) or any(
                key not in _METADATA_PROPERTIES or not isinstance(item, str)
                for key, item in properties.items()):
            raise ValueError()
        partitions = value["partitionColumns"]
        if not isinstance(partitions, list) or any(
                not isinstance(column, str) or column not in schema.names for column in partitions):
            raise ValueError()
        if len(set(partitions)) != len(partitions):
            raise ValueError()
        return GravitinoDeltaMetadata(schema, MappingProxyType(dict(properties)), tuple(partitions))
    except (ValueError, TypeError, KeyError, AssertionError):
        raise ValueError("Invalid Gravitino Delta metadata response") from None


def _metadata_source(spark: SparkSession, source: DataFrame) -> None:
    if not isinstance(source, DataFrame):
        raise TypeError("A classic PySpark DataFrame is required")
    if source.sparkSession is not spark:
        raise ValueError("Source must belong to the same SparkSession")


class GravitinoDeltaTable:
    """A named target bound to its original classic SparkSession and JVM context."""

    def __init__(self, spark: SparkSession, jtable):
        self._spark = spark
        self._jtable = jtable

    @classmethod
    def forName(cls, spark: SparkSession, name: str) -> "GravitinoDeltaTable":
        """Capture a canonical three-part Gravitino target without reading it."""
        if not isinstance(spark, SparkSession):
            raise TypeError("A classic PySpark SparkSession is required; Spark Connect is unsupported")
        _name(name, "Target name")
        entry = _entry(spark)
        return cls(spark, entry.forName(spark._jsparkSession, name))

    @classmethod
    def metadata(cls, spark: SparkSession, name: str) -> Optional[GravitinoDeltaMetadata]:
        """Read visible native schema and safe properties without selecting payload rows.

        None means positively authorized table absence. Catalog/schema absence,
        denied visibility, stale registration and unavailable metadata all raise.
        Each call performs fresh JVM identity and lease checks.
        """
        _name(name, "Target name")
        entry = _entry(spark)
        try:
            metadata = entry.metadataJson
            if not callable(metadata):
                raise TypeError("Not callable")
        except Exception as error:
            raise RuntimeError("Gravitino Delta JVM metadata API is missing; install the matching runtime") from error
        raw = metadata(spark._jsparkSession, name)
        return None if raw is None else _metadata_result(raw)

    @classmethod
    def create(cls, spark: SparkSession, name: str, schema: StructType, *,
               partition_by: Optional[Sequence] = None, properties: Optional[Mapping] = None,
               comment: Optional[str] = None, if_not_exists: bool = False,
               source: Optional[DataFrame] = None) -> None:
        """Synchronously create an empty canonical named table from a classic StructType.

        Partition entries are literal top-level column names. Inputs are copied, and
        Spark's JSON schema codec preserves types, nullability, and field metadata.
        IF NOT EXISTS succeeds only on positively published metadata after CREATE
        authorization. Unsupported declarations still reject. No final SELECT is made.
        A supplied source binds its exact live JDBC scopes to successful publication;
        it is never materialized by CREATE.
        """
        if not isinstance(spark, SparkSession):
            raise TypeError("A classic PySpark SparkSession is required; Spark Connect is unsupported")
        _name(name, "Target name")
        if not isinstance(schema, StructType):
            raise TypeError("Schema must be a PySpark StructType")
        if partition_by is not None and (not isinstance(partition_by, Sequence) or isinstance(partition_by, (str, bytes))):
            raise TypeError("partition_by must be a sequence of column names")
        partitions = [] if partition_by is None else [_name(value, "Partition column") for value in partition_by]
        if properties is not None and not isinstance(properties, Mapping):
            raise TypeError("Properties must be a mapping from strings to strings")
        copied = []
        for key, value in ({} if properties is None else properties).items():
            _name(key, "Property key")
            if not isinstance(value, str):
                raise TypeError("Property values must be strings")
            copied.append((key, value))
        if comment is not None and not isinstance(comment, str):
            raise TypeError("Comment must be a string or None")
        if type(if_not_exists) is not bool:
            raise TypeError("if_not_exists must be a bool")
        if source is not None:
            _metadata_source(spark, source)
        schema_json = schema.json()
        entry = _entry(spark)
        try:
            create = entry.create if source is None else entry.createForInput
            if not callable(create):
                raise TypeError("Not callable")
        except Exception as error:
            raise RuntimeError("Gravitino Delta JVM create API is missing; install the matching runtime") from error
        java_list = spark._sc._jvm.java.util.ArrayList()
        for value in partitions:
            java_list.add(value)
        java_map = spark._sc._jvm.java.util.HashMap()
        for key, value in copied:
            java_map.put(key, value)
        java_schema = spark._jsparkSession.parseDataType(schema_json)
        # Invocation failures carry native/publication outcomes and must never be
        # reclassified as an absent API or retried by the language binding.
        arguments = (spark._jsparkSession, name, java_schema, java_list, java_map, comment, if_not_exists)
        if source is None:
            create(*arguments)
        else:
            create(*arguments, source._jdf)

    @classmethod
    def enableChangeDataFeed(cls, spark: SparkSession, name: str, *,
                             source: Optional[DataFrame] = None) -> None:
        """Enable named CDF, retaining only explicitly participating live JDBC scopes.

        Native failures preserve their original committed or uncertain outcome. This
        operation never selects the source or retries ALTER.
        """
        _name(name, "Target name")
        if source is not None:
            _metadata_source(spark, source)
        entry = _entry(spark)
        try:
            enable = entry.enableChangeDataFeed
            if not callable(enable):
                raise TypeError("Not callable")
        except Exception as error:
            raise RuntimeError("Gravitino Delta JVM CDF API is missing; install the matching runtime") from error
        enable(spark._jsparkSession, name, None if source is None else source._jdf)

    def alias(self, name: str) -> "GravitinoDeltaTable":
        """Return a target handle with a parser-validated occurrence alias."""
        return GravitinoDeltaTable(self._spark, self._jtable.alias(_name(name, "Alias")))

    def merge(self, source: DataFrame, condition: Expression) -> "GravitinoDeltaMergeBuilder":
        """Begin a MERGE using an actual same-session bounded named CDF DataFrame."""
        if not isinstance(source, DataFrame):
            raise TypeError("Source must be a classic PySpark DataFrame; Spark Connect is unsupported")
        return GravitinoDeltaMergeBuilder(self._spark, self._jtable.merge(source._jdf, _expression(condition)))


class GravitinoDeltaMergeBuilder:
    """Ordered, single-use MERGE clauses; execute synchronously and preserve native errors."""

    def __init__(self, spark: SparkSession, jbuilder):
        self._spark = spark
        self._jbuilder = jbuilder

    def _clause(self, method: str, condition: Optional[Expression], assignments=None):
        args = [] if condition is None else [_expression(condition)]
        if assignments is not None:
            # Validate every entry before allocating/calling the gateway clause. Copy
            # values so later caller-map mutations cannot alter the authored clause.
            if not isinstance(assignments, Mapping):
                raise TypeError("Assignments must be a mapping from strings to expressions")
            if not assignments:
                raise ValueError("Assignments must not be empty")
            copied = [(_name(key, "Assignment key"), _expression(value)) for key, value in assignments.items()]
            java_map = self._spark._sc._jvm.java.util.HashMap()
            for key, value in copied:
                java_map.put(key, value)
            args.append(java_map)
        getattr(self._jbuilder, method)(*args)
        return self

    def whenMatchedUpdate(self, condition: Optional[Expression] = None, set: Optional[Mapping] = None) -> "GravitinoDeltaMergeBuilder":
        """Append an UPDATE with explicit nonempty assignments and optional predicate."""
        if set is None:
            raise ValueError("UPDATE assignments are required")
        return self._clause("whenMatchedUpdate", condition, set)

    def whenMatchedDelete(self, condition: Optional[Expression] = None) -> "GravitinoDeltaMergeBuilder":
        """Append a DELETE with an optional predicate."""
        return self._clause("whenMatchedDelete", condition)

    def whenNotMatchedInsert(self, condition: Optional[Expression] = None, values: Optional[Mapping] = None) -> "GravitinoDeltaMergeBuilder":
        """Append an INSERT with explicit nonempty values and optional predicate."""
        if values is None:
            raise ValueError("INSERT values are required")
        return self._clause("whenNotMatchedInsert", condition, values)

    def whenMatchedUpdateAll(self, condition: Optional[Expression] = None) -> "GravitinoDeltaMergeBuilder":
        """Append UPDATE star with the existing named MERGE semantics."""
        return self._clause("whenMatchedUpdateAll", condition)

    def whenNotMatchedInsertAll(self, condition: Optional[Expression] = None) -> "GravitinoDeltaMergeBuilder":
        """Append INSERT star with the existing named MERGE semantics."""
        return self._clause("whenNotMatchedInsertAll", condition)

    def execute(self) -> None:
        """Complete native MERGE and registration reconciliation; seal even on failure."""
        self._jbuilder.execute()

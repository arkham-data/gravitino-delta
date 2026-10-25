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
from collections.abc import Mapping
from typing import Optional, Union

from pyspark.sql import Column, DataFrame, SparkSession

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
        entry = spark._sc._jvm.org.apache.gravitino.spark.connector.delta.ranger.GravitinoDeltaTable
        try:
            version = entry.apiVersion()
        except Exception as error:
            raise RuntimeError("Gravitino Delta JVM companion API is missing; install the matching runtime") from error
        if type(version) is not int or version != 1:
            raise RuntimeError("Gravitino Delta JVM API version must be 1, got " + repr(version))
        return cls(spark, entry.forName(spark._jsparkSession, name))

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

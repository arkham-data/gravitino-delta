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

"""Finite-lifetime native JDBC scopes for trusted classic PySpark applications."""
from collections.abc import Mapping

from pyspark.sql import DataFrame, SparkSession


def _method(owner, name):
    # Lookup and invocation are deliberately separate: native failures must retain
    # their original type/cause and must never be retried as an API discovery step.
    try:
        member = getattr(owner, name)
        if not callable(member):
            raise TypeError("Not callable")
    except Exception as error:
        raise RuntimeError("Gravitino JDBC JVM " + name + " API is missing; install the matching runtime") from error
    return member


class GravitinoJdbcInput:
    """Own one native input scope; closing invalidates all its lazy DataFrames."""

    def __init__(self, spark: SparkSession, jinput):
        self._spark = spark
        self._jinput = jinput
        self._closed = False

    @classmethod
    def open(cls, spark: SparkSession, *, lifetime_seconds: int) -> "GravitinoJdbcInput":
        """Open once after session initialization, for 1–86400 monotonic seconds.

        The JVM independently enforces authenticated subject, exact session,
        startup configuration and supported JDBC options. Spark Connect is not
        supported. Native failures propagate unchanged without replay.
        """
        if not isinstance(spark, SparkSession):
            raise TypeError("A classic PySpark SparkSession is required; Spark Connect is unsupported")
        if type(lifetime_seconds) is not int:
            raise TypeError("lifetime_seconds must be an integer")
        if not 1 <= lifetime_seconds <= 86400:
            raise ValueError("lifetime_seconds must be between 1 and 86400")
        entry = spark._sc._jvm.org.apache.gravitino.spark.connector.delta.ranger.GravitinoJdbcInput
        version = _method(entry, "apiVersion")()
        if type(version) is not int or version != 1:
            raise RuntimeError("Gravitino JDBC JVM API version must be 1")
        return cls(spark, _method(entry, "open")(spark._jsparkSession, lifetime_seconds))

    def read(self, options: Mapping[str, str]) -> DataFrame:
        """Copy string options and return the actual issued native DataFrame."""
        if self._closed:
            raise RuntimeError("Gravitino JDBC input is closed")
        if not isinstance(options, Mapping):
            raise TypeError("JDBC options must map strings to strings")
        copied = list(options.items())
        keys = set()
        for key, value in copied:
            if not isinstance(key, str) or not isinstance(value, str):
                raise TypeError("JDBC options must map strings to strings")
            if key.lower() in keys:
                raise ValueError("Duplicate case-insensitive JDBC option")
            keys.add(key.lower())
        java_map = self._spark._sc._jvm.java.util.HashMap()
        for key, value in copied:
            java_map.put(key, value)
        return DataFrame(_method(self._jinput, "read")(java_map), self._spark)

    def close(self) -> None:
        """Close once, retaining native failures without a second close attempt."""
        if not self._closed:
            self._closed = True
            _method(self._jinput, "close")()

    def __enter__(self) -> "GravitinoJdbcInput":
        """Return this scope without renewing its lifetime."""
        if self._closed:
            raise RuntimeError("Gravitino JDBC input is closed")
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """Invalidate issued input on normal or exceptional scope exit."""
        self.close()

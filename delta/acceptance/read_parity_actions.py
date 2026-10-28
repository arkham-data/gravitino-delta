# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information regarding
# copyright ownership. The ASF licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may not use
# this file except in compliance with the License. You may obtain
# a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Native read action transport attached to the installed test JVM."""
import os
import json
import sys
from collections import Counter
import pandas
import pyarrow
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, DataFrame

os.environ["PYSPARK_GATEWAY_PORT"] = sys.argv[1]
os.environ["PYSPARK_GATEWAY_SECRET"] = os.environ["READ_ACTION_TOKEN"]
SparkContext._ensure_initialized()
gateway = SparkContext._gateway
bridge = gateway.entry_point
jsc = gateway.jvm.JavaSparkContext.fromSparkContext(bridge.session().sparkContext())
sc = SparkContext(gateway=gateway, jsc=jsc, conf=SparkConf(_jconf=jsc.getConf()))
spark = SparkSession(sc, bridge.session())
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
assert pyspark.__version__ == spark.version
print(json.dumps(dict(pandas=pandas.__version__, pyarrow=pyarrow.__version__, pyspark=pyspark.__version__, observations=json.loads(bridge.observations()))), flush=True)
query = "SELECT p.id, p.secret, d.secret AS label FROM a.db.t p INNER JOIN b.db.t d ON p.id = d.id"

expected = Counter([(1, "MASKED", "x"), (1, "MASKED", "y"), (3, "MASKED", "v")])


def planned():
    frame = spark.sql(query)
    bridge.observeFrame(frame._jdf)
    return frame


def arrow(frame):
    actual = Counter(frame.toPandas().itertuples(index=False, name=None))
    assert actual == expected, actual
    print(json.dumps(dict(action="toPandas", rows=list(actual.elements()))), flush=True)
    assert bridge.captureExpired()


def denied(frame, fresh=False):
    sc.setJobGroup("denied-arrow", "denied-arrow")
    before = set(sc.statusTracker().getJobIdsForGroup("denied-arrow"))
    if fresh:
        assert bridge.broadcastCount() > 0
        assert bridge.unstartedBroadcasts()
    try:
        frame.toPandas()
    except Exception as failure:
        assert "SecurityException" in str(failure), str(failure)
    else:
        raise AssertionError("Revoked Arrow action returned payload")
    assert set(sc.statusTracker().getJobIdsForGroup("denied-arrow")) == before
    if fresh:
        assert bridge.unstartedBroadcasts()
    assert bridge.captureExpired()
    sc.clearJobGroup() if hasattr(sc, "clearJobGroup") else sc.setLocalProperty("spark.jobGroup.id", None)


mode = sys.argv[2]
if mode.startswith("legacy"):
    expected = Counter([(1, "a-one-masked", None), (3, None, None)])
if mode == "views":
    bridge.denyColumn("secret")
    bridge.denyColumn("email")
    bridge.denyColumn("region")
    spark.table("a.db.t").createTempView("python_view")
    assert spark.table("python_view").select("id").count() == 4
    bridge.setSelectAllowed(True)
    source = spark.table("a.db.t").select("id", "email").where("id = 1L")
    assert [tuple(row) for row in source.collect()] == [(1, "M")]
    source.createOrReplaceTempView("python_view")
    retained = spark.table("python_view")
    spark.table("a.db.t").select("id", "email").where("id = 2L").createOrReplaceTempView("python_view")
    assert list(spark.table("python_view").toPandas().itertuples(index=False, name=None)) == [(2, "M")]
    assert list(retained.toPandas().itertuples(index=False, name=None)) == [(1, "M")]
    assert spark.catalog.dropTempView("python_view")
    try:
        spark.table("python_view")
    except Exception as failure:
        assert "TABLE_OR_VIEW_NOT_FOUND" in str(failure), str(failure)
    else:
        raise AssertionError("Dropped view resolved")
    assert [tuple(row) for row in retained.collect()] == [(1, "M")]
    bridge.denyColumn("email")
    try:
        retained.toPandas()
    except Exception as failure:
        assert "SecurityException" in str(failure), str(failure)
    else:
        raise AssertionError("Revoked retained view returned data")
elif mode == "show":
    import contextlib
    import io
    frame = spark.table("a.db.t").select("id", "email").orderBy("id")
    settings = spark.conf.get("spark.sql.session.timeZone")
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        frame.show(2, truncate=False)
    shown = output.getvalue()
    assert "|1  |M    |" in shown and "|2  |M    |" in shown, shown
    assert "only showing top 2 rows" in shown, shown
    assert not any(value in shown for value in ("hidden", "secret", "s1", "s2")), shown
    print(json.dumps(dict(action="show", output=shown)), flush=True)
    bridge.denyColumn("secret")
    for selected in (spark.table("a.db.t").select("secret"), spark.table("a.db.t").where("secret = 's1'").select("id")):
        try:
            selected.show(2, truncate=False)
        except Exception as failure:
            assert "SecurityException" in str(failure), str(failure)
        else:
            raise AssertionError("Forbidden show returned values")
    assert spark.conf.get("spark.sql.session.timeZone") == settings
    assert frame.toPandas()["email"].tolist() == ["M"] * 4
elif mode == "primitives":
    import datetime
    import decimal
    import time
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    def normalized(row, collected=False):
        values = []
        for value in row:
            if value is None or pandas.isna(value):
                values.append(None)
            elif isinstance(value, (datetime.datetime, pandas.Timestamp)):
                if collected:
                    value = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
                values.append(value.isoformat(timespec="microseconds"))
            elif isinstance(value, datetime.date):
                values.append(value.isoformat())
            elif isinstance(value, decimal.Decimal):
                values.append(str(value))
            else:
                values.append(value)
        return values
    expected_values = [[1, "M", 7, "visible", True, 1.5, "12.30", "2026-10-26", "2026-10-26T12:34:56.123456"], [2, "M", None, None, None, None, None, None, None]]
    for enabled in ("false", "true"):
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", enabled)
        frame = spark.table("a.db.t").select("id", "email", "number", "text", "flag", "value", "amount", "day", "stamp").orderBy("id")
        before = {key: spark.conf.get(key) for key in ("spark.sql.execution.arrow.pyspark.enabled", "spark.sql.execution.arrow.pyspark.fallback.enabled", "spark.sql.session.timeZone")}
        collected = frame.collect()
        print(json.dumps(dict(action="primitive-collect", host_timezone=time.tzname, session_timezone=spark.conf.get("spark.sql.session.timeZone"), raw_rows=[repr(row) for row in collected])), flush=True)
        governed = [normalized(row, collected=True) for row in collected]
        assert governed == expected_values, governed
        for _ in range(2):
            result = frame.toPandas()
            actual = [normalized(row) for row in result.itertuples(index=False, name=None)]
            assert actual == governed, actual
            print(json.dumps(dict(action="primitive-toPandas", arrow=enabled, rows=actual, raw_rows=[repr(row) for row in result.itertuples(index=False, name=None)], dtypes={key: str(value) for key, value in result.dtypes.items()}, settings=before)), flush=True)
        assert [normalized(row, collected=True) for row in frame.collect()] == governed
        assert before == {key: spark.conf.get(key) for key in before}
        bridge.denyColumn("amount")
        try:
            frame.toPandas()
        except Exception as failure:
            assert "SecurityException" in str(failure), str(failure)
        else:
            raise AssertionError("Denied primitive conversion returned values")
        bridge.setSelectAllowed(True)
elif mode == "legacy":
    df = planned()
    bridge.setSelectAllowed(False)
    denied(df, fresh=True)
    bridge.setSelectAllowed(True)
    arrow(planned())
    assert Counter(tuple(r) for r in planned().collect()) == expected
elif mode in ("arrow", "mixed", "named", "named-recovery"):
    df = planned()
    if mode == "mixed":
        assert Counter(tuple(r) for r in df.collect()) == expected
    arrow(df)
    arrow(df)
    assert Counter(tuple(r) for r in df.collect()) == expected
    bridge.setSelectAllowed(False)
    denied(df)
    bridge.setSelectAllowed(True)
    fresh = planned()
    bridge.setSelectAllowed(False)
    denied(fresh, fresh=True)
    bridge.setSelectAllowed(True)
    arrow(planned())
elif mode == "wrong-caller":
    assert json.loads(bridge.observations())["caller"] == "arrow-other"
    df = DataFrame(bridge.retainedFrame(), spark)
    denied(df)
elif mode == "drift":
    for change, restore in [
        (lambda: bridge.setRegistrationChanged(True), lambda: bridge.setRegistrationChanged(False)),
        (lambda: bridge.setMask("'CHANGED'"), lambda: bridge.setMask("'MASKED'")),
        (lambda: bridge.setFilter("region = 'west'"), lambda: bridge.setFilter("region = 'east'")),
        (lambda: spark.conf.set("spark.sql.caseSensitive", "true"), lambda: spark.conf.set("spark.sql.caseSensitive", "false")),
        (lambda: sc.setLocalProperty("kyuubi.session.user", "different-user"), lambda: sc.setLocalProperty("kyuubi.session.user", None)),
    ]:
        df = planned()
        change()
        try:
            denied(df, fresh=True)
        finally:
            restore()
    arrow(planned())
elif mode in ("operators", "legacy-operators"):
    expected_count = 2 if mode.startswith("legacy") else 3
    aggregate = spark.sql("SELECT count(secret) AS n FROM (" + query + ")")
    bridge.observeFrame(aggregate._jdf)
    # The legacy fixture has one NULL masked secret; the adaptive fixture masks all rows.
    expected_nonnull = 1 if mode.startswith("legacy") else 3
    assert aggregate.toPandas()["n"].tolist() == [expected_nonnull]
    assert bridge.captureExpired()
    bridge.setSelectAllowed(False)
    denied(aggregate)
    bridge.setSelectAllowed(True)
    factories = (
        lambda: spark.sql(query).limit(0),
        lambda: spark.sql(query + " WHERE false"),
        lambda: spark.sql("SELECT count(*) AS n FROM (" + query + " WHERE false)"),
        lambda: spark.sql("SELECT count(secret) AS n FROM (" + query + ")"),
    )
    for factory in factories:
        frame = factory()
        bridge.observeFrame(frame._jdf)
        bridge.setSelectAllowed(False)
        denied(frame)
        bridge.setSelectAllowed(True)
        result = frame.toPandas()
        assert result.empty or result["n"].tolist() in ([0], [expected_nonnull]), result
        assert bridge.captureExpired()
        original_mask = "concat({col}, '-masked')" if mode.startswith("legacy") else "'MASKED'"
        original_filter = "region = 'keep'" if mode.startswith("legacy") else "region = 'east'"
        for change, restore in (
            (lambda: bridge.setRegistrationChanged(True), lambda: bridge.setRegistrationChanged(False)),
            (lambda: bridge.setMask("'CHANGED'"), lambda: bridge.setMask(original_mask)),
            (lambda: bridge.setFilter("region = 'absent'"), lambda: bridge.setFilter(original_filter)),
            (lambda: spark.conf.set("spark.sql.caseSensitive", "true"), lambda: spark.conf.set("spark.sql.caseSensitive", "false")),
            (lambda: sc.setLocalProperty("kyuubi.session.user", "different-user"), lambda: sc.setLocalProperty("kyuubi.session.user", None)),
        ):
            fresh = factory()
            bridge.observeFrame(fresh._jdf)
            change()
            try:
                denied(fresh)
            finally:
                restore()
    assert spark.sql(query).count() == expected_count
    bridge.setSelectAllowed(False)
    try:
        spark.sql(query).count()
    except Exception as failure:
        assert "SecurityException" in str(failure), str(failure)
    else:
        raise AssertionError("Denied native count returned payload")
    bridge.setSelectAllowed(True)
elif mode == "conversion":
    empty = spark.sql(query + " WHERE false")
    bridge.observeFrame(empty._jdf)
    assert empty.toPandas().empty
    assert bridge.captureExpired()
    bridge.setSelectAllowed(False)
    denied(empty)
    bridge.setSelectAllowed(True)
    bad = spark.sql(query.replace("p.id,", "CAST('10000-01-01' AS DATE) AS id,"))
    bridge.observeFrame(bad._jdf)
    try:
        bad.toPandas()
    except (ValueError, OverflowError) as failure:
        assert "year" in str(failure).lower() or "range" in str(failure).lower(), str(failure)
    else:
        raise AssertionError("Out-of-range Python date conversion unexpectedly succeeded")
    assert bridge.captureExpired()
    bridge.setSelectAllowed(False)
    denied(bad)
    bridge.setSelectAllowed(True)
    arrow(planned())
elif mode in ("inflight", "legacy-inflight", "failure"):
    from concurrent.futures import ThreadPoolExecutor
    df = planned()
    bridge.arm()
    def payload():
        sc.setJobGroup("barrier-arrow", "barrier-arrow", interruptOnCancel=True)
        return df.toPandas()
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            action = pool.submit(payload)
            assert bridge.awaitPayload(), "Native Arrow never reached payload barrier"
            try:
                assert not bridge.captureExpired()
            except BaseException:
                bridge.release()
                raise
            bridge.setSelectAllowed(False)
            bridge.refresh()
            if mode == "failure":
                sc.cancelJobGroup("barrier-arrow")
            bridge.release()
            if mode in ("inflight", "legacy-inflight"):
                assert Counter(action.result(timeout=60).itertuples(index=False, name=None)) == expected
            else:
                try:
                    action.result(timeout=60)
                except Exception as failure:
                    assert "cancel" in str(failure).lower(), str(failure)
                else:
                    raise AssertionError("Cancelled Arrow returned payload")
    finally:
        bridge.release()
    assert bridge.captureExpired()
    denied(df)
    bridge.setSelectAllowed(True)
    bridge.refresh()
    arrow(planned())
observations = json.loads(bridge.observations())
assert any(event["route"] == "Arrow" for event in observations["events"])
if mode != "wrong-caller":
    assert all(event["caller"] == observations["caller"] for event in observations["events"])
if mode.startswith("named"):
    assert observations["caller"] == "arrow-origin"
print(json.dumps(dict(result="PASS", mode=mode, route="native df.toPandas Arrow fallback=false", observations=observations)), flush=True)
gateway.close()

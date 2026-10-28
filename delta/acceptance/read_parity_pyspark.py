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

"""Actual classic PySpark actions and an isolated setup-identity native seed process."""
import contextlib
import datetime
import decimal
import hashlib
import io
import json
import math
import os
from pathlib import Path
import sys


def normalize(value):
    """Keep nulls, decimal scale and timestamp microseconds explicit in JSON evidence."""
    if value is None or type(value).__name__ in ["NaTType", "NAType"]:
        return None
    if isinstance(value, decimal.Decimal):
        return {"decimal": str(value)}
    if isinstance(value, datetime.datetime):
        return {"timestamp": value.isoformat(timespec="microseconds")}
    if isinstance(value, datetime.date):
        return {"date": value.isoformat()}
    if isinstance(value, float) and math.isnan(value):
        return None
    if type(value).__name__ in ["NaTType", "NAType"]:
        return None
    if hasattr(value, "item"):
        return normalize(value.item())
    return value


def save(path, value):
    path = Path(path)
    assert not path.exists(), "Evidence already exists"
    path.write_text(json.dumps(value, indent=2) + "\n")


def ref(path):
    path = Path(path)
    return {"path":str(path), "sha256":hashlib.sha256(path.read_bytes()).hexdigest()}


def native_seed(request):
    from pyspark.sql import SparkSession
    from harness import credentials
    from s3_objects import OwnedObjects
    access, secret = credentials(request["primary"]["credentialsFile"])
    builder = SparkSession.builder.master("local[2]").appName("read-parity-native-seed")
    settings = {"spark.ui.enabled":"false", "spark.driver.host":"127.0.0.1",
                "spark.driver.bindAddress":"127.0.0.1", "spark.sql.shuffle.partitions":"2",
                "spark.databricks.delta.snapshotPartitions":"2", "spark.sql.session.timeZone":"UTC",
                "spark.sql.extensions":"io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog":"org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.hadoop.fs.s3a.endpoint":request["primary"]["endpoint"],
                "spark.hadoop.fs.s3a.endpoint.region":"us-east-1", "spark.hadoop.fs.s3a.path.style.access":"true",
                "spark.hadoop.fs.s3a.connection.ssl.enabled":"false",
                "spark.hadoop.fs.s3a.aws.credentials.provider":"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "spark.hadoop.fs.s3a.access.key":access,"spark.hadoop.fs.s3a.secret.key":secret}
    for key,value in settings.items(): builder=builder.config(key,value)
    spark=builder.getOrCreate()
    try:
        assert spark.version == request["spark"]
        assert spark.sparkContext._jsc.sc().sparkUser() == request["nativeIdentity"]
        assert spark._jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser().getShortUserName() == request["nativeIdentity"]
        rows="SELECT 1L id, 'east' region, 'z' email, 's1' secret UNION ALL SELECT 2L,'east','a','s2' UNION ALL SELECT 3L,'east',CAST(NULL AS STRING),'s3' UNION ALL SELECT 4L,'west','hidden','s4' UNION ALL SELECT 5L,'east','b','s5'"
        data=spark.sql(rows)
        observations={}
        for table in request["tables"]:
            assert not OwnedObjects(request["primary"],table["prefix"]).manifest(), "Fresh isolated seed prefix required"
            frame=data
            if table["name"]=="primitive":
                frame=spark.sql("""SELECT 1L id, 'east' region, 'z' email, 7L nlong,
                  'visible' text, true flag, 1.5D number, CAST(12.30 AS DECIMAL(10,2)) money,
                  DATE '2026-10-26' day, TIMESTAMP '2026-10-26 12:34:56.123456' stamp
                  UNION ALL SELECT 2L,'east',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
                  UNION ALL SELECT 4L,'west','hidden',9L,'hidden',false,2.0D,99.99,
                    DATE '2026-10-27',TIMESTAMP '2026-10-27 00:00:00'""")
            frame.write.format("delta").mode("errorifexists").save(table["location"])
            actual=spark.read.format("delta").load(table["location"])
            detail=spark.sql("DESCRIBE DETAIL delta.`"+table["location"]+"`").collect()[0].asDict()
            observations[table["name"]]={"id":detail["id"],"location":detail["location"],
                "schema":actual.schema.jsonValue(),"rows":[[normalize(v) for v in row] for row in actual.orderBy("id").collect()]}
        origins={}
        loader=spark._jvm.java.lang.Thread.currentThread().getContextClassLoader()
        for name in ["org.apache.spark.sql.SparkSession","org.apache.spark.sql.delta.DeltaLog","scala.Predef$"]:
            cls=loader.loadClass(name);path=Path(str(cls.getProtectionDomain().getCodeSource().getLocation().getPath())).resolve()
            origins[name]=ref(path)
        save(Path(request["output"])/"result.json",{"tables":observations,"origins":origins,"spark":spark.version,"nativeIdentity":request["nativeIdentity"]})
    finally:spark.stop()


def main(request_path):
    request=json.loads(Path(request_path).read_text())
    if request.get("mode")=="seed":
        native_seed(request)
        return
    from pyspark import SparkContext
    from pyspark.sql import SparkSession, functions as F
    import pandas
    import pyarrow
    from read_parity_checks import CASE_SPECS, PRIMITIVE_COLUMNS
    assert os.environ.get("TZ")=="UTC"
    SparkContext._ensure_initialized()
    bridge=SparkContext._jvm.org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerLiveReadParity(str(request_path))
    jsc=SparkContext._jvm.org.apache.spark.api.java.JavaSparkContext.fromSparkContext(bridge.session().sparkContext())
    context=SparkContext(gateway=SparkContext._gateway,jsc=jsc)
    spark=SparkSession(context,bridge.session())
    execute_cases(spark,bridge,request)


def execute_cases(spark,bridge,request):
    from pyspark.sql import functions as F
    import pandas
    import pyarrow
    from read_parity_checks import CASE_SPECS, PRIMITIVE_COLUMNS
    table=request["catalog"]+"."+request["database"]+".source"
    output=Path(request["pythonOutput"]);output.mkdir()
    cases=[]
    phase="initial"

    def transition(name):
        nonlocal phase
        bridge.phase(name)
        phase=name

    def run(name, factory, action=None):
        api, expected, columns=CASE_SPECS[name]
        bridge.begin(name)
        frame=None;observed=None;error=None;arrow=None;display=None
        try:
            frame=factory()
            if api=="count":observed=[[int(frame.count())]]
            elif api=="toPandas":
                enabled=name.endswith("-on")
                spark.conf.set("spark.sql.execution.arrow.pyspark.enabled",str(enabled).lower())
                spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled","false")
                calls=[]
                def profile(pyframe,event,arg):
                    if event=="return" and pyframe.f_code.co_name=="_collect_as_arrow":
                        calls.append(isinstance(arg,list) and all(isinstance(batch,pyarrow.RecordBatch) for batch in arg))
                previous=sys.getprofile();sys.setprofile(profile)
                try:pdf=frame.toPandas()
                finally:sys.setprofile(previous)
                observed=[[normalize(value) for value in row] for row in pdf.itertuples(index=False,name=None)]
                arrow={"enabled":enabled,"fallback":False,"observed":bool(calls) and all(calls),
                       "pandas":pandas.__version__,"pyarrow":pyarrow.__version__,
                       "dtypes":{column:str(dtype) for column,dtype in pdf.dtypes.items()},
                       "pythonTZ":os.environ["TZ"],"sessionTZ":spark.conf.get("spark.sql.session.timeZone")}
            elif api=="show":
                text=io.StringIO()
                with contextlib.redirect_stdout(text):frame.show(2,truncate=False)
                display=text.getvalue();(output/(name+".stdout")).write_text(display)
                parsed=[line.split("|")[1:-1] for line in display.splitlines() if line.startswith("|")][1:]
                observed=[[int(row[0].strip()),row[1].strip()] for row in parsed]
                assert "only showing top 2 rows" in display
            else:observed=[[normalize(value) for value in row] for row in frame.collect()]
        except Exception as failure:
            error={"class":type(failure).__name__,"message":str(failure)}
        observation=json.loads(Path(str(bridge.finish(name,None if frame is None else frame._jdf,error is None))).read_text())
        if expected=="ranger-outcome":
            if request.get("comparator"):
                expected=[[4]]
            else:
                access=[record["allowed"] for record in observation["rangerRequests"]
                        if record["policyType"]==0 and record["resource"].get("column")=="*"]
                assert access and len(set(access))==1,"No consistent observed wildcard decision"
                expected=[[4]] if access[0] else None
        case={"name":name,"identityProfile":request["identityProfile"],"api":api,"actionRoute":"native-pyspark","outcome":"denied" if error else "allowed",
              "expectedRows":expected,"observedRows":observed,"expectedColumns":sorted(columns),"error":error,
              "policyPhase":phase,**observation}
        if arrow is not None:case["arrow"]=arrow
        if display is not None:case["display"]=ref(output/(name+".stdout"))
        if request.get("comparator"):
            case["comparison"]="matches-delta-contract" if observed==expected and (error is not None)==(expected is None) else "measured-difference"
        cases.append(case);save(output/(name+".json"),case)
        if not request.get("comparator"):
            assert (error is not None)==(expected is None), (name,error)
            assert observed==expected,(name,observed,expected)

    try:
        if request["identityProfile"]=="id-only":
            run("id-only-bare-count",lambda:spark.table(table))
            run("id-count",lambda:spark.table(table).select("id"))
            run("id-composition",lambda:spark.table(table).select("id").filter("id > 0").orderBy("id"))
            run("explicit-star-denied",lambda:spark.table(table).select("*"))
            run("authored-region-denied",lambda:spark.table(table).filter("region = 'east'").select("id"))
            run("email-denied",lambda:spark.table(table).select("email"))
            run("sort-email-denied",lambda:spark.table(table).select("id","email").orderBy("email").select("id"))
            spark.table(table).select("id").createTempView("parity_view")
            run("view-count",lambda:spark.table("parity_view"))
            spark.table(table).select("id","email").createTempView("parity_explicit")
            run("view-explicit-denied",lambda:spark.table("parity_explicit"))
            spark.table(table).select("id").filter("id = 1").createOrReplaceTempView("parity_view")
            retained=spark.table("parity_view")
            spark.table(table).select("id").filter("id = 2").createOrReplaceTempView("parity_view")
            run("view-replaced",lambda:spark.table("parity_view"))
            run("view-retained",lambda:retained)
            spark.catalog.dropTempView("parity_view")
            run("view-dropped",lambda:spark.table("parity_view"))
            run("view-after-drop",lambda:retained)
            retained=spark.table(table).select("id").orderBy("id")
            run("id-retained-warmup",lambda:retained)
            transition("revoke-id")
            run("retained-revoked",lambda:retained)
        elif request["identityProfile"]=="denied-email":
            run("wildcard-email-denied",lambda:spark.table(table).select("email"))
            run("wildcard-deny-count",lambda:spark.table(table))
        elif request["identityProfile"]=="no-grant":
            run("no-grant-denied",lambda:spark.table(table).select("id"))
        elif request["identityProfile"]=="table-wide":
            run("table-count",lambda:spark.table(table))
            run("count-one",lambda:spark.sql("SELECT COUNT(1) n FROM "+table))
            run("raw-email-count",lambda:spark.sql("SELECT COUNT(email) n FROM "+table))
            run("raw-null-order",lambda:spark.table(table).select("id","email").orderBy(F.col("email").asc_nulls_first(),"id"))
            run("sorted-limit",lambda:spark.table(table).select("id").orderBy("id").limit(2))
            run("zero-limit",lambda:spark.table(table).select("id").limit(0))
            run("false-count",lambda:spark.table(table).select("id").filter("id < 0"))
            transition("constant-mask")
            run("constant-email-count",lambda:spark.sql("SELECT COUNT(email) n FROM "+table))
            run("constant-distinct",lambda:spark.table(table).select("email").distinct())
            ordered=lambda:spark.table(table).select("id","email").orderBy("email","id")
            run("masked-order",ordered)
            run("show",ordered)
            run("pandas-arrow-off",ordered)
            run("pandas-arrow-on",ordered)
            primitive=lambda:spark.table(request["catalog"]+"."+request["database"]+".primitive").select(*PRIMITIVE_COLUMNS).orderBy("id")
            run("primitive-pandas-off",primitive)
            run("primitive-pandas-on",primitive)
            run("aqe-join",lambda:spark.table(table).select("id","email").alias("p").join(
                spark.table(table).select("id").alias("d"),F.col("p.id")==F.col("d.id")).select("p.id","p.email").orderBy("id"))
            retained=ordered()
            run("masked-retained-warmup",lambda:retained)
            retained.createTempView("masked_view")
            retained_view=spark.table("masked_view")
            run("masked-view",lambda:retained_view)
            transition("null-mask")
            run("retained-mask-denied",lambda:retained)
            run("view-retained-mask-denied",lambda:retained_view)
            run("view-fresh-null-mask",lambda:spark.table("masked_view"))
            run("null-email-count",lambda:spark.sql("SELECT COUNT(email) n FROM "+table))
            run("null-distinct",lambda:spark.table(table).select("email").distinct())
            run("fresh-null-mask",ordered)
        else:raise AssertionError("Unknown identity profile")
        save(output/"cases.json",cases)
    finally:bridge.close()


if __name__=="__main__":main(Path(sys.argv[1]).resolve())

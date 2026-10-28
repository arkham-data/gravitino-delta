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

"""Serial isolated Gravitino/Ranger/MinIO ordinary native-read qualification."""
import argparse
import copy
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import time
import uuid
import xml.etree.ElementTree as ET
from urllib.parse import quote

from cdf_live import (ROOT, JAVA, Public, Ranger, Server, RecordingProxy, envfile, save, sha, read,
                      clean_environment, catalog_properties, submit, command, identity, RECEIPT,
                      verify_deployed, prepare_request)
from cdf_merge_live import await_file, registration
from cdf_merge_live_checks import file_ref
from cdf_read_live_package import verify_runtime
from read_parity_checks import verify_result


def start(argv, environment, directory):
    directory.mkdir(mode=0o700)
    save(directory/"argv.json",list(map(str,argv)))
    stdout=(directory/"stdout").open("wb");stderr=(directory/"stderr").open("wb")
    process=subprocess.Popen(list(map(str,argv)),cwd=ROOT,env=environment,stdout=stdout,stderr=stderr)
    return process,stdout,stderr


def finish(process,stdout,stderr,directory,terminate=False):
    if terminate and process.poll() is None:process.terminate()
    try:code=process.wait(timeout=60)
    except subprocess.TimeoutExpired:
        process.kill();code=process.wait(timeout=15)
    stdout.close();stderr.close()
    save(directory/"exit.json",{"exitCode":code,"pid":process.pid,"terminated":terminate})
    return code


def seed(plan,runtime,output):
    output.mkdir(mode=0o700)
    request={key:plan[key] for key in ["primary","tables"]}
    request.update(mode="seed",spark=runtime["spark"],delta=runtime["delta"],
                   nativeIdentity=plan["serviceAdmin"],output=str(output))
    save(output/"request.private.json",request)
    environment=clean_environment(plan)
    environment.update(TZ="UTC",SPARK_HOME=runtime["sparkHome"],PYSPARK_PYTHON=plan["python"],
                       SPARK_USER=plan["serviceAdmin"],HADOOP_USER_NAME=plan["serviceAdmin"])
    process,out,err=start([runtime["sparkHome"]+"/bin/spark-submit","--driver-memory","2g",
                          ROOT/"delta/acceptance/read_parity_pyspark.py",output/"request.private.json"],environment,output/"process")
    try:
        process.wait(timeout=300)
    finally:code=finish(process,out,err,output/"process",process.poll() is None)
    assert code==0,"Original native seed failed"
    result=read(output/"result.json")
    assert all(runtime["jarHashes"].get(origin["path"])==origin["sha256"] for origin in result["origins"].values())
    return result


def fixture_resources(kind,plan,columns):
    """Resources for explicitly fixture-authored SELECT/filter/mask policies only."""
    assert kind in (0,1,2)
    values={"catalog":[plan["catalog"]],"database":[plan["database"]],"table":["source","primitive"]}
    if kind in (0,1):values["column"]=columns
    return {key:{"values":value,"isExcludes":False,"isRecursive":False} for key,value in values.items()}


def provision(runtime,inputs,output,association,handles):
    output.mkdir(mode=0o700)
    for name in ["private","public-trace","ranger-trace"]:(output/name).mkdir()
    nonce=uuid.uuid4().hex[:12];endpoints=read(inputs["endpoints"]);admin=envfile(inputs["rangerAdminFile"])
    subjects={name:"read_"+name.replace("-","_")+"_"+nonce for name in ["id-only","table-wide","no-grant","denied-email"]}
    plan={"nonce":nonce,"subjects":subjects,"subject":subjects["table-wide"],
          "metadataPrincipal":"read_metadata_"+nonce,"serviceAdmin":"read_setup_"+nonce,
          "metalake":"read_lake_"+nonce,"catalog":"read_catalog","database":"shared",
          "resourceService":"read_service_"+nonce,"resourceDefinition":"read_definition_"+nonce,
          "prefix":"read-parity-"+nonce,"javaHome":JAVA,
          "python":str(ROOT/(".superpowers/runtime-20260914/spark-"+runtime["spark"]+"/bin/python")),
          "rangerUrl":endpoints["ranger-admin"],"fixtureDirectory":str(output),
          "primary":{"endpoint":endpoints["minio"],"region":"us-east-1","bucket":"delta-fixtures","credentialsFile":inputs["minioCredentialsFile"]},
          "rangerAdmin":{"username":admin["RANGER_ADMIN_USER"],"password":admin["RANGER_ADMIN_PASSWORD"]},
          "plugin":{"username":"read_download_"+nonce,"password":"Aa1!"+secrets.token_urlsafe(24)},
          "deployedRangerEvidence":str(association),"deployedRangerEvidenceSha256":sha(association)}
    plan["tables"]=[{"name":name,"prefix":plan["prefix"]+"/adoption/"+name+"/",
                     "location":"s3a://delta-fixtures/"+plan["prefix"]+"/adoption/"+name} for name in ["source","primitive"]]
    save(output/"state.private.json",plan)
    ranger=Ranger(plan,output/"ranger-trace");owned=[]
    def create(kind,path,body):
        result=ranger.call("POST",path,body);owned.append({"kind":kind,"id":result["id"],"name":body["name"]})
        save(output/"ranger-owned.json",owned);return result
    create("downloader","/service/xusers/secure/users",{"name":plan["plugin"]["username"],"firstName":"Read","lastName":"Fixture",
           "password":plan["plugin"]["password"],"emailAddress":plan["plugin"]["username"]+"@fixture.invalid","userRoleList":["ROLE_USER"],"status":1,"userSource":0,"isVisible":1})
    definition=read(ROOT/"authorizations/authorization-ranger-delta/src/main/resources/ranger-delta-servicedef.json")
    definition["name"]=plan["resourceDefinition"];create("definition","/service/public/v2/api/servicedef",definition)
    create("service","/service/public/v2/api/service",{"name":plan["resourceService"],"type":plan["resourceDefinition"],"isEnabled":True,
           "configs":{"create.default.policy.per.hierarchy":"false","policy.download.auth.users":plan["plugin"]["username"],"ranger.plugin.spark.gravitino.metalake":plan["metalake"]}})
    proxy=RecordingProxy(plan["rangerUrl"],output/"callbacks.private.jsonl");proxy.__enter__();handles["proxy"]=proxy
    plan["callbackUri"]="http://127.0.0.1:"+str(proxy.port)
    server=Server(Path(runtime["assemblyManifest"]).parent/"server",output,plan);handles["server"]=server
    server.start();plan["gravitinoUrl"]=server.uri
    rest=Public(server.uri,plan["serviceAdmin"],output/"public-trace");base="/api/metalakes/"+plan["metalake"]
    rest.call("POST","/api/metalakes",{"name":plan["metalake"],"comment":"Isolated ordinary read qualification","properties":{}})
    props=catalog_properties(plan,"primary",plan["prefix"])
    props.update({"authorization-provider":"ranger-delta","authorization.ranger.admin.url":plan["callbackUri"],
        "authorization.ranger.auth.type":"simple","authorization.ranger.username":plan["rangerAdmin"]["username"],
        "authorization.ranger.password":plan["rangerAdmin"]["password"],"authorization.ranger.service.name":plan["resourceService"],
        "authorization.ranger.service.type":plan["resourceDefinition"],"authorization.ranger.service.create-if-absent":"false",
        "authorization.ranger.delta.metalake":plan["metalake"],"authorization.ranger.delta.catalog":plan["catalog"]})
    rest.call("POST",base+"/catalogs",{"name":plan["catalog"],"type":"RELATIONAL","provider":"lakehouse-delta","properties":props})
    rest.call("POST",base+"/catalogs/"+plan["catalog"]+"/schemas",{"name":plan["database"],"properties":{}})
    native=seed(plan,runtime,output/"native-seed");plan["nativeSeed"]=str(output/"native-seed/result.json")
    receipts={}
    for table in plan["tables"]:
        receipt=submit(rest,base,plan["catalog"],table["name"],command("ADOPT",token=str(uuid.uuid4()),location=table["location"],runId=str(uuid.uuid4()),reference="owned-native-seed-exit-zero"))
        receipts[table["name"]]=submit(rest,base,plan["catalog"],table["name"],command("PUBLISH",expected=identity(receipt)))
        assert receipts[table["name"]][RECEIPT+"state"]=="PUBLISHED"
    save(output/"receipts.private.json",receipts)
    def obj(kind,name,priv):return {"type":kind,"fullName":name,"privileges":[{"name":priv,"condition":"ALLOW"}]}
    use=[obj("CATALOG",plan["catalog"],"USE_CATALOG"),obj("SCHEMA",plan["catalog"]+"."+plan["database"],"USE_SCHEMA")]
    roles={}
    for profile,user in dict(subjects,metadata=plan["metadataPrincipal"]).items():
        role="read_role_"+profile.replace("-","_")+"_"+nonce;roles[profile]=role
        rest.call("POST",base+"/users",{"name":user})
        selected=[obj("TABLE",plan["catalog"]+"."+plan["database"]+"."+table["name"],"SELECT_TABLE") for table in plan["tables"]] if profile=="metadata" else []
        rest.call("POST",base+"/roles",{"name":role,"properties":{},"securableObjects":use+selected})
        rest.call("PUT",base+"/permissions/users/"+quote(user,safe="")+"/grant/",{"roleNames":[role]})
    plan["roles"]=roles
    def policy(name,kind,columns,users):
        resource=fixture_resources(kind,plan,columns)
        body={"service":plan["resourceService"],"name":name+"_"+nonce,"policyType":kind,"isEnabled":True,"isAuditEnabled":True,
              "policyLabels":["read-parity-fixture-authored"],"resources":resource}
        item={"users":users,"roles":[],"groups":[],"accesses":[{"type":"select","isAllowed":True}]}
        return body,item
    ids={}
    for profile,columns in [("id-only",["id"]),("table-wide",["*"]),("denied-email",["*"])]:
        body,item=policy("allow_"+profile,0,columns,[subjects[profile]]);body["policyItems"]=[item]
        ids[profile]=create("fixture-policy","/service/public/v2/api/policy",body)["id"]
    body,item=policy("deny_email",0,["email"],[subjects["denied-email"]]);body["denyPolicyItems"]=[item]
    ids["deny-email"]=create("fixture-policy","/service/public/v2/api/policy",body)["id"]
    body,item=policy("east_only",2,[],list(subjects.values()));item["rowFilterInfo"]={"filterExpr":"region = 'east'"};body["rowFilterPolicyItems"]=[item]
    ids["filter"]=create("fixture-policy","/service/public/v2/api/policy",body)["id"]
    plan["policyIds"]=ids
    security=output/"private/source-security";security.mkdir();cache=output/"private/source-cache";cache.mkdir()
    values={"ranger.plugin.spark.service.name":plan["resourceService"],"ranger.plugin.spark.policy.rest.url":plan["rangerUrl"],
            "ranger.plugin.spark.policy.rest.client.username":plan["plugin"]["username"],"ranger.plugin.spark.policy.rest.client.password":plan["plugin"]["password"],
            "ranger.plugin.spark.policy.cache.dir":str(cache),"ranger.plugin.spark.policy.pollIntervalMs":"500",
            "ranger.plugin.spark.policy.rest.client.connection.timeoutMs":"3000","ranger.plugin.spark.policy.rest.client.read.timeoutMs":"3000",
            "ranger.plugin.spark.policy.rest.client.max.retry.attempts":"0","ranger.plugin.spark.policy.rest.client.cookie.enabled":"false",
            "ranger.plugin.spark.supports.policy.deltas":"false"}
    xml=ET.Element("configuration")
    for key,value in values.items():
        prop=ET.SubElement(xml,"property");ET.SubElement(prop,"name").text=key;ET.SubElement(prop,"value").text=value
    ET.indent(xml);ET.ElementTree(xml).write(security/"ranger-spark-security.xml",encoding="utf-8",xml_declaration=True)
    shutil.copyfile(Path(inputs["sourceSecurityDirectory"])/"log4j2.xml",security/"log4j2.xml")
    (output/"private/metadata.properties").write_text("auth-type=simple\nprincipal="+plan["metadataPrincipal"]+"\nlifetime-seconds=3600\n")
    plan["sourceSecurityDirectory"]=str(security);save(output/"state.private.json",plan)
    return plan


def snapshot(plan,output):
    output.mkdir(mode=0o700)
    ranger=Ranger(plan,output/"ranger-trace")
    policies=ranger.call("GET","/service/plugins/secure/policies/download/"+plan["resourceService"]+"?lastKnownVersion=-1&pluginId=read-parity-supervisor",download=True)
    roles=ranger.call("GET","/service/roles/secure/download/"+plan["resourceService"]+"?lastKnownRoleVersion=-1&pluginId=read-parity-supervisor",download=True)
    assert policies["serviceName"]==roles["serviceName"]==plan["resourceService"]
    save(output/"ranger-state.private.json",{"policies":policies,"roles":roles})
    rest=Public(plan["gravitinoUrl"],plan["serviceAdmin"],output/"public-trace")
    base="/api/metalakes/"+plan["metalake"]
    public={key:rest.call("GET",base+"/roles/"+role)["role"] for key,role in plan["roles"].items()}
    save(output/"public-state.private.json",public);save(output/"policies.json",policies)
    result={"policy":policies["policyVersion"],"role":roles["roleVersion"],"publicStateFile":str(output/"public-state.private.json"),
            "rangerStateFile":str(output/"ranger-state.private.json"),"policiesFile":str(output/"policies.json")}
    save(output/"state-evidence.private.json",result)
    return result


def change_policy(plan,phase,output):
    output.mkdir(mode=0o700);ranger=Ranger(plan,output/"ranger-trace")
    if phase=="revoke-id":
        policy=ranger.call("GET","/service/public/v2/api/policy/"+str(plan["policyIds"]["id-only"]))
        policy["isEnabled"]=False;ranger.call("PUT","/service/public/v2/api/policy/"+str(policy["id"]),policy)
    else:
        assert phase in ["constant-mask","null-mask"]
        if "mask" in plan["policyIds"]:
            policy=ranger.call("GET","/service/public/v2/api/policy/"+str(plan["policyIds"]["mask"]))
        else:
            resources=fixture_resources(1,plan,["email"])
            policy={"service":plan["resourceService"],"name":"read_mask_"+plan["nonce"],"policyType":1,"isEnabled":True,"isAuditEnabled":True,
                    "policyLabels":["read-parity-fixture-authored"],"resources":resources,"dataMaskPolicyItems":[]}
        policy["dataMaskPolicyItems"]=[{"users":list(plan["subjects"].values()),"roles":[],"groups":[],
            "accesses":[{"type":"select","isAllowed":True}],"dataMaskInfo":({"dataMaskType":"CUSTOM","valueExpr":"'M'"} if phase=="constant-mask" else {"dataMaskType":"MASK_NULL"})}]
        if "id" in policy:ranger.call("PUT","/service/public/v2/api/policy/"+str(policy["id"]),policy)
        else:plan["policyIds"]["mask"]=ranger.call("POST","/service/public/v2/api/policy",policy)["id"]
    return snapshot(plan,output/"after")


def application_command(runtime,request,request_path):
    """Expose the generated Ranger resource directory in the real driver class path."""
    return [runtime["sparkHome"]+"/bin/spark-submit","--driver-memory","3g","--master","local[2]",
            "--properties-file",request["sparkConfigurationFile"],"--driver-class-path",request["driverSecurityDirectory"],
            ROOT/"delta/acceptance/read_parity_pyspark.py",request_path]


def application(plan,runtime,profile,output):
    output.mkdir(mode=0o700);application_plan=dict(plan,subject=plan["subjects"][profile])
    initial=snapshot(plan,output/"initial")
    request=prepare_request(application_plan,runtime,initial,output/"request")
    request.update(identityProfile=profile,output=str(output/"app-evidence"),pythonOutput=str(output/"python-evidence"))
    properties=Path(request["sparkConfigurationFile"])
    settings={"spark.sql.adaptive.enabled":"true","spark.sql.shuffle.partitions":"8",
              "spark.sql.autoBroadcastJoinThreshold":"-1","spark.sql.adaptive.autoBroadcastJoinThreshold":"-1",
              "spark.sql.session.timeZone":"UTC","spark.sql.execution.arrow.pyspark.fallback.enabled":"false"}
    lines=[line for line in properties.read_text().splitlines() if line.split("=",1)[0] not in settings]
    properties.write_text("\n".join(lines+[key+"="+value for key,value in sorted(settings.items())])+"\n")
    request["requestedSettings"]=settings
    save(output/"request/request.private.json",request)
    environment=clean_environment(application_plan);environment.update(TZ="UTC",SPARK_HOME=runtime["sparkHome"],PYSPARK_PYTHON=plan["python"],
        SPARK_USER=application_plan["subject"],HADOOP_USER_NAME=application_plan["subject"])
    argv=application_command(runtime,request,output/"request/request.private.json")
    process,out,err=start(argv,environment,output/"process")
    states={"initial":initial}
    try:
        for phase in ({"id-only":["revoke-id"],"table-wide":["constant-mask","null-mask"]}.get(profile,[])):
            await_file(Path(request["controlDirectory"])/(phase+"-ready.json"),process,300)
            state=change_policy(plan,phase,output/phase);states[phase]=state
            save(Path(request["controlDirectory"])/(phase+"-verified.json"),{"stateEvidenceFile":str(Path(state["rangerStateFile"]).parent/"state-evidence.private.json")})
        process.wait(timeout=300)
    finally:code=finish(process,out,err,output/"process",process.poll() is None)
    assert code==0,"Original Python application failed"
    cases=read(output/"python-evidence/cases.json")
    for case in cases:
        phase=case["policyPhase"]
        case["policySnapshot"]=file_ref(states[phase]["policiesFile"])
        case["appliedPolicy"]=file_ref(output/"app-evidence"/(phase+"-applied.json"))
    return cases


def worker(runtime_path,inputs_path,output):
    os.umask(0o077)
    output.mkdir(mode=0o700);runtime=read(runtime_path);runtime["manifestPath"]=str(runtime_path)
    verify_runtime(runtime);inputs=read(inputs_path);handles={}
    try:
        association=verify_deployed(inputs,output/"deployed-ranger")
        plan=provision(runtime,inputs,output/"fixture",association,handles)
        cases=[]
        for profile in ["id-only","table-wide","no-grant","denied-email"]:
            cases.extend(application(plan,runtime,profile,output/profile))
        server=handles["server"]
        before=registration(plan,output/"registration-before-restart");server.stop()
        from cdf_provider import store_rows
        persistence=output/"persistence";persistence.mkdir()
        rows=store_rows(server,persistence,"tables","SELECT t.table_name,t.table_id,t.current_version,v.properties FROM table_meta t JOIN table_version_info v ON t.table_id=v.table_id AND t.current_version=v.version WHERE t.deleted_at=0")
        stored={}
        for row in rows:
            state=json.loads(json.loads(row["PROPERTIES"])["delta.registration.internal.state"])
            receipt=before[row["TABLE_NAME"]]["properties"]
            assert state["state"]=="PUBLISHED"
            for key in ["id","generation","token"]:assert str(state[key])==receipt[RECEIPT+key]
            stored[row["TABLE_NAME"]]=state
        server.start();plan["gravitinoUrl"]=server.uri
        after=registration(plan,output/"registration-after-restart");assert before==after
        save(output/"registration.json",{"beforeRestart":before,"afterRestart":after,"persisted":stored})
        exits={profile:file_ref(output/profile/"process/exit.json") for profile in ["id-only","table-wide","no-grant","denied-email"]}
        save(output/"python-exits.json",{"exitCode":0,"children":exits})
        profiles={profile:{"origins":file_ref(output/profile/"app-evidence/origins.json"),
                           "observed":file_ref(output/profile/"app-evidence/observed.json"),
                           "metadata":file_ref(output/profile/"app-evidence/metadata-identity-final.json"),
                           "security":file_ref(output/profile/"app-evidence/security-manifest.json")}
                  for profile in plan["subjects"]}
        result={"schemaVersion":1,"spark":runtime["spark"],"delta":runtime["delta"],"engine":"delta","catalogRoute":"gravitino",
            "catalog":plan["catalog"],"database":plan["database"],"runtimeManifest":file_ref(runtime_path),
            "rangerServerVersion":"2.8.0","rangerClientVersion":"2.8.0","deployedRanger":file_ref(association),
            "profiles":profiles,"subjects":plan["subjects"],"metadataPrincipal":plan["metadataPrincipal"],"setupIdentity":plan["serviceAdmin"],
            "nativeSeed":file_ref(plan["nativeSeed"]),"cases":cases,"origins":file_ref(output/"table-wide/app-evidence/origins.json"),"registration":file_ref(output/"registration.json"),
            "processExits":{"python":file_ref(output/"python-exits.json"),"native":file_ref(output/"fixture/native-seed/process/exit.json")},
            "policyAuthoring":"isolated fixture-authored policies; not Gravitino grant propagation"}
        save(output/"result.worker.json",result)
    finally:
        if "server" in handles:handles["server"].stop()
        if "proxy" in handles:handles["proxy"].__exit__(None,None,None)


def main():
    parser=argparse.ArgumentParser();parser.add_argument("--runtime",required=True);parser.add_argument("--inputs",required=True)
    parser.add_argument("--out",required=True);parser.add_argument("--worker",action="store_true")
    args=parser.parse_args();output=Path(args.out).resolve();runtime=Path(args.runtime).resolve();inputs=Path(args.inputs).resolve()
    if args.worker:worker(runtime,inputs,output);return
    assert not output.exists(),"Fresh evidence directory required"
    process_dir=output.parent/(output.name+"-supervisor")
    process,out,err=start([sys.executable,Path(__file__).resolve(),"--worker","--runtime",runtime,"--inputs",inputs,"--out",output],dict(os.environ),process_dir)
    try:process.wait(timeout=2400)
    finally:code=finish(process,out,err,process_dir,process.poll() is None)
    assert code==0,"Original supervisor failed"
    result=read(output/"result.worker.json");result["processExits"]["supervisor"]=file_ref(process_dir/"exit.json")
    save(output/"result.json",result);verify_result(result)
    print("Verified ordinary read parity cell",runtime.name,output)


if __name__=="__main__":main()

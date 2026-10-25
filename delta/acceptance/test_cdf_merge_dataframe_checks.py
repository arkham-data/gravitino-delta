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

"""Fail-closed archive/input and actual DataFrame frontend evidence checks."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

import cdf_read_live_package as package
from cdf_merge_live import steps


class TestDataframePackage(unittest.TestCase):
    def test_rejects_ambiguous_requested_and_stale_python_archives(self):
        # A stale archive must never win by directory enumeration order.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            requested = root / 'gravitino-delta-python-2.0.0-SNAPSHOT.zip'
            requested.write_bytes(b'requested')
            self.assertEqual(requested, package.select_python_archive(root))
            stale = root / 'gravitino-delta-python-1.0.0.zip'
            stale.write_bytes(b'stale')
            with self.assertRaisesRegex(AssertionError, 'Ambiguous Python archives'):
                package.select_python_archive(root)
            requested.unlink()
            stale.unlink()
            with self.assertRaisesRegex(AssertionError, 'Missing Python archive'):
                package.select_python_archive(root)

    def fixture(self, directory):
        path = Path(directory) / "gravitino-delta-python-test.zip"
        bodies = {"gravitino_delta/__init__.py": b"export", "gravitino_delta/tables.py": b"facade"}
        with zipfile.ZipFile(path, "w") as archive:
            for name, body in bodies.items():archive.writestr(name, body)
        members = {name: hashlib.sha256(body).hexdigest() for name, body in bodies.items()}
        sources = {package.MODULE + "python/" + name: value for name, value in members.items()}
        value = {"pythonApiVersion": 1, "pythonZip": str(path), "pythonZipSha256": package.sha(path), "pythonZipMembers": members, "pythonSourceInputs": sources, "sourceInputs": sources}
        return value

    def test_binds_exact_production_members_and_input_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            value = self.fixture(directory)
            package.verify_python_package(value)
            for mutate in [lambda v:v.update(pythonApiVersion=2), lambda v:v.update(pythonZipSha256="0"*64), lambda v:v["pythonZipMembers"].update({"gravitino_delta/tables.py":"0"*64}), lambda v:v["pythonSourceInputs"].clear(), lambda v:v["sourceInputs"].update({package.MODULE+"python/gravitino_delta/tables.py":"0"*64})]:
                changed=copy.deepcopy(value);mutate(changed)
                with self.assertRaises(AssertionError):package.verify_python_package(changed)
            with zipfile.ZipFile(value["pythonZip"], "a") as archive:archive.writestr("tests/test.py", b"extra")
            with self.assertRaises(AssertionError):package.verify_python_package(value)

    def test_python_changes_invalidate_zip_binding_without_forcing_jvm_rebuild(self):
        with tempfile.TemporaryDirectory() as directory:
            value=self.fixture(directory)
            base=dict(value,changedRuntime="same.jar",spark="3.5.4",delta="3.3.0")
            changed=dict(value["sourceInputs"]);changed[package.MODULE+"python/gravitino_delta/tables.py"]="0"*64
            self.assertTrue(package.can_reuse_companion(base,changed,"3.5.4","3.3.0"))
            value["sourceInputs"]=changed
            with self.assertRaises(AssertionError):package.verify_python_package(value)

    def test_dataframe_steps_are_structured_and_sql_remains_default(self):
        plan={"partitioned":True,"catalog":"c","database":"d","nativeInitial":{"tables":{n:{"id":"uuid-"+n} for n in ["source","same"]}}}
        sql=steps(plan,True)
        self.assertTrue(all("sql" in s and "operation" not in s for s in sql))
        actual=steps(plan,True,frontend="dataframe")
        self.assertEqual(["distinct","columnDenied","selectDenied","modifyDenied","same"],[s["name"] for s in actual])
        for step in actual:
            self.assertNotIn("sql",step)
            operation=step["operation"]
            self.assertEqual({"source":"c.d."+step["source"],"target":"c.d."+step["target"],"start":1,"end":1,"clauseProfile":"partitioned-cdf","expressionMode":"columns-mixed" if step["name"]=="same" else "strings"},operation)


class TestDataframeFrontend(unittest.TestCase):
    def fixture(self, directory):
        runtime=TestDataframePackage().fixture(directory)
        runtime['spark']='3.5.4'
        runtime['providers']={"org.apache.gravitino.spark.connector.delta.ranger."+n:"companion.jar" for n in ['GravitinoDeltaTable','GravitinoDeltaTable$','GravitinoDeltaMergeBuilder']}
        plan={"partitioned":True,"catalog":"c","database":"d","nativeInitial":{"tables":{n:{"id":"uuid-"+n} for n in ["source","same"]}}}
        request={'frontend':'dataframe','steps':steps(plan,True,'dataframe'),'subject':'app'}
        frontend={'frontend':'classic-pyspark-dataframe','apiVersion':1,'pysparkVersion':'3.5.4','python':'3.11','operations':[s['operation'] for s in request['steps']],
            'modules':{name:{'file':runtime['pythonZip']+'/'+member,'archive':runtime['pythonZip'],'sha256':runtime['pythonZipMembers'][member]} for name,member in [('gravitino_delta','gravitino_delta/__init__.py'),('gravitino_delta.tables','gravitino_delta/tables.py')]},
            'queryIdentities':{s['name']:{'subject':'app','ugi':'app','thread':'query-thread'} for s in request['steps']}}
        return runtime,request,frontend

    def test_rejects_sql_source_checkout_mismatch_and_wrong_operation(self):
        from cdf_merge_live_checks import verify_dataframe_frontend
        with tempfile.TemporaryDirectory() as directory:
            runtime,request,frontend=self.fixture(directory)
            verify_dataframe_frontend(frontend,request,runtime)
            mutations=[lambda v:v.update(frontend='classic-pyspark'),lambda v:v.update(apiVersion=2),lambda v:v['operations'][0].update(start=0),lambda v:v['modules']['gravitino_delta.tables'].update(file='/checkout/gravitino_delta/tables.py'),lambda v:v['modules']['gravitino_delta.tables'].update(archive='/other.zip'),lambda v:v['modules']['gravitino_delta.tables'].update(sha256='0'*64),lambda v:v['queryIdentities']['same'].update(subject='setup'),lambda v:v['queryIdentities'].pop('columnDenied')]
            for mutation in mutations:
                changed=copy.deepcopy(frontend);mutation(changed)
                with self.assertRaises((AssertionError,KeyError)):verify_dataframe_frontend(changed,request,runtime)
            Path(runtime['pythonZip']).write_bytes(b'corrupted')
            with self.assertRaises(AssertionError):verify_dataframe_frontend(frontend,request,runtime)


class TestDataframeOriginalEvidence(unittest.TestCase):
    def test_rejects_forged_final_bounds_denial_bags_jars_and_missing_exits(self):
        from test_cdf_merge_partitioned_checks import partition_fixture
        from cdf_merge_live_checks import verify_result
        def change(ref, mutate):
            document = json.loads(Path(ref['path']).read_text())
            mutate(document)
            Path(ref['path']).write_text(json.dumps(document))
            ref['sha256'] = package.sha(ref['path'])
        mutations = {
            'source request bound': lambda r: change(r['provenance']['apps']['python']['request'], lambda v: v['steps'][0]['operation'].update(start=0)),
            'successful denial with commit': lambda r: r['stages']['modifyDenied'].update(outcome='success', errors=[], after=dict(r['stages']['modifyDenied']['after'], version=2)),
            'changed partition bag': lambda r: r['stages']['same']['after']['actions'][1]['remove']['partitionValues'].update(part='forged'),
            'changed CDF multiplicity': lambda r: r['stages']['same']['cdc'].append(r['stages']['same']['cdc'][0]),
            'missing Python exit': lambda r: r['provenance']['apps']['python'].pop('exit'),
            'missing native exit': lambda r: r['provenance'].pop('nativeExit'),
            'wrong JVM provider bytes': lambda r: Path(json.loads(Path(r['provenance']['runtime']['path']).read_text())['changedRuntime']).write_bytes(b'wrong JAR'),
            'wrong requested JVM version': lambda r: change(r['provenance']['runtime'], lambda v: v.update(delta='3.3.2')),
        }
        verify_result(partition_fixture(frontend='dataframe'))
        for name, mutate in mutations.items():
            value = partition_fixture(frontend='dataframe')
            mutate(value)
            with self.subTest(name=name), self.assertRaises((AssertionError, KeyError, ValueError)):
                verify_result(value)

    def test_requires_original_dataframe_command_frontend_and_zip(self):
        from test_cdf_merge_partitioned_checks import partition_fixture
        from cdf_merge_live_checks import verify_result
        value=partition_fixture(frontend='dataframe')
        verify_result(value)
        def change(ref, mutate):
            value=json.loads(Path(ref['path']).read_text());mutate(value)
            Path(ref['path']).write_text(json.dumps(value))
            ref['sha256']=package.sha(ref['path'])
        mutations=[lambda r:change(r['provenance']['frontend'],lambda v:v.update(frontend='classic-pyspark')),
            lambda r:change(r['provenance']['frontend'],lambda v:v['modules']['gravitino_delta.tables'].update(file='/checkout/gravitino_delta/tables.py')),
            lambda r:change(r['provenance']['apps']['python']['command'],lambda v:v['argv'].__setitem__(v['argv'].index('--py-files')+1,'/wrong.zip')),
            lambda r:change(r['provenance']['outerCommand'],lambda v:v['argv'].__setitem__(v['argv'].index('--frontend')+1,'sql')),
            lambda r:r['stages']['same']['operation'].update(expressionMode='strings'),
            lambda r:change(r['provenance']['frontend'],lambda v:v.update(apiVersion=2)),
            lambda r:Path(json.loads(Path(r['provenance']['runtime']['path']).read_text())['pythonZip']).write_bytes(b'wrong')]
        for index,mutation in enumerate(mutations):
            value=partition_fixture(frontend='dataframe');mutation(value)
            with self.subTest(index=index),self.assertRaises((AssertionError,KeyError,ValueError)):
                verify_result(value)


if __name__ == "__main__":unittest.main()

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

"""Validate the built distribution without importing a source checkout."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import zipfile


class TestPackage(unittest.TestCase):
    def test_built_zip_contains_only_production_package_and_imports_in_isolation(self):
        distribution=Path(__file__).resolve().parents[2]/'build/distributions'
        archives=list(distribution.glob('gravitino-delta-python-*.zip'))
        self.assertEqual(1,len(archives),'Run deltaPythonZip before package tests')
        archive=archives[0]
        with zipfile.ZipFile(archive) as bundle:
            self.assertEqual(['gravitino_delta/','gravitino_delta/__init__.py','gravitino_delta/tables.py'],bundle.namelist())
        script="import json, gravitino_delta, gravitino_delta.tables; print(json.dumps({m.__name__:m.__file__ for m in [gravitino_delta,gravitino_delta.tables]}))"
        with tempfile.TemporaryDirectory() as directory:
            result=subprocess.run([sys.executable,'-c',script],cwd=directory,env=dict(os.environ,PYTHONPATH=str(archive)),text=True,capture_output=True,check=True)
        self.assertEqual({'gravitino_delta':str(archive)+'/gravitino_delta/__init__.py','gravitino_delta.tables':str(archive)+'/gravitino_delta/tables.py'},json.loads(result.stdout))


if __name__=='__main__':unittest.main()

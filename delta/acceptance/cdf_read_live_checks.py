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

"""Closed consumption result contract; expected rows derive only from native events."""
from collections import Counter
from datetime import datetime


def projected(native):
    """Apply the independently specified live Ranger filter and masks."""
    return [[0,'MASKED',r[2],r[3],0,'2000-01-01 00:00:00.0'] for r in native if r[2]=='allowed']


def verify_result(result: dict) -> None:
    """Reject missing evidence, partial intervals and changed row multiplicity."""
    required=set(('spark delta java scala ranger requested nativeHead coveredVersions nativeRows sql dataframe empty incomplete retained freshDenied freshGranted grant revoke regrant identities sourceBefore sourceAfter registrationBefore registrationAfter persistedRegistration productionDiscovery runtimeBound exitCode faultBefore faultNativeRows').split())
    assert required <= set(result), 'Missing consumption evidence'
    assert result['spark'] in ['3.5.4','3.5.6'] and result['delta'] in ['3.3.0','3.3.2']
    assert (result['java'],result['scala'],result['ranger'])==('17','2.12.21','2.8.0')
    start,end=result['requested']
    assert all(type(v) is int for v in [start,end,result['nativeHead']]) and 0<=start<=end<=result['nativeHead']
    assert result['coveredVersions']==list(range(start,end+1)), 'Requested terminal coverage missing'
    native=result['nativeRows']
    def shape(row):
        return isinstance(row,list) and len(row)==6 and type(row[0]) is int and type(row[4]) is int and all(isinstance(row[i],str) for i in [1,2,3,5])
    assert native and all(shape(r) and start<=r[4]<=end for r in native)
    assert {r[3] for r in native}=={'insert','delete','update_preimage','update_postimage'}
    fault=result['faultBefore']
    assert fault['outcome']=='success' and fault['rows'] and result['faultNativeRows']
    def native_key(row):return tuple(row[:5])+(datetime.fromisoformat(row[5]),)
    assert Counter(map(native_key,fault['rows']))==Counter(map(native_key,result['faultNativeRows']))
    expected=Counter(map(tuple,projected(native)))
    assert expected and any(n>1 for n in expected.values()), 'Duplicate fixture missing'
    for name in ['sql','dataframe','freshGranted']:
        value=result[name]
        assert value['outcome']=='success' and all(shape(row) for row in value['rows']) and Counter(map(tuple,value['rows']))==expected, 'Governed multiset differs: '+name
    assert result['empty']=={'outcome':'success','bounds':[end,end],'rows':[]}
    for name,outcome in [('incomplete','failed'),('retained','denied'),('freshDenied','denied')]:
        value=result[name]
        assert value['outcome']==outcome and value['rows'] is None and value['failure'], 'Failed interval reported as empty success'
        if name=='incomplete':
            assert value.get('authorized') is True and any('FileNotFoundException' in kind for kind in value['failure'])
        else:assert 'java.lang.SecurityException' in value['failure']
    assert result['incomplete']['bounds']==[start,end] and result['incomplete']['table']=='incomplete'
    for name,select in [('grant',True),('revoke',False),('regrant',True)]:
        value=result[name]
        assert value['publicPrivilege']=='SELECT_TABLE' and value['downloadedSelect'] is select
        assert type(value['policy']) is int and value['policy']>=0 and isinstance(value['downloadSha256'],str) and len(value['downloadSha256'])==64 and all(c in '0123456789abcdef' for c in value['downloadSha256'])
    assert result['regrant']['policy']>result['revoke']['policy']>result['grant']['policy']
    assert set(result['identities'])=={'setup','metadata','application'} and len(set(result['identities'].values()))==3
    for before,after in [('sourceBefore','sourceAfter'),('registrationBefore','registrationAfter')]:
        assert isinstance(result[before],str) and len(result[before])==64 and result[before]==result[after]
    assert all(result[k] is True for k in ['persistedRegistration','productionDiscovery','runtimeBound'])
    assert type(result['exitCode']) is int and result['exitCode']==0

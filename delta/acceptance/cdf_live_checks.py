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

"""Exact public MODIFY and native CDF evidence checks for the owned live lane."""
from collections import Counter
import copy


def exact_access(policies, catalog, database, table, role):
    result = []
    for policy in policies:
        if policy.get('policyType', 0) != 0:
            continue
        resources = policy['resources']
        expected = {'catalog': [catalog], 'database': [database], 'table': [table], 'column': ['*']}
        if set(resources) != set(expected) or any(resources[k]['values'] != v or resources[k].get('isExcludes', False) or resources[k].get('isRecursive', False) for k,v in expected.items()):
            continue
        for item in policy.get('policyItems', []):
            roles = item.get('roles', [])
            assert len(roles) == len(set(roles)), 'Duplicate role membership'
            if role in roles:
                assert not item.get('users') and not item.get('groups') and not item.get('delegateAdmin', False)
                accesses = item['accesses']
                assert all(x['isAllowed'] is True for x in accesses)
                result.extend(x['type'] for x in accesses)
    assert len(result) == len(set(result)), 'Duplicate access'
    return set(result)


def project_modify(policies, role, target):
    values = {p['id']: copy.deepcopy(p) for p in policies}
    assert len(values) == len(policies)
    present = False
    target_id = None
    for policy in values.values():
        if policy.get('policyType', 0) != 0 or policy['resources']['table']['values'] != [target]:
            continue
        if not any(role in x.get('roles', []) for x in policy.get('policyItems', [])):
            continue
        assert target_id is None
        target_id = policy['id']
        items = []
        eligible = Counter()
        app_accesses = set()
        for item in policy.get('policyItems', []):
            accesses = item.get('accesses', [])
            kinds = [x['type'] for x in accesses]
            assert len(kinds) == len(set(kinds)), 'Duplicate privilege'
            roles = item.get('roles', [])
            assert len(roles) == len(set(roles)), 'Duplicate role membership'
            if 'roles' in item:
                item['roles'] = sorted(roles)
                roles = item['roles']
            if set(kinds).intersection({'update', 'alter'}):
                assert len(kinds)==1 and kinds[0] in {'update','alter'}, 'Provider default requires one exact access per item'
                assert all(x['isAllowed'] is True and set(x) == {'type', 'isAllowed'} for x in accesses)
                assert set(item) <= {'roles','accesses','delegateAdmin','users','groups','conditions'}
                assert all(not item.get(k) for k in ['delegateAdmin','users','groups','conditions'])
                eligible[kinds[0]] += 1
                assert eligible[kinds[0]] == 1, 'Duplicate provider access item'
                if role in roles:
                    app_accesses.add(kinds[0])
                    roles.remove(role)
                    if not roles:
                        continue
            items.append(item)
        assert not eligible or eligible==Counter({'update':1,'alter':1}), 'Incomplete provider MODIFY access pair'
        assert app_accesses in [set(),{'update','alter'}], 'Partial application MODIFY membership'
        present = bool(app_accesses)
        policy['policyItems'] = items
        for key in ['version', 'updateTime', 'updatedBy']:
            policy.pop(key, None)
    assert target_id is not None
    return values, target_id, present


def cdf_rows(actual, expected, commit):
    assert all(len(row) == 5 and row[3] == 'insert' and row[4] == commit for row in actual)
    assert Counter(tuple(row) for row in actual) == Counter(tuple(row) for row in expected), 'CDF row multiplicities differ'
    return True


def manifest_origins(manifest):
    """Require actual resolved provider origins to join the artifact hash inventory."""
    assert set(manifest['providers'].values()).issubset(manifest['jarHashes']), 'Unhashed actual provider origin'
    assert all(isinstance(path,str) and isinstance(digest,str) and len(digest)==64 for path,digest in manifest['jarHashes'].items())
    return True


def diagnostic_update(policy, role):
    """Add one explicit UPDATE-only item to a fresh owned diagnostic resource."""
    assert policy.get('policyType',0)==0 and role
    assert all(role not in item.get('roles',[]) and not item.get('groups') for item in policy.get('policyItems',[]))
    assert 'gravitino-managed' in policy['policyLabels']
    result=copy.deepcopy(policy)
    result.setdefault('policyItems',[]).append({'roles':[role],'users':[],'groups':[],'accesses':[{'type':'update','isAllowed':True}]})
    return result


def canonical_privileges(privileges):
    """Normalize public enum spelling, rejecting semantic duplicate privileges."""
    assert all(set(p)=={'name','condition'} and isinstance(p['name'],str) and isinstance(p['condition'],str) for p in privileges)
    values=[(p['name'].upper(),p['condition'].upper()) for p in privileges]
    assert all(name in {'USE_CATALOG','USE_SCHEMA','SELECT_TABLE','MODIFY_TABLE'} for name,_ in values), 'Unknown public privilege'
    assert all(condition in ('ALLOW','DENY') for _,condition in values)
    assert len(values)==len(set(values)), 'Duplicate public privilege'
    return set(values)


def admin_role_state(value):
    """Preserve complete administrative role documents except measured query duration."""
    return {key: entry for key,entry in value.items() if key != 'queryTimeMS'}


def canonical_owner_type(value):
    """Join the REST and persisted spellings of the two supported owner types."""
    assert isinstance(value,str) and value.upper() in {'USER','GROUP'}, 'Unknown owner type'
    return value.upper()


def canonical_protocol(value):
    """Compare full native protocol envelopes, with unordered unique feature lists."""
    assert set(value)=={'protocol'}
    protocol=value['protocol']
    assert {'minReaderVersion','minWriterVersion'} <= set(protocol) <= {'minReaderVersion','minWriterVersion','readerFeatures','writerFeatures'}
    assert all(type(protocol[key]) is int and protocol[key]>0 for key in ['minReaderVersion','minWriterVersion'])
    features=[]
    for key in ['readerFeatures','writerFeatures']:
        values=protocol.get(key,[])
        assert isinstance(values,list) and all(isinstance(v,str) and v for v in values)
        assert len(values)==len(set(values)), 'Duplicate native protocol feature'
        features.append(tuple(sorted(values)))
    return (protocol['minReaderVersion'],protocol['minWriterVersion'],*features)


def cdf_configuration(original,value):
    """Retain every native property while changing only the exact CDF flag."""
    assert value in [None,'true','false']
    result=dict(original)
    if value is not None:result['delta.enableChangeDataFeed']=value
    return result

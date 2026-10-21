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

"""Focused negative tests for live CDF evidence acceptance."""
import copy
import unittest

from cdf_live_checks import exact_access, project_modify, cdf_rows, manifest_origins, diagnostic_update, canonical_privileges, admin_role_state, canonical_owner_type, canonical_protocol, cdf_configuration


def policy(accesses, roles=None):
    return {'id': 7, 'policyLabels': ['gravitino-managed', 'gravitino-metalake-id:1', 'gravitino-catalog-id:2'],
            'resources': {k: {'values': [v], 'isExcludes': False, 'isRecursive': False}
                          for k,v in [('catalog','a'),('database','db'),('table','t'),('column','*')]},
            'policyItems': [{'roles': roles or ['app'], 'users': [], 'groups': [],
                             'accesses': [{'type': x, 'isAllowed': True} for x in accesses]}]}


class TestCdfLiveChecks(unittest.TestCase):
    def test_native_cdf_protocol_preserves_complete_feature_profile(self):
        initial={'protocol':{'minReaderVersion':1,'minWriterVersion':2}}
        enabled={'protocol':{'minReaderVersion':1,'minWriterVersion':7,'writerFeatures':['changeDataFeed','appendOnly','invariants']}}
        self.assertEqual((1,2,(),()),canonical_protocol(initial))
        self.assertEqual((1,7,(),('appendOnly','changeDataFeed','invariants')),canonical_protocol(enabled))
        reordered=copy.deepcopy(enabled);reordered['protocol']['writerFeatures'].reverse()
        self.assertEqual(canonical_protocol(enabled),canonical_protocol(reordered))
        for key,value in [('writerFeatures',['changeDataFeed','changeDataFeed']),('unknown',1)]:
            bad=copy.deepcopy(enabled);bad['protocol'][key]=value
            with self.assertRaises(AssertionError):canonical_protocol(bad)
        self.assertNotEqual(canonical_protocol(initial),canonical_protocol(enabled))
        base={'delta.appendOnly':'true'}
        self.assertEqual({'delta.appendOnly':'true','delta.enableChangeDataFeed':'false'},cdf_configuration(base,'false'))
        self.assertEqual(base,cdf_configuration(base,None))
        with self.assertRaises(AssertionError):cdf_configuration(base,'TRUE')

    def test_owner_type_normalizes_only_known_enum_spelling(self):
        for value in ['USER','user']:self.assertEqual('USER',canonical_owner_type(value))
        for value in ['GROUP','group']:self.assertEqual('GROUP',canonical_owner_type(value))
        for value in ['admin',' user','']:
            with self.assertRaises(AssertionError):canonical_owner_type(value)

    def test_admin_role_comparison_excludes_only_query_timing(self):
        original={'queryTimeMS':1,'roles':[{'id':1,'name':'r','users':[{'name':'app','isAdmin':False}],'groups':[],'roles':[]}]}
        changed=copy.deepcopy(original);changed['queryTimeMS']=2
        self.assertEqual(admin_role_state(original),admin_role_state(changed))
        changed['roles'][0]['users'][0]['isAdmin']=True
        self.assertNotEqual(admin_role_state(original),admin_role_state(changed))
        changed=copy.deepcopy(original);changed['unknown']=True
        self.assertNotEqual(admin_role_state(original),admin_role_state(changed))

    def test_public_enum_case_normalizes_without_dropping_duplicates(self):
        lower=[{'name':'select_table','condition':'allow'}]
        self.assertEqual({('SELECT_TABLE','ALLOW')},canonical_privileges(lower))
        with self.assertRaises(AssertionError):canonical_privileges(lower+[{'name':'SELECT_TABLE','condition':'ALLOW'}])
        for bad in [[{'name':'CREATE_TABLE','condition':'allow'}],[{'name':'select_table','condition':'maybe'}],[{'name':'select_table','condition':'allow','extra':True}]]:
            with self.assertRaises(AssertionError):canonical_privileges(bad)

    def test_every_actual_provider_origin_has_a_hash(self):
        manifest={'providers':{'A':'/real/a.jar'},'jarHashes':{'/real/a.jar':'a'*64}}
        self.assertTrue(manifest_origins(manifest))
        manifest['jarHashes']={'/alias/a.jar':'a'*64}
        with self.assertRaises(AssertionError):manifest_origins(manifest)

    def test_access_is_exact_and_retains_duplicates(self):
        self.assertEqual({'select', 'update', 'alter'}, exact_access([policy(['select','update','alter'])], 'a','db','t','app'))
        for p in [policy(['select','select']), policy(['select'], ['app','app'])]:
            with self.assertRaises(AssertionError): exact_access([p],'a','db','t','app')
        self.assertEqual(set(), exact_access([policy(['alter'])],'a','db','other','app'))

    def test_modify_projection_requires_exact_two_provider_items(self):
        base=policy(['select'],['app','metadata']); granted=copy.deepcopy(base)
        granted['policyItems'][0]['roles'].reverse()
        for name in ['update','alter']:
            granted['policyItems'].append({'roles':['app'],'accesses':[{'type':name,'isAllowed':True}],'users':[],'groups':[]})
        self.assertEqual(project_modify([base],'app','t')[0],project_modify([granted],'app','t')[0])
        self.assertTrue(project_modify([granted],'app','t')[2])
        malformed=[]
        for index in [-1,-2]:
            missing=copy.deepcopy(granted);missing['policyItems'].pop(index);malformed.append(missing)
        duplicated=copy.deepcopy(granted);duplicated['policyItems'].append(copy.deepcopy(duplicated['policyItems'][-1]));malformed.append(duplicated)
        combined=copy.deepcopy(granted);combined['policyItems'][-2]['accesses'].append(combined['policyItems'][-1]['accesses'][0]);combined['policyItems'].pop();malformed.append(combined)
        widened=copy.deepcopy(granted);widened['policyItems'][-1]['accesses'].append({'type':'all','isAllowed':True});malformed.append(widened)
        member=copy.deepcopy(granted);member['policyItems'][-1]['roles'].append('app');malformed.append(member)
        for bad in malformed:
            with self.assertRaises(AssertionError):project_modify([bad],'app','t')
        changed=copy.deepcopy(granted);changed['policyLabels'].append('other')
        self.assertNotEqual(project_modify([base],'app','t')[0],project_modify([changed],'app','t')[0])

    def test_diagnostic_item_preserves_metadata_policy_and_rejects_existing_app(self):
        original=policy(['select'],['metadata'])
        original['policyItems'].insert(0,{'roles':['owner-role'],'users':['setup'],'groups':[],'accesses':[{'type':'all','isAllowed':True}]})
        changed=diagnostic_update(original,'app')
        self.assertEqual(original['policyItems'],changed['policyItems'][:-1])
        self.assertEqual(original['resources'],changed['resources'])
        self.assertEqual(original['policyLabels'],changed['policyLabels'])
        self.assertEqual({'update'}, exact_access([changed],'a','db','t','app'))
        with self.assertRaises(AssertionError):diagnostic_update(changed,'app')

    def test_projection_normalizes_order_without_changing_membership(self):
        original=policy(['select'],['app','metadata'])
        reordered=copy.deepcopy(original);reordered['policyItems'][0]['roles'].reverse()
        self.assertEqual(project_modify([original],'app','t')[0],project_modify([reordered],'app','t')[0])
        reordered['policyItems'][0]['roles'].append('other')
        self.assertNotEqual(project_modify([original],'app','t')[0],project_modify([reordered],'app','t')[0])

    def test_cdf_oracle_compares_multiplicity_and_commit_bound(self):
        rows=[[1,'MASKED','allowed','insert',2],[1,'MASKED','allowed','insert',2]]
        self.assertTrue(cdf_rows(rows, rows, 2))
        for actual in [rows[:1],rows+rows[:1], [[1,'MASKED','allowed','insert',3]]*2,[[1,'MASKED','allowed','delete',2]]*2]:
            with self.assertRaises(AssertionError): cdf_rows(actual,rows,2)


if __name__ == '__main__': unittest.main()

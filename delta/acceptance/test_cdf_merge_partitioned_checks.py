# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Partition action/file corruption cases independent of deployed services."""
import copy
import unittest
from cdf_merge_live_checks import verify_partition_files


def file(path, part):
    return {'path':path,'partitionValues':{'part':part},'size':30,'dataChange':True,'stats':'{}'}


def fixture():
    old=file('part=old/old.parquet','old');hidden=file('part=old/new.parquet','old')
    moved=file('part=moved%252F%253D%2520%2525/moved.parquet','moved/= %');stable=file('part=__HIVE_DEFAULT_PARTITION__/stable.parquet',None)
    def snapshot(files):
        files=[dict(copy.deepcopy(f),dataChange=False) for f in files]
        return {'partitionColumns':['part'],'schema':'fixed-schema','files':files,
                'fileObjects':{f['path']:{'sha256':('a' if f==stable else 'b')*64,'size':30} for f in files}}
    before=snapshot([old,stable]);after=snapshot([hidden,moved,stable])
    after['actions']=[{'commitInfo':{'operation':'MERGE'}},{'remove':dict(old,extendedFileMetadata=True)},
                      {'add':hidden},{'add':moved},{'cdc':dict(file('_change_data/part=old/a.parquet','old'),dataChange=False)},
                      {'cdc':dict(file('_change_data/part=moved%252F%253D%2520%2525/b.parquet','moved/= %'),dataChange=False)}]
    return before,after


class TestPartitionFiles(unittest.TestCase):
    def test_accepts_native_partition_maps_and_untouched_bytes(self):
        before,after=fixture()
        verify_partition_files(before,after,{'old','moved/= %'},{'old'},{'old','moved/= %'})

    def test_rejects_corrupt_action_metadata_and_untouched_evidence(self):
        mutations=[lambda b,a:a['actions'][2]['add'].update(partitionValues={}),
                   lambda b,a:a['actions'][2]['add'].update(dataChange=False),
                   lambda b,a:a['actions'][1]['remove'].update(dataChange=False),
                   lambda b,a:a['actions'][4]['cdc'].update(dataChange=True),
                   lambda b,a:a['files'][0].update(dataChange=True),
                   lambda b,a:b['files'][0].update(dataChange=True),
                   lambda b,a:a['actions'][3]['add']['partitionValues'].update(part='wrong'),
                   lambda b,a:a['actions'][1]['remove'].update(partitionValues={'part':'wrong'}),
                   lambda b,a:a['actions'][4]['cdc'].update(partitionValues={}),
                   lambda b,a:a['files'].pop(),
                   lambda b,a:a['fileObjects']['part=__HIVE_DEFAULT_PARTITION__/stable.parquet'].update(sha256='c'*64),
                   lambda b,a:a.update(partitionColumns=[]),lambda b,a:a.update(schema='drift'),
                   lambda b,a:a['actions'][3]['add'].update(path='wrong/partition/file.parquet')]
        for index,mutate in enumerate(mutations):
            before,after=copy.deepcopy(fixture());mutate(before,after)
            with self.subTest(index=index),self.assertRaises((AssertionError,KeyError)):
                verify_partition_files(before,after,{'old','moved/= %'},{'old'},{'old','moved/= %'})


def partition_fixture():
    """An independently spelled native result for the complete partition profile."""
    from test_cdf_merge_live_checks import artifact_fixture
    source=[[1,'changed','allowed','new','update_preimage',1],[1,'new','allowed','new','update_postimage',1],
            [2,'gone','allowed','old2','delete',1],[4,'four','allowed','four','insert',1],[5,'five','blocked','blocked','insert',1]]
    source_rows=[[1,'new','allowed','new'],[3,'hidden','blocked','new'],[4,'four','allowed','four'],[5,'five','blocked','blocked'],[6,'dup','allowed','dup'],[6,'dup','allowed','dup']]
    fresh=[[1,'old','allowed','old'],[2,'old2','allowed','old2'],[4,'hidden','blocked','old'],[9,'stable','allowed',None]]
    target=[[1,'MASKED|TARGET_MASK','allowed','moved/= %'],[4,'hidden','blocked','old'],[9,'stable','allowed',None],[104,'MASKED','allowed','moved/= %']]
    same=[[1,'MASKED|MASKED','allowed','moved/= %'],[3,'hidden','blocked','new'],[4,'MASKED|MASKED','allowed','moved/= %'],[5,'five','blocked','blocked'],[6,'dup','allowed','dup'],[6,'dup','allowed','dup']]
    def snapshot(name,rows,version):
        from urllib.parse import quote
        parts=list(dict.fromkeys(row[3] for row in rows))
        files=[file('part='+quote(quote(part if part is not None else '__HIVE_DEFAULT_PARTITION__',safe=''),safe='')+'/'+name+'-'+str(i)+'.parquet',part) for i,part in enumerate(parts)]
        return {'id':'uuid-'+name,'version':version,'rows':copy.deepcopy(rows),'partitionColumns':['part'],'schema':'fixed-schema','files':[dict(copy.deepcopy(f),dataChange=False) for f in files],'fileObjects':{f['path']:{'sha256':'a'*64,'size':30} for f in files},'actions':[],'cdc':[]}
    initial={name:snapshot(name,fresh if name=='target' else source_rows,0 if name=='target' else 2) for name in ['source','same','target']}
    stages={};history=copy.deepcopy(initial)
    for name in ['distinct','columnDenied','selectDenied','modifyDenied','same']:
        table='same' if name=='same' else 'target';before=copy.deepcopy(history[table]);after=copy.deepcopy(before)
        success=name in ['distinct','same']
        if success:
            removed={'new','four'} if name=='same' else {'old','old2'}
            retained=[f for f in before['files'] if f['partitionValues']['part'] not in removed]
            after=snapshot(table+'-after',same if name=='same' else target,before['version']+1);after['id']=before['id']
            after['files']=[f for f in after['files'] if f['partitionValues']['part'] in {'moved/= %','new' if name=='same' else 'old'}]+retained
            after['fileObjects']={f['path']:{'sha256':'a'*64,'size':30} for f in after['files']}
            after['cdc']=([[1,'new','allowed','new','update_preimage'],[1,'MASKED|MASKED','allowed','moved/= %','update_postimage'],[4,'four','allowed','four','update_preimage'],[4,'MASKED|MASKED','allowed','moved/= %','update_postimage']] if name=='same' else
                [[1,'old','allowed','old','update_preimage'],[1,'MASKED|TARGET_MASK','allowed','moved/= %','update_postimage'],[2,'old2','allowed','old2','delete'],[104,'MASKED','allowed','moved/= %','insert']])
            after['actions']=[{'commitInfo':{'operation':'MERGE'}}]+[{'remove':dict(f,dataChange=True)} for f in before['files'] if f['partitionValues']['part'] in removed]+[{'add':dict(f,dataChange=True)} for f in after['files'] if f not in retained]
            from urllib.parse import quote
            after['actions'] += [{'cdc':dict(file('_change_data/part='+quote(quote(part,safe=''),safe='')+'/cdc.parquet',part),dataChange=False)} for part in removed|{'moved/= %'}]
        else:after.update(actions=[],cdc=[])
        stages[name]={'before':before,'after':after,'cdc':after['cdc'],'checkpointBefore':0,'checkpointAfter':1 if success else 0,
            'outcome':'success' if success else 'failed','errors':[] if success else ['java.lang.SecurityException'],
            'sourceBefore':'a'*64,'sourceAfter':('b' if name=='same' else 'a')*64,'sql':'will be assigned by artifact fixture',
            'authorization':{'sourceAccess':[] if name=='selectDenied' else (['alter','select','update'] if name=='same' else ['select']),
            'targetAccess':['select'] if name=='modifyDenied' else ['alter','select','update'],'policy':1,'downloadSha256':'b'*64}}
        history[table]=copy.deepcopy(after)
    registration={name:{'id':i+1,'generation':1,'token':'token-'+name,'uuid':snap['id'],'nativeVersion':snap['version']} for i,(name,snap) in enumerate(history.items())}
    value={'profile':'partitioned','spark':'3.5.4','delta':'3.3.0','java':'17','scala':'2.12.21','ranger':'2.8.0','exits':{'outer':0,'native':0,'python':0},
           'identities':{'setup':'s','metadata':'m','application':'a'},'nativeInitial':initial,'sourceNative':source,
           'sourceGoverned':[[row[0]+100,'MASKED',row[2],'moved/= %',row[4],0] for row in source if row[2]=='allowed'],
           'stages':stages,'registration':{key:copy.deepcopy(registration) for key in ['beforeRestart','afterRestart','persisted']}}
    return artifact_fixture(value)


class TestPartitionProfile(unittest.TestCase):
    def test_accepts_complete_original_native_and_python_evidence(self):
        from cdf_merge_live_checks import verify_result
        verify_result(partition_fixture())

    def test_rejects_missing_native_rows_checkpoints_masks_and_registration(self):
        from cdf_merge_live_checks import verify_result
        mutations=[lambda r:r['stages']['same']['cdc'].pop(),
                   lambda r:r['sourceGoverned'][0].__setitem__(3,'raw'),
                   lambda r:r['stages']['distinct']['after']['rows'].pop(1),
                   lambda r:r['stages']['same']['after']['rows'].pop(),
                   lambda r:r['stages']['columnDenied'].update(checkpointAfter=1),
                   lambda r:r['stages']['selectDenied']['authorization'].update(sourceAccess=['select']),
                   lambda r:r['stages']['modifyDenied']['authorization'].update(targetAccess=['alter','select','update']),
                   lambda r:r['registration']['persisted']['target'].update(nativeVersion=0),
                   lambda r:r['stages']['same'].update(sourceAfter='a'*64),
                   lambda r:r['stages']['distinct'].pop('nativeBefore'),
                   lambda r:r['stages']['same']['consumerAfter'].update(sha256='0'*64)]
        for index,mutate in enumerate(mutations):
            value=partition_fixture();mutate(value)
            with self.subTest(index=index),self.assertRaises((AssertionError,KeyError)):
                verify_result(value)

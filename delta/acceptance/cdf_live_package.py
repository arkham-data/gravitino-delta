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

"""Build only the CDF thin app, then manifest shared immutable production artifacts."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import zipfile

from cdf_live import ROOT,H,JAVA,read,save,sha,launch
sys.path.insert(0,str(H))
from package_binding import capture_sources,canonical_digest
from prepare import PROVIDERS,provider_inventory
from cdf_live_checks import manifest_origins


def build(output):
    output.mkdir(mode=0o700)
    before=capture_sources(ROOT);save(output/'source-before.json',{'files':before,'digest':canonical_digest(before)})
    argv=['./gradlew','-I','delta/acceptance/cdf_live.gradle',':spark-connector:spark-delta-ranger-3.5:spotlessApply',':spark-connector:spark-delta-ranger-3.5:cdfLiveThinJar',
        '-PcdfLiveAppOut='+str(output/'app'),'-PdeltaSparkVersion=3.5.4','-PdeltaVersion=3.3.0','-PskipITs','-PskipDockerTests=true','--offline','--max-workers=2','--no-daemon','-Dorg.gradle.jvmargs=-Xmx1536m','--console=plain']
    environment=dict(os.environ,JAVA_HOME=JAVA)
    launch(argv,environment,output/'build')
    after=capture_sources(ROOT);save(output/'source-after.json',{'files':after,'digest':canonical_digest(after)})
    assert before==after, 'Source changed during thin-app build'
    return output/'app/public-grants-live-append.jar'


def package(base_path,app_path,stock_path,spark,delta,output):
    output.mkdir(mode=0o700)
    base=read(base_path)
    assert all(sha(path)==digest for path,digest in base['jarHashes'].items())
    # Production source inputs are unchanged; the new harness and app have independent provenance.
    original=read(Path(base['buildReceipt']).parent/'source-before.json')['files']
    source=capture_sources(ROOT)
    app_build=read(app_path.parent.parent/'source-before.json')['files']
    app_stems={'DeltaRangerLiveAppend','DeltaRangerLivePhasePin','DeltaRangerLiveMetadataBarrier','DeltaRangerDownloadedSecurity','DeltaRangerCallbackGateway','DeltaRangerStartupCache'}
    app_sources={p:d for p,d in app_build.items() if '/src/test/' in p and Path(p).stem in app_stems}
    assert app_sources and all(source.get(p)==d for p,d in app_sources.items()), 'App source moved after thin build'
    production={p:d for p,d in original.items() if '/src/main/' in p or p.endswith(('.gradle','.gradle.kts'))}
    assert all(source.get(p)==d for p,d in production.items()), 'Production source/build inputs changed'
    home=output/'spark-home';home.mkdir();(home/'jars').mkdir()
    stock_path=Path(stock_path).resolve()
    for child in stock_path.iterdir():
        if child.name!='jars':(home/child.name).symlink_to(child,target_is_directory=child.is_dir())
    omitted={c['name'] for c in base['changes'] if c['action']=='omit'}
    sources={p.name:p for p in sorted((stock_path/'jars').glob('*.jar')) if p.name not in omitted}
    selected={Path(p).name:Path(p) for p in base['jarHashes'] if Path(p).name in {c['name'] for c in base['changes'] if c['action']=='add'}}
    selected['public-grants-live-append.jar']=app_path.resolve()
    for name in list(selected):
        if name.startswith(('delta-spark_2.12-', 'delta-storage-')):
            del selected[name]
    for artifact in ['delta-spark_2.12','delta-storage']:
        matches=list((Path.home()/'.gradle/caches/modules-2/files-2.1/io.delta'/artifact/delta).glob('*/*.jar'))
        assert len(matches)==1
        selected[matches[0].name]=matches[0]
    assert not set(sources).intersection(selected)
    sources.update(selected)
    for name,path in sources.items():(home/'jars'/name).symlink_to(path.resolve())
    jars=sorted((home/'jars').glob('*.jar'))
    cdf=['DeltaRangerCdfPropertyCommand','DeltaRangerCdfPropertyMutation','DeltaRangerCdfContext$','DeltaRangerCdfCacheMaintenance$','DeltaRangerCdfCatalogIdentity$']
    # Include all actual CDF provider classes rather than inventing assumed class names.
    prefix='org/apache/gravitino/spark/connector/delta/ranger/'
    optional=Path(base['providers']['org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerConfig'])
    with zipfile.ZipFile(optional) as bundle:
        cdf=[name[:-6].replace('/','.') for name in bundle.namelist() if name.startswith(prefix+'DeltaRangerCdf') and name.endswith('.class') and '$anon' not in name and '$$' not in name]
    providers=provider_inventory(jars,PROVIDERS+cdf)
    with zipfile.ZipFile(app_path) as bundle:thin={name:hashlib.sha256(bundle.read(name)).hexdigest() for name in bundle.namelist() if name.endswith('.class')}
    manifest=dict(base,spark=spark,delta=delta,sparkHome=str(home),app=str(home/'jars/public-grants-live-append.jar'),
        jarHashes={str(p.resolve()):sha(p) for p in jars},runtimeJarLinks={str(p):str(p.resolve()) for p in jars},providers=providers,thinClasses=thin,addedJarNames=sorted(selected),
        sharedProductionManifest=str(base_path),sharedProductionManifestSha256=sha(base_path),
        sharedProductionArtifacts={str(p):sha(p) for name,p in selected.items() if name.startswith('gravitino-')},
        stockSparkHome=str(stock_path),stockJarHashes={p.name:sha(p) for p in sorted((stock_path/'jars').glob('*.jar'))},
        appOriginal=str(app_path.resolve()),appSha256=sha(app_path),appBuildSourceCapture=str(app_path.parent.parent/'source-before.json'),appBuildSourceCaptureSha256=sha(app_path.parent.parent/'source-before.json'),sourceInputs=source,sourceInputDigest=canonical_digest(source),
        productionSourceInputMatch=production,appSourceInputMatch=app_sources,
        provenanceScope='Verified shared Ranger artifact and unchanged Task3 server/base components; separately built CDF app and actual per-cell stock/native runtime')
    # Historical changes list describes the original assembly; use only the current additions below.
    manifest['changes']=[{'action':'add','name':name,'source':str(path),'sha256':sha(path)} for name,path in sorted(selected.items())]
    manifest_origins(manifest)
    save(output/'manifest.json',manifest)
    return output/'manifest.json'


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('mode',choices=['build','package'])
    for k in ['base','app','stock','spark','delta']:parser.add_argument('--'+k)
    parser.add_argument('--out',required=True)
    a=parser.parse_args();os.umask(0o077)
    if a.mode=='build':build(Path(a.out).resolve())
    else:package(Path(a.base).resolve(),Path(a.app).resolve(),Path(a.stock).resolve(),a.spark,a.delta,Path(a.out).resolve())


if __name__=='__main__':main()

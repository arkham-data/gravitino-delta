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

"""Requested-pin companion packaging with exact shared-component and runtime provenance."""
import argparse
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import zipfile

# Reuse the existing source, artifact, provider and process verification primitives.
from cdf_live_package import ROOT, JAVA, read, save, sha, launch, capture_sources, canonical_digest, provider_inventory, manifest_origins

MODULE='spark-connector/v3.5/spark-delta-ranger/'
RUNTIME='spark-connector/v3.5/spark-delta-ranger-runtime/'
APP='org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerLiveCdfRead'


def classes(path):
    with zipfile.ZipFile(path) as jar:
        return {name:hashlib.sha256(jar.read(name)).hexdigest() for name in jar.namelist() if name.endswith('.class')}


def archive_entries(path):
    """All entry bytes, excluding only ZIP container timestamps and compression metadata."""
    with zipfile.ZipFile(path) as jar:
        names=jar.namelist();assert len(names)==len(set(names)), 'Duplicate archive entries'
        return {name:hashlib.sha256(jar.read(name)).hexdigest() for name in names}


def python_archive_entries(path):
    """Hash production files; permit only the Zip task's optional package directory."""
    entries=archive_entries(path)
    assert {name for name in entries if name.endswith('/')}<={'gravitino_delta/'}
    return {name:digest for name,digest in entries.items() if not name.endswith('/')}


def python_sources(source):
    """Complete production facade inputs, independently bound to the Python zip."""
    return {p:d for p,d in source.items() if p.startswith(MODULE+'python/gravitino_delta/') and p.endswith('.py')}


def select_python_archive(directory):
    """Reject stale-output ambiguity instead of trusting glob enumeration order."""
    archives=sorted(Path(directory).glob('gravitino-delta-python-*.zip'))
    assert archives, 'Missing Python archive'
    assert len(archives)==1, 'Ambiguous Python archives; clean stale distributions before packaging'
    return archives[0]


def verify_python_package(runtime):
    """Reject changed archives, extra/missing members and wrong source/member bindings."""
    assert runtime['pythonApiVersion']==1
    assert sha(runtime['pythonZip'])==runtime['pythonZipSha256']
    members=python_archive_entries(runtime['pythonZip'])
    assert members==runtime['pythonZipMembers']
    sources=python_sources(runtime['sourceInputs'])
    assert sources and sources==runtime['pythonSourceInputs']
    expected={p.removeprefix(MODULE+'python/'):d for p,d in sources.items()}
    assert {'gravitino_delta/__init__.py','gravitino_delta/tables.py'}<=set(expected)
    assert members==expected


def companion_sources(source):
    """Complete changed-module production inputs, separate from thin-app test sources."""
    return {p:d for p,d in source.items() if p.startswith((MODULE,RUNTIME)) and
        ('/src/main/' in p or p.endswith(('.gradle','.gradle.kts')))}


def can_reuse_companion(base, source, spark, delta):
    """Reuse a verified companion only for identical runtime pins and production inputs."""
    return bool(base.get('changedRuntime')) and (base['spark'],base['delta'])==(spark,delta) and companion_sources(base['sourceInputs'])==companion_sources(source)


def shared_sources(source):
    """Exact complete production inventory outside the independently rebuilt companion."""
    return {p:d for p,d in source.items() if ('/src/main/' in p or p.endswith(('.gradle','.gradle.kts'))) and
        not p.startswith((MODULE,RUNTIME,'delta/acceptance/','.superpowers/'))}


def verify_supplemental_artifacts(runtime):
    """Recheck archived build-path dependencies even after subsequent pair builds overwrite them."""
    for original, artifact in runtime.get('supplementalArtifacts', {}).items():
        assert sha(artifact['path'])==artifact['sha256'], 'Supplemental artifact changed: '+original
        assert archive_entries(artifact['path'])==artifact['members'], 'Supplemental members changed'


def verify_runtime(runtime):
    """Recheck immutable build receipts and every class/provider/origin before live use."""
    assert capture_sources(ROOT)==runtime['sourceInputs'],'Source moved after package capture'
    assert canonical_digest(runtime['sourceInputs'])==runtime['sourceInputDigest']
    if runtime.get('pythonZip'):verify_python_package(runtime)
    verify_supplemental_artifacts(runtime)
    assert all(sha(p)==d for p,d in runtime['jarHashes'].items())
    assert all(str(Path(p).resolve())==target for p,target in runtime['runtimeJarLinks'].items())
    assert all(sha(p)==d for p,d in runtime['sharedArtifacts'].items())
    base=read(runtime['sharedProductionManifest'])
    assert sha(runtime['sharedProductionManifest'])==runtime['sharedProductionManifestSha256']
    assert shared_sources(base['sourceInputs'])==shared_sources(runtime['sourceInputs'])==runtime['sharedComponentSources']
    receipt=read(runtime['freshBuildReceipt']);assert sha(runtime['freshBuildReceipt'])==runtime['freshBuildReceiptSha256']
    assert read(receipt['exit'])['exitCode']==0
    assert read(receipt['before'])['files']==read(receipt['after'])['files']==runtime['sourceInputs']
    assert all(sha(p)==d for p,d in receipt['artifacts'].items())
    if runtime.get('pythonZip'):assert receipt['artifacts'][runtime['pythonZip']]==runtime['pythonZipSha256']
    assert classes(runtime['app'])==runtime['thinClasses']
    assert archive_entries(runtime['app'])==archive_entries(receipt['appBuildArtifact'])
    assert all(runtime['thinClasses'][name]==digest for name,digest in receipt['thinCompiledClasses'].items())
    assert set(runtime['thinClasses'])==set(receipt['thinCompiledClasses'])
    if receipt.get('reusedCompanion'):
        assert can_reuse_companion(base,runtime['sourceInputs'],runtime['spark'],runtime['delta'])
        assert runtime['changedRuntime']==base['changedRuntime']
    assert classes(runtime['changedRuntime'])==runtime['changedRuntimeClasses']
    inventories={path:classes(path) for path in set(runtime['providers'].values())}
    assert all(runtime['providerClasses'][name]==inventories[path][name.replace('.','/')+'.class'] for name,path in runtime['providers'].items())
    manifest_origins(runtime)


def app_profile(app):
    assert app in ['read','merge','read-parity','append-aqe'], 'Unknown thin application'
    if app=='append-aqe':
        return {'init':'delta/acceptance/append_aqe_live.gradle','task':'appendAqeLiveThinJar',
                'property':'appendAqeLiveAppOut','jar':'append-aqe-live.jar',
                'class':APP.replace('CdfRead','AppendAqe'),'provider':APP.replace('CdfRead','AppendAqe').replace('/','.')}
    if app=='read-parity':
        return {'init':'delta/acceptance/read_parity_live.gradle','task':'readParityLiveThinJar',
                'property':'readParityLiveAppOut','jar':'read-parity-live.jar',
                'class':APP.replace('CdfRead','ReadParity'),'provider':APP.replace('CdfRead','ReadParity').replace('/','.')}
    stem='cdf-'+app+'-live'
    return {'init':'delta/acceptance/'+stem.replace('-','_')+'.gradle','task':'cdf'+app.title()+'LiveThinJar', 'property':'cdf'+app.title()+'LiveAppOut','jar':stem+'.jar','class':APP if app=='read' else APP.replace('CdfRead','CdfMerge')}


def build_package(base_path, output, spark='3.5.4', delta='3.3.0', application='read'):
    """Build one pinned pair; copy only changed artifacts and link verified immutable inputs."""
    assert spark in ['3.5.4','3.5.6'] and delta in ['3.3.0','3.3.2']
    profile=app_profile(application)
    output.mkdir(mode=0o700)
    assert shutil.disk_usage(output).free>8*1024**3,'Insufficient space for changed artifact build'
    base=read(base_path);before=capture_sources(ROOT)
    assert shared_sources(base['sourceInputs'])==shared_sources(before),'Shared component inputs differ, including additions/deletions'
    assert all(sha(p)==d for p,d in base['jarHashes'].items())
    assembly=read(base['assemblyManifest'])
    shared={p:d for p,d in assembly['artifactFiles'].items() if '/server/' in p}
    assert shared and all(sha(p)==d for p,d in shared.items()),'Shared server artifact drift'
    save(output/'source-before.json',{'files':before,'digest':canonical_digest(before),'gitHead':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()})
    reuse_companion=can_reuse_companion(base,before,spark,delta)
    tasks=[':spark-connector:spark-delta-ranger-3.5:'+profile['task'],':spark-connector:spark-delta-ranger-3.5:deltaPythonZip']
    if not reuse_companion:tasks=[':spark-connector:spark-delta-ranger-3.5:clean']+tasks+[':spark-connector:spark-delta-ranger-runtime-3.5:shadowJar']
    argv=['./gradlew','-I',profile['init']]+tasks+[
        '-P'+profile['property']+'='+str(output/'app'),'-PdeltaSparkVersion='+spark,'-PdeltaVersion='+delta,
        '-PskipITs','-PskipDockerTests=false','--offline','--max-workers=2','--no-daemon','-Dorg.gradle.jvmargs=-Xmx1536m','--console=plain']
    launch(argv,dict(os.environ,JAVA_HOME=JAVA),output/'build',timeout=900)
    after=capture_sources(ROOT);save(output/'source-after.json',{'files':after,'digest':canonical_digest(after)})
    assert before==after,'Source changed during fresh build'
    if reuse_companion:changed=Path(base['changedRuntime']).resolve()
    else:
        shadow=next(p for p in (ROOT/RUNTIME/'build/libs').glob('*runtime*.jar') if not p.name.endswith('-empty.jar'))
        ranger=output/'ranger';ranger.mkdir();changed=ranger/shadow.name;shutil.copyfile(shadow,changed)
    built_app=output/'app'/profile['jar'];app=built_app
    if archive_entries(built_app)==archive_entries(base['app']):app=Path(base['app']).resolve()
    compiled={str(p.relative_to(ROOT)):sha(p) for p in sorted((ROOT/MODULE/'build/classes').rglob('*.class'))}
    if reuse_companion:
        previous=read(base['freshBuildReceipt'])['compiledClasses']
        assert {p:d for p,d in compiled.items() if '/main/' in p}=={p:d for p,d in previous.items() if '/main/' in p},'Compiled production class drift despite unchanged source'
    thin_compiled={}
    for name,digest in classes(built_app).items():
        matches=[d for p,d in compiled.items() if p.endswith('/'+name)]
        assert matches==[digest],'Thin JAR class does not match exactly one compiled output'
        thin_compiled[name]=digest
    save(output/'compiled-classes.json',compiled)
    built_python=select_python_archive(ROOT/MODULE/'build/distributions')
    python_zip=output/built_python.name
    shutil.copyfile(built_python,python_zip)
    artifacts={str(p):sha(p) for p in [changed,built_app,python_zip,output/'compiled-classes.json']}
    supplemental={}
    if application in ['merge','read-parity','append-aqe']:
        build_inputs=output/({'read-parity':'app/read-parity-build-inputs.json','append-aqe':'app/append-aqe-build-inputs.json'}.get(application,'app/cdf-merge-build-inputs.json'))
        artifacts[str(build_inputs)]=sha(build_inputs)
        resolved=read(build_inputs)
        dependencies=output/'supplemental';dependencies.mkdir()
        for path in sorted(set().union(*(set(paths) for paths in resolved.values()))):
            if str(ROOT) in path and '/build/' in path:
                original=Path(path);retained=dependencies/(sha(path)+'-'+original.name)
                shutil.copyfile(original,retained)
                supplemental[path]={'path':str(retained),'sha256':sha(retained),'members':archive_entries(retained)}
                artifacts[str(retained)]=sha(retained)
        for configuration in ['compileClasspath','testCompileClasspath']:
            assert all(sha(path)==digest for path,digest in resolved[configuration].items())
            names={Path(path).name for path in resolved[configuration]}
            assert {'spark-sql_2.12-'+spark+'.jar','delta-spark_2.12-'+delta+'.jar','scala-library-2.12.21.jar'}<=names
    receipt={'before':str(output/'source-before.json'),'after':str(output/'source-after.json'),'exit':str(output/'build/exit.json'),
        'command':str(output/'build/command.json'),'artifacts':artifacts,'appBuildArtifact':str(built_app),'thinCompiledClasses':thin_compiled,'reusedCompanion':reuse_companion,'reusedApp':app!=built_app,'moduleSources':{p:d for p,d in before.items() if p.startswith((MODULE,RUNTIME))},
        'compiledClasses':compiled,'gitHead':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()}
    save(output/'build-receipt.json',receipt)
    home=output/'spark-home';home.mkdir();(home/'jars').mkdir()
    stock=Path(base['stockSparkHome'])
    if spark!=base['spark']:stock=Path(str(stock).replace('spark-'+base['spark'],'spark-'+spark))
    for child in stock.iterdir():
        if child.name!='jars':(home/child.name).symlink_to(child,target_is_directory=child.is_dir())
    # Preserve the prior verified overlay, changing only Spark stock, native Delta, companion and app.
    oldstock=base['stockJarHashes'];oldjars={Path(p).name:Path(p) for p in base['jarHashes']}
    omitted=set(oldstock)-set(oldjars)
    sources={p.name:p for p in (stock/'jars').glob('*.jar') if p.name not in omitted}
    selected={n:p for n,p in oldjars.items() if n not in oldstock and not n.startswith(('gravitino-spark-connector-delta-ranger-runtime','public-grants-live-append','cdf-read-live','cdf-merge-live','read-parity-live','append-aqe-live','delta-spark_2.12-','delta-storage-'))}
    selected[changed.name]=changed;selected[app.name]=app
    for artifact in ['delta-spark_2.12','delta-storage']:
        matches=list((Path.home()/'.gradle/caches/modules-2/files-2.1/io.delta'/artifact/delta).glob('*/*.jar'));assert len(matches)==1
        selected[matches[0].name]=matches[0]
    assert not set(sources).intersection(selected);sources.update(selected)
    for name,path in sources.items():(home/'jars'/name).symlink_to(path.resolve())
    jars=sorted((home/'jars').glob('*.jar'))
    prefix='org/apache/gravitino/spark/connector/delta/ranger/'
    ranger_classes=classes(changed)
    names=[n for n in base['providers'] if not n.startswith(('org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerLiveAppend','org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerLivePhasePin','org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerLiveCdf','org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerLiveReadParity','org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerLiveAppendAqe'))]
    names+= [n[:-6].replace('/','.') for n in ranger_classes if n.startswith((prefix+'DeltaRangerCdf',prefix+'DeltaRangerMerge',prefix+'GravitinoDeltaTable',prefix+'GravitinoDeltaMergeBuilder')) and '$anon' not in n and '$$' not in n]
    if application=='append-aqe':
        names += ['py4j.JavaServer','py4j.ClientServer','py4j.ClientServerConnection']
        stems=('DeltaRangerAppend','DeltaRangerExecutionContext','DeltaRangerNativeScans','DeltaRangerNativeIdempotency','DeltaRangerPhysicalProvenance','DeltaRangerRaw','DeltaRangerCache')
        names += [n[:-6].replace('/','.') for n in ranger_classes if n.startswith(tuple(prefix+stem for stem in stems)) and '$anon' not in n and '$$' not in n]
    names+=[profile.get('provider',profile['class'].replace('/','.')+'$')]
    providers=provider_inventory(jars,sorted(set(names)))
    for name,path in selected.items():
        if path not in [changed,app] and name.startswith('gravitino-'):shared[str(path)]=sha(path)
    inventories={path:classes(path) for path in set(providers.values())}
    manifest=dict(schemaVersion=1,javaHome=JAVA,manifestRole='CDF '+application+' pinned-pair runtime',application=application,
        assemblyManifest=base['assemblyManifest'],assemblySha256=sha(base['assemblyManifest']),
        assemblyProvenanceScope='Shared server only; current companion/application provenance and reuse flags use freshBuildReceipt',spark=spark,delta=delta,sparkHome=str(home),app=str(app),changedRuntime=str(changed.resolve()),
        jarHashes={str(p.resolve()):sha(p) for p in jars},runtimeJarLinks={str(p):str(p.resolve()) for p in jars},addedJarNames=sorted(selected),
        providers=providers,providerClasses={n:inventories[p][n.replace('.','/')+'.class'] for n,p in providers.items()},
        supplementalArtifacts=supplemental,thinClasses=classes(app),changedRuntimeClasses=ranger_classes,sourceInputs=before,sourceInputDigest=canonical_digest(before),
        pythonApiVersion=1,pythonZip=str(python_zip),pythonZipSha256=sha(python_zip),pythonZipMembers=python_archive_entries(python_zip),pythonSourceInputs=python_sources(before),
        sharedComponentSources=shared_sources(before),sharedArtifacts=shared,sharedProductionManifest=str(base_path),sharedProductionManifestSha256=sha(base_path),
        freshBuildReceipt=str(output/'build-receipt.json'),freshBuildReceiptSha256=sha(output/'build-receipt.json'),
        stockSparkHome=str(stock),stockJarHashes={p.name:sha(p) for p in (stock/'jars').glob('*.jar')},
        provenanceScope=('Reused identical-pin companion' if reuse_companion else 'Requested-pin Gradle companion output (task outcome retained in build log)')+'; '+('reused byte-identical application' if app!=built_app else 'fresh application')+'; exact unchanged shared component source inventory and server/base artifact bytes')
    verify_runtime(manifest);save(output/'manifest.json',manifest)
    return output/'manifest.json'


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--base',required=True);parser.add_argument('--out',required=True)
    parser.add_argument('--app',choices=['read','merge','read-parity','append-aqe'],default='read');parser.add_argument('--spark',default='3.5.4');parser.add_argument('--delta',default='3.3.0')
    args=parser.parse_args();os.umask(0o077)
    build_package(Path(args.base).resolve(),Path(args.out).resolve(),args.spark,args.delta,args.app)


if __name__=='__main__':main()

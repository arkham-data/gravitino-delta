/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.spark.connector.delta.ranger;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;
import org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerRegistrationReader.PublishedObservation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.DeltaLog$;
import org.apache.spark.sql.delta.OptimisticTransaction;
import org.apache.spark.sql.internal.SharedState;
import org.apache.spark.sql.types.DataType;
import scala.None$;
import scala.Option;
import scala.Some;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.immutable.Map$;

/**
 * Pinned non-loading observation of the ordinary Delta module cache. No loader, native operation or
 * reconstruction authority is issued. Stock access bookkeeping and expiration remain possible.
 *
 * <p>S3A requires immutable trusted application storage settings since native object creation.
 * Current equal maps cannot authenticate historical backend provenance. The supported S3 profile is
 * static credentials with stock Hadoop AWS 3.3.6 and AWS SDK bundle 1.12.367; IAM/STS is
 * unqualified.
 */
final class DeltaRangerRawDeltaLogCache {
  private static final String S3 = "org.apache.hadoop.fs.s3a.";
  private static final String[] STORAGE = {
    "fs.s3a.impl",
    "fs.s3a.endpoint",
    "fs.s3a.access.key",
    "fs.s3a.secret.key",
    "fs.s3a.aws.credentials.provider",
    "fs.s3a.path.style.access",
    "fs.s3a.connection.ssl.enabled"
  };

  private DeltaRangerRawDeltaLogCache() {}

  static CacheObservation observe(
      @Nullable SparkSession session, @Nullable PublishedObservation publication) {
    return observe(session, publication, null);
  }

  private static CacheObservation observe(
      @Nullable SparkSession session,
      @Nullable PublishedObservation publication,
      @Nullable DeltaRangerNativeOperation.RawOwner owner) {
    try {
      Profile profile = Profile.capture(session, owner);
      DeltaLog$ module = DeltaLog$.MODULE$;
      requireRuntime(module, DeltaLog$.class, DeltaLog.class);
      Object cache = cache(module);
      Object map = null;
      if (cache != null) {
        map = nativeCacheMap(cache);
      }
      Path raw = new Path(publication.location());
      URI uri = raw.toUri();
      String scheme = uri.getScheme();
      boolean s3 = "s3a".equals(scheme);
      if (!raw.isAbsolute()
          || uri.getQuery() != null
          || uri.getFragment() != null
          || uri.getUserInfo() != null
          || uri.getPort() != -1) throw denied();
      if (s3) {
        String bucket = uri.getAuthority();
        if (bucket == null
            || !bucket.matches("[a-z0-9][a-z0-9.-]*")
            || uri.getPath().equals("/")
            || uri.getPath().isEmpty()) throw denied();
        validateS3(profile.hadoop, bucket);
      } else {
        if ((scheme != null && !"file".equals(scheme)) || uri.getAuthority() != null)
          throw denied();
        if (!"file:///".equals(profile.hadoop.get("fs.defaultFS", "file:///"))
            || !LocalFileSystem.class
                .getName()
                .equals(profile.hadoop.get("fs.file.impl", LocalFileSystem.class.getName()))
            || FileSystem.getFileSystemClass("file", profile.hadoop) != LocalFileSystem.class)
          throw denied();
      }
      Path rawLog = module.logPathFor(raw);
      requireRuntime(rawLog, Path.class, FileSystem.class);
      FileSystem fs = rawLog.getFileSystem(profile.hadoop);
      if (s3) {
        requireNamed(
            fs,
            S3 + "S3AFileSystem",
            Class.forName(S3 + "S3AFileSystem", false, FileSystem.class.getClassLoader()));
        validateS3(fs.getConf(), uri.getAuthority());
        for (String key : STORAGE)
          if (!profile.hadoop.get(key).equals(fs.getConf().get(key))) throw denied();
        Object manager = fs.getClass().getMethod("getAuditManager").invoke(fs);
        boolean enabled = profile.hadoop.getBoolean("fs.s3a.audit.enabled", true);
        Class<?> anchor = fs.getClass();
        requireNamed(
            manager,
            S3 + "audit.impl." + (enabled ? "ActiveAuditManagerS3A" : "NoopAuditManagerS3A"),
            anchor);
        Object auditor = manager.getClass().getMethod("getAuditor").invoke(manager);
        requireNamed(
            auditor, S3 + "audit.impl." + (enabled ? "LoggingAuditor" : "NoopAuditor"), anchor);
      } else {
        requireRuntime(fs, LocalFileSystem.class, FileSystem.class);
        requireRuntime(
            ((LocalFileSystem) fs).getRawFileSystem(), RawLocalFileSystem.class, FileSystem.class);
      }
      Map<String, String> filesystemStamp = stamp(fs.getConf());
      Path logPath = fs.makeQualified(rawLog);
      requireRuntime(logPath, Path.class, FileSystem.class);
      scala.collection.immutable.Map<String, String> empty = Map$.MODULE$.empty();
      Tuple2<Path, scala.collection.immutable.Map<String, String>> key =
          new Tuple2<>(logPath, empty);
      requireRuntime(key, Tuple2.class, Option.class);
      Object found = map == null ? null : ((Map<?, ?>) map).get(key);
      DeltaLog log = null;
      if (found != null) {
        requireRuntime(found, DeltaLog.class, DeltaLog.class);
        log = (DeltaLog) found;
        if (log.org$apache$spark$sql$delta$DeltaLog$$sparkContext().get() != profile.context)
          throw denied();
        Map<String, String> creator = safeOptions(log.allOptions());
        // DeltaLog.apply produces this stock lazy filterKeys view, including on ordinary reads.
        // Its source creator map is checked first. This is an independently cached ordinary log,
        // not authority to traverse a candidate-supplied arbitrary map or native constructor.
        Object filesystemOptions = log.options();
        if (filesystemOptions != null
            && filesystemOptions
                .getClass()
                .getName()
                .equals("scala.collection.immutable.MapLike$$anon$1")) {
          requireNamed(
              filesystemOptions, "scala.collection.immutable.MapLike$$anon$1", Option.class);
        } else {
          safeOptions(log.options());
        }
        if (!log.options().isEmpty()) throw denied();
        if (!creator.isEmpty() && (creator.size() != 1 || !creator.containsKey("path")))
          throw denied();
        if (!creator.isEmpty()) {
          Path creatorPath = new Path(creator.get("path"));
          if (!creatorPath.isAbsolute()
              || !fs.makeQualified(creatorPath).equals(logPath.getParent())) throw denied();
        }
        requireRuntime(log.logPath(), Path.class, FileSystem.class);
        requireRuntime(log.dataPath(), Path.class, FileSystem.class);
        if (!log.logPath().equals(logPath) || !log.dataPath().equals(logPath.getParent()))
          throw denied();
      }
      return new CacheObservation(
          session, publication, profile, module, cache, map, key, log, fs, filesystemStamp);
    } catch (ReflectiveOperationException | IOException | RuntimeException | LinkageError failure) {
      throw denied();
    }
  }

  static void requireRuntime(@Nullable Object value, Class<?> expected, Class<?> anchor) {
    if (value == null || value.getClass() != expected) throw denied();
    Class<?> type = value.getClass();
    CodeSource origin = anchor.getProtectionDomain().getCodeSource();
    if (origin == null
        || type.getClassLoader() != anchor.getClassLoader()
        || !origin.equals(type.getProtectionDomain().getCodeSource())) throw denied();
  }

  static void requireSession(SparkSession session) {
    requireSession(session, null);
  }

  static void requireSession(
      SparkSession session, @Nullable DeltaRangerNativeOperation.RawOwner owner) {
    requireRuntime(session, SparkSession.class, SparkSession.class);
    DeltaRangerNativeOperation operation = DeltaRangerNativeOperation.current();
    if (owner != null
        && (operation == null
            || operation.rawOwner() != owner
            || !operation.active()
            || owner.session().sharedState() != session.sharedState())) throw denied();
    if (session.sparkContext().isStopped()
        || (operation != null && owner == null)
        || OptimisticTransaction.getActive().isDefined()) throw denied();
  }

  private static Object nativeCacheMap(Object cache) throws ReflectiveOperationException {
    ClassLoader loader = DeltaLog.class.getClassLoader();
    // These fixed native names must survive the companion's Ranger-private Guava relocation.
    // Runtime joining avoids both constant folding and relocation of complete class-name literals.
    String prefix = String.join(".", "com", "google", "common", "cache");
    Class<?> anchor = Class.forName(prefix + ".CacheBuilder", false, loader);
    Class<?> contract = Class.forName(prefix + ".Cache", false, loader);
    Class<?> implementation = Class.forName(prefix + ".LocalCache$LocalManualCache", false, loader);
    Class<?> mapType = Class.forName(prefix + ".LocalCache", false, loader);
    requireNativeType(contract, anchor);
    requireNativeType(implementation, anchor);
    requireNativeType(mapType, anchor);
    requireRuntime(cache, implementation, anchor);
    Method asMap = contract.getMethod("asMap");
    if (!contract.isInterface()
        || !Modifier.isPublic(contract.getModifiers())
        || asMap.getDeclaringClass() != contract
        || !Modifier.isPublic(asMap.getModifiers())
        || Modifier.isStatic(asMap.getModifiers())
        || asMap.getParameterCount() != 0
        || asMap.getReturnType() != ConcurrentMap.class) throw denied();
    Object map = asMap.invoke(cache);
    requireRuntime(map, mapType, anchor);
    return map;
  }

  private static void requireNativeType(Class<?> type, Class<?> anchor) {
    CodeSource origin = anchor.getProtectionDomain().getCodeSource();
    if (origin == null
        || type.getClassLoader() != anchor.getClassLoader()
        || !origin.equals(type.getProtectionDomain().getCodeSource())) throw denied();
  }

  private static Object cache(DeltaLog$ module) throws ReflectiveOperationException {
    Field field = DeltaLog$.class.getDeclaredField("deltaLogCache");
    if (field.getType() != Option.class
        || !Modifier.isPrivate(field.getModifiers())
        || Modifier.isStatic(field.getModifiers())) throw denied();
    field.setAccessible(true);
    synchronized (module) {
      Object value = field.get(module);
      if (value == None$.MODULE$) return null;
      requireRuntime(value, Some.class, Option.class);
      Object result = ((Some<?>) value).get();
      if (result == null) throw denied();
      return result;
    }
  }

  private static void requireNamed(Object value, String name, Class<?> anchor)
      throws ClassNotFoundException {
    requireRuntime(value, Class.forName(name, false, anchor.getClassLoader()), anchor);
  }

  private static Map<String, String> safeOptions(
      scala.collection.immutable.Map<String, String> options) {
    if (options != null && options.getClass() == CaseInsensitiveMap.class) {
      requireRuntime(options, CaseInsensitiveMap.class, DataType.class);
      CaseInsensitiveMap<String> wrapper = (CaseInsensitiveMap<String>) options;
      // Stock getters expose stored maps. Validate both before invoking map iteration/equality.
      Map<String, String> original = plainOptions(wrapper.originalMap());
      Map<String, String> lowered = plainOptions(wrapper.keyLowerCasedMap());
      if (!original.equals(lowered)) throw denied();
      return original;
    }
    return plainOptions(options);
  }

  private static Map<String, String> plainOptions(
      scala.collection.immutable.Map<String, String> options) {
    String name = options == null ? "" : options.getClass().getName();
    if (!(name.equals("scala.collection.immutable.Map$EmptyMap$")
        || name.equals("scala.collection.immutable.Map$Map1"))) throw denied();
    requireRuntime(options, options.getClass(), Option.class);
    Map<String, String> result = new LinkedHashMap<>();
    Iterator<Tuple2<String, String>> iterator = options.iterator();
    while (iterator.hasNext()) {
      Tuple2<String, String> entry = iterator.next();
      requireRuntime(entry, Tuple2.class, Option.class);
      if (entry._1() == null || entry._2() == null) throw denied();
      result.put(entry._1(), entry._2());
    }
    return result;
  }

  private static void validateS3(Configuration conf, String bucket)
      throws ReflectiveOperationException, IOException {
    if (!(S3 + "S3AFileSystem").equals(conf.get("fs.s3a.impl"))) throw denied();
    CodeSource hadoopOrigin = FileSystem.class.getProtectionDomain().getCodeSource();
    if (hadoopOrigin == null
        || !hadoopOrigin.getLocation().getPath().endsWith("/hadoop-client-api-3.3.6.jar")
        || !hadoopOrigin.equals(Path.class.getProtectionDomain().getCodeSource())
        || FileSystem.class.getClassLoader() != Path.class.getClassLoader()) throw denied();
    Class<?> s3 = Class.forName(S3 + "S3AFileSystem", false, FileSystem.class.getClassLoader());
    CodeSource source = s3.getProtectionDomain().getCodeSource();
    if (source == null
        || !source.getLocation().getPath().endsWith("/hadoop-aws-3.3.6.jar")
        || s3.getClassLoader() != FileSystem.class.getClassLoader()
        || FileSystem.getFileSystemClass("s3a", conf) != s3) throw denied();
    for (String type :
        Arrays.asList(
            "SimpleAWSCredentialsProvider",
            "DefaultS3ClientFactory",
            "audit.impl.LoggingAuditor",
            "audit.impl.NoopAuditor")) {
      Class<?> actual = Class.forName(S3 + type, false, s3.getClassLoader());
      if (actual.getClassLoader() != s3.getClassLoader()
          || !source.equals(actual.getProtectionDomain().getCodeSource())) throw denied();
    }
    Class<?> sdk =
        Class.forName("com.amazonaws.auth.BasicAWSCredentials", false, s3.getClassLoader());
    if (sdk.getProtectionDomain().getCodeSource() == null
        || !sdk.getProtectionDomain()
            .getCodeSource()
            .getLocation()
            .getPath()
            .endsWith("/aws-java-sdk-bundle-1.12.367.jar")) throw denied();
    if (!s3.getName().equals(conf.get("fs.s3a.impl"))
        || !(S3 + "SimpleAWSCredentialsProvider")
            .equals(conf.get("fs.s3a.aws.credentials.provider"))
        || !"true".equals(conf.get("fs.s3a.path.style.access"))
        || !"false".equals(conf.get("fs.s3a.connection.ssl.enabled"))) throw denied();
    for (String key : Arrays.asList("fs.s3a.endpoint", "fs.s3a.access.key", "fs.s3a.secret.key"))
      if (conf.get(key, "").isEmpty()) throw denied();
    for (Map.Entry<String, String> entry : conf) {
      String key = entry.getKey();
      if (key.startsWith("fs.s3a.bucket." + bucket + ".")
          || (((key.startsWith("fs.s3a.assumed.role.")
                      && !(key.equals("fs.s3a.assumed.role.session.duration")
                          && entry.getValue().equals("30m"))
                      && !(key.equals("fs.s3a.assumed.role.credentials.provider")
                          && entry.getValue().equals(S3 + "SimpleAWSCredentialsProvider")))
                  || key.equals("fs.s3a.session.token")
                  || key.equals("fs.s3a.delegation.token.binding")
                  || key.equals("fs.s3a.custom.signers")
                  || key.equals("fs.s3a.signing-algorithm")
                  || key.equals("fs.s3a.s3.signing-algorithm")
                  || key.equals("fs.s3a.sts.signing-algorithm")
                  || key.equals("fs.s3a.audit.request.handlers")
                  || key.contains("credential.provider.path"))
              && !entry.getValue().isEmpty())) throw denied();
    }
    if (!(S3 + "DefaultS3ClientFactory")
        .equals(conf.get("fs.s3a.s3.client.factory.impl", S3 + "DefaultS3ClientFactory")))
      throw denied();
    boolean enabled = conf.getBoolean("fs.s3a.audit.enabled", true);
    String expected = S3 + "audit.impl." + (enabled ? "LoggingAuditor" : "NoopAuditor");
    if (!expected.equals(conf.get("fs.s3a.audit.service.classname", expected))) throw denied();
  }

  private static Map<String, String> stamp(Configuration conf) {
    Map<String, String> result = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : conf)
      result.put(entry.getKey(), conf.get(entry.getKey()));
    return Collections.unmodifiableMap(result);
  }

  private static List<String> credentials(UserGroupInformation ugi) {
    Credentials credentials = ugi.getCredentials();
    List<String> result = new ArrayList<>();
    for (Token<?> token : credentials.getAllTokens()) {
      result.add(
          token.getKind().toString()
              + ":"
              + token.getService()
              + ":"
              + Base64.getEncoder().encodeToString(token.getIdentifier())
              + ":"
              + Base64.getEncoder().encodeToString(token.getPassword()));
    }
    for (Text alias : credentials.getAllSecretKeys())
      result.add(
          alias.toString()
              + ":"
              + Base64.getEncoder().encodeToString(credentials.getSecretKey(alias)));
    Collections.sort(result);
    return Collections.unmodifiableList(result);
  }

  private static SecurityException denied() {
    return new SecurityException("Raw native cache observation rejected");
  }

  /** Immutable complete current configuration stamp, never historical provenance. */
  @SuppressWarnings("ReferenceEquality") // UGI wrapper identity is part of this sampled profile.
  static final class Profile {
    private final SparkSession session;
    private final SparkContext context;
    private final SharedState sharedState;
    private final Configuration hadoop;
    private final Map<String, String> settings;
    private final Map<String, String> sql;
    private final Map<String, String> storage;
    private final UserGroupInformation ugi;
    private final List<String> credentials;

    private Profile(SparkSession session, @Nullable DeltaRangerNativeOperation.RawOwner owner)
        throws IOException {
      requireSession(session, owner);
      this.session = session;
      context = session.sparkContext();
      sharedState = session.sharedState();
      hadoop = session.sessionState().newHadoopConfWithOptions(Map$.MODULE$.empty());
      settings = stamp(hadoop);
      // SessionState overlays SQLConf wholesale. Stock LocalFileSystem/S3A do not consume the
      // unrelated Spark SQL settings; both complete maps still retain their own immutable stamps.
      Map<String, String> storageValues = new LinkedHashMap<>();
      for (Map.Entry<String, String> entry : settings.entrySet()) {
        if (!entry.getKey().startsWith("spark.") || entry.getKey().startsWith("spark.hadoop."))
          storageValues.put(entry.getKey(), entry.getValue());
      }
      storage = Collections.unmodifiableMap(storageValues);
      sql = new LinkedHashMap<>();
      Iterator<Tuple2<String, String>> entries =
          session.sessionState().conf().getAllConfs().iterator();
      while (entries.hasNext()) {
        Tuple2<String, String> entry = entries.next();
        sql.put(entry._1(), entry._2());
      }
      ugi = UserGroupInformation.getCurrentUser();
      if (!ugi.getCredentials().getAllTokens().isEmpty()) throw denied();
      credentials = credentials(ugi);
    }

    static Profile capture(SparkSession session) {
      return capture(session, null);
    }

    private static Profile capture(
        SparkSession session, @Nullable DeltaRangerNativeOperation.RawOwner owner) {
      try {
        return new Profile(session, owner);
      } catch (IOException | RuntimeException failure) {
        throw denied();
      }
    }

    void requireCurrent(SparkSession actual) {
      requireCurrent(actual, null);
    }

    void requireCurrent(SparkSession actual, @Nullable DeltaRangerNativeOperation.RawOwner owner) {
      if (actual != session) throw denied();
      Profile current = capture(actual, owner);
      if (current.context != context
          || current.sharedState != sharedState
          || current.ugi != ugi
          || !settings.equals(current.settings)
          || !sql.equals(current.sql)
          || !credentials.equals(current.credentials)) throw denied();
    }

    void requireStoragePeer(
        SparkSession peer, @Nullable DeltaRangerNativeOperation.RawOwner owner) {
      Profile current = capture(peer, owner);
      if (current.context != context
          || current.sharedState != sharedState
          || current.ugi != ugi
          || !storage.equals(current.storage)
          || !credentials.equals(current.credentials)) throw denied();
    }
  }

  /** Sampled optimistic association; an absent entry never invokes the native cache loader. */
  static final class CacheObservation {
    private final SparkSession session;
    private final PublishedObservation publication;
    private final Profile profile;
    private final DeltaLog$ module;
    @Nullable private final Object cache;
    @Nullable private final Object map;
    private final Tuple2<Path, scala.collection.immutable.Map<String, String>> key;
    @Nullable private final DeltaLog log;
    private final FileSystem filesystem;
    private final Map<String, String> filesystemStamp;

    private CacheObservation(
        SparkSession session,
        PublishedObservation publication,
        Profile profile,
        DeltaLog$ module,
        @Nullable Object cache,
        @Nullable Object map,
        Tuple2<Path, scala.collection.immutable.Map<String, String>> key,
        @Nullable DeltaLog log,
        FileSystem filesystem,
        Map<String, String> filesystemStamp) {
      this.session = session;
      this.publication = publication;
      this.profile = profile;
      this.module = module;
      this.cache = cache;
      this.map = map;
      this.key = key;
      this.log = log;
      this.filesystem = filesystem;
      this.filesystemStamp = filesystemStamp;
    }

    @Nullable
    DeltaLog log() {
      return log;
    }

    Path dataPath() {
      return key._1().getParent();
    }

    void requirePeer(SparkSession peer) {
      requirePeer(peer, null);
    }

    void requirePeer(SparkSession peer, @Nullable DeltaRangerNativeOperation.RawOwner owner) {
      profile.requireStoragePeer(peer, owner);
    }

    void requireCurrent(SparkSession actual) {
      requireCurrent(actual, null);
    }

    void requireCurrent(SparkSession actual, @Nullable DeltaRangerNativeOperation.RawOwner owner) {
      if (actual != session) throw denied();
      profile.requireCurrent(actual, owner);
      CacheObservation current = observe(actual, publication, owner);
      if (current.module != module
          || current.cache != cache
          || current.map != map
          || current.log != log
          || !current.key.equals(key)
          || current.filesystem != filesystem
          || !current.filesystemStamp.equals(filesystemStamp)) throw denied();
    }

    /** Returns no private settings or capabilities. */
    @Override
    public String toString() {
      return "Raw native cache observation";
    }
  }
}

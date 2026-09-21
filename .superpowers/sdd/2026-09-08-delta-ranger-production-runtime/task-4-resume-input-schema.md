# Post-lifecycle resume inputs (implementation contract)

Properties under `gravitino.ranger.delta.production.`: `resumeCheckpointFile`, `resumeCheckpointSha256`, `resumeBaselineFile`, `resumeBaselineSha256`, `resumePreflightFile`, `resumePreflightSha256`. Original fixture still requires `expectedMetalakeId`.

All three inputs are absolute owner-only regular nonsymlink JSON files; independent SHA-256 checked before parsing. Original checkpoint digest is fixed `90653533d333a59880459f9a88c6d4db236f38e2236abc13bffb444f6e088cda` and keeps40/63.

Baseline has exactly these fields:
```
{schemaVersion:1, stage:"post-role-lifecycle-resume-baseline",
 metalake:<fixture>, expectedMetalakeId:"986854579816189983",
 checkpointSha256:<original>, preflightSha256:<independent>,
 policyVersion:44, roleVersion:63, observations:{<key>:{status:<HTTP>,body:<complete JSON>}}}
```
Required observation keys / GET paths (segments URL encoded; query encoding Java URLEncoder):
- `metalake`: public `/api/metalakes/{metalake}`
- `catalogs`: public `/api/metalakes/{metalake}/catalogs`
- `table:<catalogName>`: public `/api/metalakes/{metalake}/catalogs/{name}/schemas/{database}/tables/{table}` for every fixture catalog
- `role:<suffix>`: public `/api/metalakes/{metalake}/roles/{rolesPrefix}{suffix}` for all12 original checkpoint roles
- `user:<key>`: public `/api/metalakes/{metalake}/users/{principal}` for all5 checkpoint users
- `group:query`: public `/api/metalakes/{metalake}/groups/{queryGroup}`
- `definition`: admin `/service/public/v2/api/servicedef/217`
- `tagService`: admin `/service/public/v2/api/service/15`
- `resourceService`: admin `/service/public/v2/api/service/16`
- `sideService`: admin `/service/public/v2/api/service/17`
- `downloader`: admin `/service/xusers/users/9`
- `roles`: admin `/service/roles/roles`
- `policies`: admin `/service/plugins/policies?serviceName={resourceService}`
- `sidePolicy`: admin `/service/public/v2/api/policy/53`
- `policyDownload`: admin `/service/plugins/secure/policies/download/{resourceService}?lastKnownVersion=-1&pluginId=delta-post-lifecycle-resume`
- `roleDownload`: admin `/service/roles/secure/download/{resourceService}?lastKnownRoleVersion=-1&pluginId=delta-post-lifecycle-resume`

Preflight has exactly fields:
```
{schemaVersion:1, stage:"post-role-lifecycle-root-preflight",
 metalake:<fixture>, expectedMetalakeId:"986854579816189983",
 mode:"retained-process"|"cold-restart",
 processIdentityVerified:true, executableInputsVerified:true,
 rangerPersistenceVerified:true, callbackOwnershipReleased:true,
 publicContinuityVerified:true, s3ExactBytesVerified:true,
 s3ObjectCount:12, s3ManifestSha256:<digest>,
 storageBaseline:"restored-original-bytes-new-mtime",
 transitionSha256:<post-column-continuity.json digest>,
 processEvidenceSha256:<digest>, rangerPersistenceEvidenceSha256:<digest>,
 executableManifestSha256:<digest>,
 coldH2Sha256:<digest only for cold-restart, otherwise null>}
```

No timestamps ignored. Array order is normalized only for response collections (roles/policies/rangerRoles), local user/group role lists and local securable-object/privilege arrays; all complete model fields preserved. Ranger list wrapper `queryTimeMS` is request telemetry only, explicitly removed; pagination fields retained. Only audit_cleanup 404 message/stack diagnostics removed. Side service `configs["ranger.plugin.audit.filters"]` JSON string parsed if present, keeping full parsed value. Download policy models are compared to full expected downloads and checked for exact IDs/generations; Admin full policy models separately exact. Runtime completes and privately records every GET response before validated event.


Coordinator fixture adds `resumeDownloaderCredentialsFile` (owner-only JSON with `username`/`password`). Downloads use this principal, independently matched to user9; public reads use configured Gravitino admin simple auth, Admin model reads use existing Ranger Admin credentials. New-route gateway constructors retain actual response bytes separately in0600 files; events reference paths without headers or credentials. The existing default gateway does not retain bodies.

The accepted generation44 Admin registry has19 policies35..52+54. The secured download has18: policy48 alone is disabled and omitted; every remaining policy model equals its complete Admin model. All19 full Ranger role models match the secured role download. Resource service DTO16 exposes policyVersion44 but no roleVersion; roleVersion63 is taken from the completed secured role download.

Commands are closed shapes: `{"operation":"close"}`, `{"operation":"snapshot","id":"safe-alphanumeric-or-dash-underscore"}`, or mutation objects with `operation` and explicit `roleSuffix`. `grant-user`/`revoke-user` additionally require `subject`. Allowed ordinary/group operations: grant/revoke-user, grant/revoke-group, grant/revoke-select, delete-role, fault-role-update. The `visibility_visibilityOnly` role permits only grant/revoke-user for `subject=visibilityOnly`. Ordinary/group user subjects: freshCallback,columnReader,visibilityOnly,groupReader. All other operations/fields/roles/subjects reject before dispatch. After any dispatched mutation failure only snapshot/close remain allowed. Every new-route event identifies its mode, preserved divergence and protected objects even when no new failure has quarantined mutation work.


## Reviewed final public handle read

The route performs 34 completed GETs covering the 33 unique baseline keys: the fixed 33-request validation set, then exactly one public `loadMetalake` GET. The final complete HTTP status/body is compared against the same sealed `metalake` observation. Before accepting the boundary, Java requires the exact gateway-qualified request multiset (including raw path/query targets), compares every retained response body, and rejects missing, duplicate, unexpected, or incomplete responses. The validated event reports `completedReadResponses=34`, `semanticComparisonsPassed=34`, and `distinctObservationKeys=33`, with public completions 24 and callback completions 10.

For this new route only, client construction uses the existing public `withVersionCheckDisabled()` builder option. This prevents the client's otherwise automatic `/api/version` GET from escaping the closed read set. Root's required executable-input preflight independently binds the exact server/client archives; the route does not claim to perform a new protocol version compatibility check. Existing routes retain their version-check behavior.

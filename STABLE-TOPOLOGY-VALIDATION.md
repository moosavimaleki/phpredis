# Internal RedisCluster topology backport for 6.3.0

Base: official `6.3.0` tag, commit `df4fab2d`.
Adapted from `ccfb0e7d`, with a narrower scope for this internal build.
The upstream PR branch remains unchanged.

## Scope

Refresh the complete slot map after MOVED, using redirect targets, known nodes,
and seeds. Build the replacement before publication, preserve retained sockets,
and close retired sockets. Keep failed discovery non-destructive, accept holes,
preserve cached-seed credentials, and protect WATCH/MULTI state. ASK remains a
temporary redirect. Missing-slot access retains the original error behavior.

The develop patch's additional READONLY/CLUSTERDOWN retry policy and recovery of
unsent commands are deliberately excluded. The release's command construction,
transport retry policy and public PHP API are retained. No RedisCmd rewrite,
background polling or new configuration options are imported.

The one-line library.c change resets connection-scoped READONLY state on
socket disconnect, including replicas retained through remapping.

## Validation

Environment: PHP 8.5.4 NTS, Redis 8.0.5, Linux. Build enables session, JSON,
igbinary, msgpack, lzf, lz4 and zstd. Runtime support on other PHP/Redis versions
has not been established by these runs.

- Redis suite: 242 passed, 10 skipped, zero failures.
- RedisCluster suite: 233 passed, 43 skipped, zero failures.
- 14 unique topology regressions passed; run the release runner separately with
  `--test topology` and `--test moved` (comma-separated filters are unsupported).
- Both focused groups passed Valgrind with USE_ZEND_ALLOC=0: zero reported
  errors and zero definitely/indirectly/possibly lost bytes.
- Real TLS/ACL/cache/persistent failover and two-slot resharding passed.
- Three repeated failovers and two-slot resharding passed for all four
  cache/persistent combinations; nonpersistent close left no orphan sockets.

Build with phpize, configure and make as documented for PhpRedis. On this
machine extracted msgpack/lz4 development packages are used from
`/tmp/phpredis-topology-test/deps/root`; the built module is `.libs/redis.so`.
Load igbinary and msgpack before redis when using `php -n`.

## Concurrent load comparison

Compared the patched build with unmodified 6.3.0 built using identical options.
Five runs per version/concurrency level, alternating version order; each PHP
worker performed 500 warmup GETs then 10,000 measured operations, 80% GET and
20% SET, 64-byte values, keys distributed across three primaries. Clients wait
at a common start barrier; connection setup is excluded. Latency samples every
11 operations cover both GET and SET. No failover was injected during load.

5,300,000 measured commands completed with zero errors or unexpected values.
Median throughput, commands/second:

| Clients | Unmodified 6.3.0 | Patched | Change |
|---|---|---|---|
| 1 | 26,329 | 26,170 | -0.6% |
| 4 | 64,947 | 58,877 | -9.3% |
| 16 | 139,977 | 133,565 | -4.6% |
| 32 | 152,411 | 168,201 | +10.4% |

This is a shared machine with variable competing load. Mixed timing results,
including lower throughput at 4 and 16 clients, do not establish performance
neutrality. Do not treat this build as having passed a production performance
gate without an isolated comparison. Correctness under this workload passed.

Raw results: `/tmp/phpredis-topology-test/stable-load-results.json`.
Logs: `/tmp/phpredis-stable-{build,redis,cluster,tls,load}.log` and
`/tmp/phpredis-stable-valgrind-{topology,moved}.log`.

The real-cluster integration script intentionally changes roles and slot
ownership. Run `tests/clusterTopology.php` only against a disposable local
six-node cluster; see its header for arguments and authentication/TLS options.

## Follow-up ownership review

A remap regression was reproduced with two objects sharing a persistent PHP
stream when `redis.pconnect.pooling_enabled=0`. Force-closing a retired node
invalidated the other object's stream pointer and caused a use-after-free.
Retired sockets now use non-forced disconnect in this shared-stream mode;
ordinary connections and pooled persistent cleanup retain forced retirement.
The new regression verifies both objects remain usable through remapping.
Valgrind reported zero errors and zero lost allocations after the fix.

A separate, pre-existing issue was reproduced on unmodified 6.3.0: explicitly
calling `close()` on one object can invalidate another object's shared stream
in this same pooling-disabled configuration, even without topology changes.
That general connection-lifecycle issue is not changed by this topology patch.
The shared-stream regression uses normal object destruction for cleanup.

The full Redis (242 passed) and RedisCluster (233 passed) suites, focused
Valgrind cases, plain and TLS/ACL failovers, and two-slot resharding passed.
No additional load test was run for this ownership correction.


## Historical follow-up on the six patch risks (superseded below)

Reviewed and corrected only the master worktree, based on `eaf023f7`.
The develop worktree was not modified. These changes are not committed yet.

Five of the six scenarios were reproduced against the pre-change module:

- A READONLY connection retained from a promoted replica could conceal a later
  routing change. Before using it as the command's primary connection, send
  READWRITE outside MULTI. Replica distribution still sends READONLY as usual.
- A valid but stale SLOTS snapshot could discard the newer MOVED destination.
  If the source then disappeared, the command failed despite a healthy target.
  Apply the validated hint whenever the new snapshot disagrees with it.
- After hint-only fallback, routing could keep working for that slot indefinitely
  while primary/replica metadata stayed incomplete. Mark the topology dirty,
  rebuild owner ranges, and attempt complete discovery on the next safe command.
  Transient ASK nodes remain owned for cleanup but are omitted from KEYS and
  _masters until they own slots. The ASK registry regression is covered too.
- Recovery of an uncovered slot left a stale persistent cache for the next
  object. Invalidate it before this discovery attempt as in the MOVED path.
- The newly introduced discovery timeout could force-close a shared data
  stream, causing a second object's GET to segfault. Valgrind confirmed the
  invalid read before correction. A routing-loop timeout has no pending data
  reply to discard, so it now preserves that data stream. Communication-error
  cleanup retains its existing behavior.

The sixth concern, replaying an already completed part of a distributed write,
was not reproduced. A new MSET regression verifies that the completed part is
not resent when another part receives MOVED. Discovery remains bounded per
internal command loop, not per PHP call containing multiple wire commands.

Six new regression methods were added; existing ASK and uncovered-slot tests
were strengthened. A MOVED hint now remains usable even when the received
snapshot omits that slot; a different uncovered slot still raises an error.

Validation with JSON/session/igbinary/msgpack/lzf/lz4/zstd enabled:

- Redis: 242 passed, 10 skipped, zero failures.
- RedisCluster: 239 passed, 43 skipped, zero failures.
- 18 topology test executions under Valgrind: zero errors and zero
  definitely/indirectly/possibly lost bytes (reachable allocations remain).
- Plain and TLS/ACL/cache/persistent real-cluster round-trip failovers and
  two-slot resharding passed. A further real-cluster run first read from a
  replica before promoting it, and then repeated failover and resharding.
- No new load test was run.

Evidence: `/tmp/phpredis-master-fix/`, including `before.so`, `red.log`,
`timeout-red-vg.log`, `final-cluster.log`, `final-vg.log`, `real.log`, `tls.log`,
`real-readonly.log`, and `build.log`.

This follow-up covers the six stated hypotheses. The older explicit-close
shared-stream bug and ASK/replica-distribution bug remain outside this change.
The previously reported strict deadline issue inside a chunked discovery reply
also remains; preserving the stream on timeout does not make parsing obey an
absolute deadline.

## Historical directed SCAN and READWRITE follow-up (superseded below)

Two further regressions introduced by the preceding corrections were reproduced
and fixed in the master worktree:

- With hint-only routing marked dirty, SCAN addressed to a host/port array could
  refresh the map and send its cursor to a different primary. Explicit-node SCAN
  now uses the existing directed-send path and bypasses replica distribution.
  Scalar key-based SCAN retains its normal routing. The regression also covers
  SCAN_RETRY with an empty first page and replica distribution enabled, and
  verifies that the following key command still repairs the dirty topology.
- A denied READWRITE consumed only the error's type byte, leaving its body in
  the stream and making subsequent resets fail. The reset now uses the existing
  reply reader, consumes the error, preserves exceptions, and clears the readonly
  flag only after +OK. The regression denies the first reset and permits the
  next one, checking that the original SET is sent exactly once.

Both new regressions failed before the fixes. After rebuilding with all optional
backends enabled, 22 focused topology tests passed. Eight sensitive cases,
including the strengthened SCAN test, passed under Valgrind with zero errors
and zero definitely/indirectly/possibly lost bytes. Full server-backed suites
and real-cluster failovers were not rerun for these two corrections.

Evidence: `/tmp/phpredis-master-fix/additional-review.php`,
`additional-build.log`, and `additional-vg.log`.


## Final simplification: refresh or fail

The final implementation removes the hint-only topology policy, the
`topology_dirty` field, and the full slot-range reconstruction routine.
A failed MOVED refresh raises an exception and leaves the published map intact.
A successful refresh publishes the validated snapshot; the current request
follows its explicit MOVED destination even if that snapshot is stale. Further
redirects in the same command follow their targets without another discovery.
Replica distribution cannot override a MOVED destination.

Missing-slot access uses the original error behavior, so no additional discovery
is introduced there. The dirty-map SCAN special case and its test were removed
along with the state that required it. The earlier cache-hole recovery test was
also removed because that recovery feature no longer exists. READWRITE reply
consumption, safe socket reuse/retirement, cache invalidation on MOVED, temporary
node ownership, validation, and WATCH/MULTI protection remain.

The failed-discovery tests now require an exception, unchanged primary routing,
and a successful later retry once discovery recovers. A separate regression
covers a stale snapshot plus replica distribution: the explicit MOVED target
must serve the request without being published as a primary slot owner.

Final build enables JSON/session/igbinary/msgpack/lzf/lz4/zstd. All 21 focused
topology regressions pass under Valgrind with zero errors and zero
definitely/indirectly/possibly lost bytes. Redis passes 242 tests and
RedisCluster passes 240 tests, with zero failures. The disposable real cluster
passes three round-trip failovers and a two-slot reshard with slot caching and
persistent connections enabled. Evidence is in `/tmp/phpredis-master-fix/`:
`simple-build.log`, `simple-vg.log`, `simple-cluster.log`, `simple-redis.log`,
and `simple-real.log`. Earlier sections record historical iterations and do
not describe the final fallback policy.

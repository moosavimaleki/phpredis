# Cluster topology recovery

This patch keeps the existing slot array, SLOTS parser, and command API.
An ordinary MOVED response triggers at most one full discovery per internal
send loop, trying the advertised destination, known nodes/replicas, and original
seeds. Discovery uses private nonpersistent streams with the client's AUTH and
TLS context. Invalid snapshots leave the previous map intact; valid snapshots
replace the complete map and retire the previous connections. Uncovered slots
remain NULL and produce an exception when requested.

The command follows MOVED only when the new snapshot agrees with its target.
ASK uses a separately owned temporary connection, without adding a primary.
WATCH, MULTI, subscriptions, and explicitly addressed commands do not enter
general topology recovery. A WATCH failure is reported to the caller; earlier
successful watches remain available for explicit UNWATCH.

For ordinary primary routing, two failed connection-establishment attempts on
a disconnected socket trigger one discovery before sending the payload. This
counter is local to that invocation, not shared across commands or pool members.
AUTH errors and failures while writing or reading do not replay the command.
In particular, a missing write response can mean that the operation executed;
the application decides whether retrying is appropriate.

Connection failure during an already open exchange fails that command. An
application pool can discard that RedisCluster object. The inspected emscore
pool already invalidates a member when an exception leaves its run callback;
no application pool changes are included here. Exceptions swallowed inside a
callback do not activate that behavior, and unfinished WATCH/MULTI state is
the callback's responsibility.

There is no background watcher or DNS polling. Idle objects and silent replica
changes are discovered only after a trigger or object replacement. Original
hostname seeds are retained even on slot-cache hits. At least one discoverable
address must remain reachable. Existing per-read timeouts are not an absolute
deadline for a response sent in small chunks.

Persistent pooling remains distinct from an application object pool. Active
streams leased through redis.pconnect.pooling_enabled are closed on retirement
to avoid carrying old READONLY state into a new routing generation. Legacy
persistent streams without that pooling can be shared by live objects; this
patch does not introduce shared-stream ownership tracking or make concurrent
use of one RedisCluster object safe.

## Tests

Load the freshly built extension and its enabled serializer dependencies:

```sh
php -n -d extension=igbinary -d extension=msgpack -d extension=modules/redis.so tests/clusterTopology.php
```

`clusterTopology.php` runs deterministic RESP peers on ephemeral local ports.
`clusterTopologyPool.php /path/to/emscore/src` additionally requires Swoole and
tests exception propagation, isolation of a failed member, a healthy member,
and borrowing a replacement using the real emscore pool classes.

`clusterTopologyFailover.php` performs three actual failovers and writes test
keys. Run it only against a disposable six-node cluster (three primaries and
three replicas) on consecutive ports. Set TOPOLOGY_PORT to its first port.
Arguments select slot caching and persistent connections, respectively:

```sh
TOPOLOGY_PORT=18379 php -d extension=modules/redis.so tests/clusterTopologyFailover.php 0 0
```

For TLS set TOPOLOGY_TLS=1, TOPOLOGY_CA, TOPOLOGY_USER, and TOPOLOGY_PASSWORD.

Validation on PHP 8.5.4 and Redis 8.0.5:

- Redis suite: 242 passed, 10 skipped; RedisCluster: 219 passed, 43 skipped.
- All 19 deterministic topology scenarios passed; the original 6.3.0 build
  fails the first scenario (MOVED to a new primary without a replica table).
- The emscore 38035ce4 pool passed with Swoole 6.2.2 and coroutine hooks enabled.
- Three failovers each passed with nonpersistent connections, slot cache plus
  persistent pooling, and TLS/ACL plus slot cache. INCR results and primary
  enumeration stayed correct; nonpersistent close left no extra data clients.
- Valgrind on all deterministic scenarios: zero errors, zero definitely,
  indirectly, or possibly lost bytes.
- Eight alternating original/patch runs of 200,000 GETs: median 3.689 seconds
  original, 3.667 seconds patched. The host is noisy; this is a smoke comparison,
  not proof of zero overhead or a production throughput guarantee.

The final suite runs used a fixed built module throughout. Rebuilding a shared
module while PHP processes have it mapped can invalidate the test run.

## Review of 2abc3157

The follow-up review compared the patch with the original `df4fab2d` build,
including malformed responses. It confirmed and fixed three introduced defects:

| Trigger | Before the fix | Corrected behavior |
| --- | --- | --- |
| MOVED names a different slot and the new map omits the command's slot | PHP exits with SIGSEGV (139); the original build throws | Reject mismatched MOVED/ASK before discovery or redirect; retain the map and allow the next command |
| SLOTS contains a RESP null array as a range row | PHP exits with SIGSEGV (139); the original build throws | Reject the row before endpoint normalization can index its NULL element array; retain the previous map |
| A second persistent client reaches the PHP connection pool limit | Counts local capacity as two network failures and runs discovery | Return a distinct internal connection-limit result; fail the command without discovery or payload, preserving the healthy client's lease |

The first two reproducers were run against both the original build and the
unfixed patch. The capacity reproducer observed three SLOTS calls before the
fix and two after it (the two constructor discoveries). No error-message
matching, new retry loop, or application-pool change is involved. All existing
callers of `redis_sock_connect` treat non-success results as failures; the
topology retry path counts only transport `FAILURE` results.

Validation after these fixes, on the same PHP 8.5.4 / Redis 8.0.5 environment:

- All 23 deterministic topology scenarios passed, including both mismatched
  redirect types, null-array discovery, and persistent pool exhaustion.
- Redis: 242 passed, 10 skipped. RedisCluster: 219 passed, 43 skipped.
  Session tests used explicit extension paths for their child processes.
- The real emscore pool test passed with Swoole 6.2.2 and coroutine hooks.
- Three failovers passed with nonpersistent connections; another three passed
  with slot caching and persistent pooling. Counters reached six without
  duplicated INCRs; nonpersistent close left only the six observer clients.
- Valgrind over all 23 deterministic scenarios reported zero errors and zero
  definitely, indirectly, or possibly lost bytes.

These checks do not establish that no unknown bugs remain. Legacy shared
persistent-stream limitations described above are unchanged. TLS/ACL failover
results in the preceding section belong to the initial patch validation;
they were not rerun for this follow-up.

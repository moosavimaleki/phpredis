<?php
/* Destructive integration test: use a dedicated, disposable six-node cluster.
 * TOPOLOGY_PORT: first of six consecutive ports, 3 primaries + 3 replicas.
 * TOPOLOGY_TLS=1 requires TOPOLOGY_CA, TOPOLOGY_USER, TOPOLOGY_PASSWORD.
 * Arguments: cache_slots (0/1), persistent (0/1). Performs three failovers. */
function check($condition, $message) { if (!$condition) throw new RuntimeException($message); }
function until(callable $condition, $message) {
    $deadline = microtime(true) + 15;
    do { if ($condition()) return; usleep(20000); } while (microtime(true) < $deadline);
    throw new RuntimeException($message);
}
$tls = getenv('TOPOLOGY_TLS') === '1';
$start = (int)(getenv('TOPOLOGY_PORT') ?: ($tls ? 17479 : 17379));
$context = $tls ? ['stream' => ['cafile' => getenv('TOPOLOGY_CA'), 'verify_peer' => true, 'peer_name' => 'localhost']] : null;
$ports = range($start,$start+5);
$peers=[];
foreach ($ports as $port) {
    $r = new Redis; $r->connect($tls ? 'tls://127.0.0.1' : '127.0.0.1', $port, 2, null, 0, 2, $context);
    if ($tls) $r->auth([getenv('TOPOLOGY_USER'),getenv('TOPOLOGY_PASSWORD')]);
    $peers[$port]=$r;
}
function owner($peers, $slot) {
    foreach (reset($peers)->rawCommand('CLUSTER','SLOTS') as $range)
        if ($range[0] <= $slot && $range[1] >= $slot) return $range[2][1];
    throw new RuntimeException('slot has no owner');
}
function connections($peers) {
    $count=0;
    foreach ($peers as $r) {
        foreach (explode("\n", trim($r->rawCommand('CLIENT','LIST'))) as $client) {
            if (preg_match('/flags=([^ ]+)/', $client, $flags) && !strpbrk($flags[1], 'MS')) $count++;
        }
    }
    return $count;
}
function redirectCount($r) { return (int)($r->info('errorstats')['errorstat_MOVED']['count'] ?? 0); }
$key = '{foo}:topology-integration'; $slot=12182;
$cache = ($argv[1] ?? '0') === '1'; $persistent = ($argv[2] ?? '0') === '1';
ini_set('redis.clusters.cache_slots', $cache ? '1' : '0');
$c = new RedisCluster(null, array_map(fn($p) => '127.0.0.1:'.$p, range($start,$start+2)), 2, 2, $persistent, $tls ? [getenv('TOPOLOGY_USER'),getenv('TOPOLOGY_PASSWORD')] : null, $tls ? $context['stream'] : null);
$c->set($key, 0); $expected=0;
foreach (range(1,3) as $round) {
    usleep(1200000); /* Let Redis finish the preceding election before reversing it. */
    $old=owner($peers,$slot);
    $id=$peers[$old]->rawCommand('CLUSTER','MYID');
    until(fn() => count($peers[$old]->rawCommand('CLUSTER','REPLICAS',$id))===1, 'expected one replica');
    $replicas=$peers[$old]->rawCommand('CLUSTER','REPLICAS',$id);
    preg_match('/127[.]0[.]0[.]1:(\d+)/', $replicas[0], $match); $new=(int)$match[1];
    until(fn() => (int)($peers[$new]->info('replication')['master_link_status']==='up'), 'replication link did not recover');
    check($c->incr($key)===++$expected, 'pre-failover INCR');
    check($peers[$new]->rawCommand('CLUSTER','FAILOVER')!==false, 'failover rejected');
    until(function() use($peers,$new,$old,$slot) {
        foreach ($peers as $r) {
            $found=false;
            foreach ($r->rawCommand('CLUSTER','SLOTS') as $range) {
                if ($range[0] <= $slot && $slot <= $range[1]) $found=$range[2][1]===$new;
            }
            if (!$found) return false;
        }
        return $peers[$old]->info('replication')['role']==='slave';
    }, 'cluster views did not converge');
    check($c->incr($key)===++$expected, 'recovered INCR was lost or repeated');
    check($c->get($key)===(string)$expected, 'read after failover');
    $masters=array_column($c->_masters(),1);sort($masters);
    $actual=array_unique(array_map(fn($range) => $range[2][1], $peers[$new]->rawCommand('CLUSTER','SLOTS')));sort($actual);
    check($masters===array_values($actual), 'masters registry stale');
    $count=connections($peers);
    check($count<=12, 'orphan data connections: '.$count);
    echo "failover $round $old->$new; clients=$count; counter=$expected\n";
}
$c->close();
if (!$persistent) check(connections($peers)===6, 'nonpersistent close left orphan connections');
echo 'PASS cache='.(int)$cache.' persistent='.(int)$persistent."\n";

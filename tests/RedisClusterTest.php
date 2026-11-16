<?php defined('PHPREDIS_TESTRUN') or die("Use TestRedis.php to run tests!\n");

require_once __DIR__ . "/RedisTest.php";
require_once __DIR__ . "/ClusterTopologyFixture.php";

/**
 * Most RedisCluster tests should work the same as the standard Redis object
 * so we only override specific functions where the prototype is different or
 * where we're validating specific cluster mechanisms
 */
class Redis_Cluster_Test extends Redis_Test {
    private $redis_types = [
        Redis::REDIS_STRING,
        Redis::REDIS_SET,
        Redis::REDIS_LIST,
        Redis::REDIS_ZSET,
        Redis::REDIS_HASH
    ];

    private $failover_types = [
        RedisCluster::FAILOVER_NONE,
        RedisCluster::FAILOVER_ERROR,
        RedisCluster::FAILOVER_DISTRIBUTE
    ];

    protected static array $seeds = [];

    private static array  $seed_messages = [];
    private static string $seed_source = '';

    public function testTopologyReadwriteErrorLeavesConnectionUsable() {
        $f = new ClusterTopologyFixture(function ($n) {
            return [
                ['CLUSTER SLOTS' => [[[0, 16383, $n[0], $n[1]]]],
                 'SET' => [['error' => "MOVED 12182 {$n[1][0]}:{$n[1][1]}" ]]],
                ['CLUSTER SLOTS' => [[[0, 16383, $n[1]]]], 'GET' => ['replica-value'],
                 'READWRITE' => [['error' => 'NOPERM readwrite denied'], ['raw' => "+OK\r\n"]]],
            ];
        });
        try {
            $c = new RedisCluster(null, [implode(':', $f->nodes[0])], 1, 1);
            $c->setOption(RedisCluster::OPT_SLAVE_FAILOVER, RedisCluster::FAILOVER_DISTRIBUTE_SLAVES);
            $this->assertEquals('replica-value', $c->get('{foo}'));
            $c->setOption(RedisCluster::OPT_SLAVE_FAILOVER, RedisCluster::FAILOVER_NONE);
            $this->assertThrowsMatch($c, function ($c) { $c->set('{foo}', 'value'); }, '/promoted|NOPERM/');
            $this->assertTrue($c->set('{foo}', 'value'));
            $this->assertEquals('replica-value', $c->get('{foo}'));
            $calls = json_decode($c->rawCommand($f->nodes[1], 'FIXTURE'), true);
            $this->assertEquals(2, $calls[1]['READWRITE']);
            $this->assertEquals(1, $calls[1]['SET']);
            $c->close();
        } finally { $f->close(); }
    }

    public function testTopologyDistributedWriteDoesNotReplayCompletedPart() {
        $f = new ClusterTopologyFixture(function ($n) {
            $old = [[0, 16383, $n[0]]];
            $new = [[0, 8191, $n[1]], [8192, 16383, $n[0]]];
            return [['CLUSTER SLOTS' => [$old], 'MSET' => [['raw' => "+OK\r\n"],
                        ['error' => "MOVED 5061 {$n[1][0]}:{$n[1][1]}"]]],
                    ['CLUSTER SLOTS' => [$new], 'MSET' => [['raw' => "+OK\r\n"]]]];
        });
        try {
            $c = new RedisCluster(null, [implode(':', $f->nodes[0])], 1, 1);
            $this->assertTrue($c->mset(['{foo}' => 'a', '{bar}' => 'b']));
            $calls = json_decode($c->rawCommand($f->nodes[0], 'FIXTURE'), true);
            $this->assertEquals(2, $calls[0]['MSET']);
            $this->assertEquals(1, $calls[1]['MSET']);
            $c->close();
        } finally { $f->close(); }
    }

    public function testTopologyPromotionResetsReadonlyConnection() {
        $f = new ClusterTopologyFixture(function ($n) {
            $old = [[0, 16383, $n[0], $n[1]]];
            $promoted = [[0, 16383, $n[1]]];
            $new = [[0, 16383, $n[2]]];
            return [
                ['CLUSTER SLOTS' => [$old], 'SET' => [['error' => "MOVED 12182 {$n[1][0]}:{$n[1][1]}"]]],
                ['__track_readonly' => true, '__readonly_reply' => 'stale',
                 'CLUSTER SLOTS' => [$promoted], 'GET' => [['error' => "MOVED 12182 {$n[2][0]}:{$n[2][1]}"]]],
                ['CLUSTER SLOTS' => [$new], 'GET' => ['fresh']],
            ];
        });
        try {
            $c = new RedisCluster(null, [implode(':', $f->nodes[0])], 1, 1);
            $c->setOption(RedisCluster::OPT_SLAVE_FAILOVER, RedisCluster::FAILOVER_DISTRIBUTE_SLAVES);
            $this->assertEquals('stale', $c->get('{foo}'));
            $c->setOption(RedisCluster::OPT_SLAVE_FAILOVER, RedisCluster::FAILOVER_NONE);
            $this->assertTrue($c->set('{foo}', 'value'));
            $this->assertEquals('fresh', $c->get('{foo}'));
            $c->close();
        } finally { $f->close(); }
    }

    public function testTopologyFailedDiscoveryCanRetry() {
        $f = new ClusterTopologyFixture(function ($n) {
            $old = [[0, 16383, $n[0]]];
            $new = [[0, 16383, $n[1]]];
            return [['CLUSTER SLOTS' => [$old, [], $new],
                     'GET' => [['error' => "MOVED 12182 {$n[1][0]}:{$n[1][1]}"]]],
                    ['CLUSTER SLOTS' => [[], $new], 'GET' => ['new']]];
        });
        try {
            $c = new RedisCluster(null, [implode(':', $f->nodes[0])], 1, 1);
            $this->assertThrowsMatch($c, function ($c) { $c->get('{foo}'); }, '/refresh/');
            $this->assertEquals([$f->nodes[0]], $c->_masters());
            $this->assertEquals('new', $c->get('{foo}'));
            $this->assertEquals([$f->nodes[1]], $c->_masters());
            $c->close();
        } finally { $f->close(); }
    }

    public function testTopologyFreshMovedHintSurvivesStaleSnapshot() {
        $f = new ClusterTopologyFixture(function ($n) {
            $old = [[0, 16383, $n[0]]];
            return [
                ['CLUSTER SLOTS' => [$old], 'GET' => [['error' => "MOVED 12182 {$n[1][0]}:{$n[1][1]}", 'stop' => true]]],
                ['CLUSTER SLOTS' => [$old], 'GET' => ['new']],
            ];
        });
        try {
            $c = new RedisCluster(null, [implode(':', $f->nodes[0])], .2, .2);
            $this->assertEquals('new', $c->get('{foo}'));
            $peer = new Redis;
            $peer->connect(...$f->nodes[1]);
            $calls = json_decode($peer->rawCommand('FIXTURE'), true);
            $peer->close();
            $this->assertEquals(1, $calls[0]['GET']);
            $this->assertEquals([$f->nodes[0]], $c->_masters());
            $c->close();
        } finally { $f->close(); }
    }

    public function testTopologyMovedTargetBypassesReplicaDistribution() {
        $f = new ClusterTopologyFixture(function ($n) {
            $old = [[0, 16383, $n[0], $n[2]]];
            return [['CLUSTER SLOTS' => [$old]],
                    ['CLUSTER SLOTS' => [$old], 'GET' => ['target-value']],
                    ['GET' => [['error' => "MOVED 12182 {$n[1][0]}:{$n[1][1]}" ]]]];
        });
        try {
            $c = new RedisCluster(null, [implode(':', $f->nodes[0])], .2, .2);
            $c->setOption(RedisCluster::OPT_SLAVE_FAILOVER, RedisCluster::FAILOVER_DISTRIBUTE_SLAVES);
            $this->assertEquals('target-value', $c->get('{foo}'));
            $this->assertEquals([$f->nodes[0]], $c->_masters());
            $c->close();
        } finally { $f->close(); }
    }

    public function testTopologyTimeoutPreservesSharedStream() {
        $pool = ini_get('redis.pconnect.pooling_enabled');
        ini_set('redis.pconnect.pooling_enabled', '0');
        $f = new ClusterTopologyFixture(function ($n) {
            $old = [[0, 8191, $n[1]], [8192, 16383, $n[0]]];
            return [['CLUSTER SLOTS' => [$old], 'GET' => [['error' => "MOVED 12182 {$n[1][0]}:{$n[1][1]}"], 'old']],
                    ['GET' => ['old'], 'CLUSTER SLOTS' => [['reply' => $old, 'delay' => 90000]]]];
        });
        try {
            $seed = implode(':', $f->nodes[0]);
            $a = new RedisCluster(null, [$seed], .03, .03, true);
            $this->assertEquals('old', $a->get('{bar}'));
            $b = new RedisCluster(null, [$seed], .03, .03, true);
            $this->assertEquals('old', $b->get('{bar}'));
            $this->assertThrowsMatch($a, function ($c) { $c->get('{foo}'); });
            usleep(100000); /* Let the timed-out fixture probe finish. */
            $this->assertEquals('old', $b->get('{bar}'));
        } finally { unset($a, $b); $f->close(); ini_set('redis.pconnect.pooling_enabled', $pool); }
    }

    public function testMovedRefreshesWholeTopology() {
        $fixture = new ClusterTopologyFixture(function ($nodes) {
            $before = [[0, 16383, $nodes[0]]];
            $after = [[0, 16383, $nodes[1], $nodes[0]]];
            return [
                ['CLUSTER SLOTS' => [$before, $after],
                 'GET' => [['error' => "MOVED 12182 {$nodes[1][0]}:{$nodes[1][1]}"]]],
                ['CLUSTER SLOTS' => [$after], 'GET' => ['value']],
            ];
        });
        $class = $this->getRedisClusterClass();
        $client = null;
        try {
            $seed = implode(':', $fixture->nodes[0]);
            $client = new $class(null, [$seed], 1, 1);
            $this->assertEquals('value', $client->get('{foo}first'));
            $this->assertEquals('value', $client->get('{bar}second'));
            $this->assertEquals([$fixture->nodes[1]], $client->_masters());
            $calls = json_decode($client->rawCommand($fixture->nodes[1], 'FIXTURE'), true);
            $this->assertEquals(1, $calls[0]['GET']);
        } finally {
            if ($client) $client->close();
            $fixture->close();
        }
    }

    public function testMovedBetweenKnownMasters() {
        $fixture = new ClusterTopologyFixture(function ($nodes) {
            $before = [[0, 8191, $nodes[1]], [8192, 16383, $nodes[0]]];
            $after = [[0, 16383, $nodes[1], $nodes[0]]];
            return [
                ['CLUSTER SLOTS' => [$before, $after],
                 'GET' => [['error' => "MOVED 12182 {$nodes[1][0]}:{$nodes[1][1]}"],
                           ['error' => "MOVED 9995 {$nodes[1][0]}:{$nodes[1][1]}"]]],
                ['CLUSTER SLOTS' => [$after], 'GET' => ['value']],
            ];
        });
        $class = $this->getRedisClusterClass();
        $client = null;
        try {
            $client = new $class(null, [implode(':', $fixture->nodes[0])], 1, 1);
            $this->assertEquals('value', $client->get('{foo}'));
            $this->assertEquals('value', $client->get('{qux}'));
            $calls = json_decode($client->rawCommand($fixture->nodes[1], 'FIXTURE'), true);
            $this->assertEquals(1, $calls[0]['GET']);
            $this->assertEquals([$fixture->nodes[1]], $client->_masters());
        } finally {
            if ($client) $client->close();
            $fixture->close();
        }
    }

    public function testFailedTopologyRefreshPreservesRouting() {
        $fixture = new ClusterTopologyFixture(function ($nodes) {
            $before = [[0, 16383, $nodes[0]]];
            $broken = [[0, 8191, $nodes[1]], ['bad', 16383, $nodes[1]]];
            return [
                ['CLUSTER SLOTS' => [$before, $broken],
                 'GET' => [['error' => "MOVED 12182 {$nodes[1][0]}:{$nodes[1][1]}"], 'old-route']],
                ['GET' => ['new-route']],
            ];
        });
        $class = $this->getRedisClusterClass();
        $client = null;
        try {
            $client = new $class(null, [implode(':', $fixture->nodes[0])], 1, 1);
            $this->assertThrowsMatch($client, function ($c) { $c->get('{foo}'); }, '/refresh/');
            $this->assertEquals('old-route', $client->get('{bar}'));
            $this->assertEquals([$fixture->nodes[0]], $client->_masters());
            $calls = json_decode($client->rawCommand($fixture->nodes[0], 'FIXTURE'), true);
            $this->assertFalse(isset($calls[1]['GET']));
        } finally {
            if ($client) $client->close();
            $fixture->close();
        }
    }

    public function testTopologyRefreshWithUncoveredSlot() {
        $fixture = new ClusterTopologyFixture(function ($nodes) {
            $before = [[0, 16383, $nodes[0], $nodes[1]]];
            $partial = [[0, 8191, $nodes[1]]];
            return [
                ['CLUSTER SLOTS' => [$before, $partial],
                 'GET' => [['error' => "MOVED 12182 {$nodes[1][0]}:{$nodes[1][1]}"]]],
                ['CLUSTER SLOTS' => [$partial], 'GET' => ['covered']],
            ];
        });
        $class = $this->getRedisClusterClass();
        $client = null;
        try {
            $client = new $class(null, [implode(':', $fixture->nodes[0])], 1, 1);
            $this->assertEquals('covered', $client->get('{foo}'));
            $this->assertEquals('covered', $client->get('{bar}'));
            $this->assertThrowsMatch($client, function ($c) { $c->get('{qux}'); });
            $this->assertEquals('covered', $client->get('{bar}'));
        } finally {
            if ($client) $client->close();
            $fixture->close();
        }
    }

    public function testAskDoesNotRemapTopology() {
        $fixture = new ClusterTopologyFixture(function ($nodes) {
            return [
                ['CLUSTER SLOTS' => [[[0, 16383, $nodes[0]]]],
                 'KEYS' => [['source-key']], 'GET' => [['error' => "ASK 12182 {$nodes[1][0]}:{$nodes[1][1]}"], 'old-route']],
                ['KEYS' => [['target-key']], 'GET' => ['asking-route']],
            ];
        });
        $class = $this->getRedisClusterClass();
        $client = null;
        try {
            $client = new $class(null, [implode(':', $fixture->nodes[0])], 1, 1);
            $masters = $client->_masters();
            $this->assertEquals('asking-route', $client->get('{foo}'));
            $this->assertEquals($masters, $client->_masters());
            $this->assertEquals(['source-key'], $client->keys('*'));
            $this->assertEquals('old-route', $client->get('{foo}'));
            $calls = json_decode($client->rawCommand($fixture->nodes[0], 'FIXTURE'), true);
            $this->assertEquals(1, $calls[0]['CLUSTER SLOTS']);
            $this->assertEquals(1, $calls[1]['ASKING']);
        } finally {
            if ($client) $client->close();
            $fixture->close();
        }
    }

    public function testMovedDoesNotDiscardWatch() {
        $fixture = new ClusterTopologyFixture(function ($nodes) {
            return [
                ['CLUSTER SLOTS' => [[[0, 16383, $nodes[0], $nodes[1]]]],
                 'GET' => [['error' => "MOVED 12182 {$nodes[1][0]}:{$nodes[1][1]}"]]],
            ];
        });
        $class = $this->getRedisClusterClass();
        $client = null;
        try {
            $client = new $class(null, [implode(':', $fixture->nodes[0])], 1, 1);
            $this->assertTrue($client->watch('{bar}'));
            $this->assertThrowsMatch($client, function ($c) { $c->get('{foo}'); }, '/WATCH/');
            $this->assertTrue($client->unwatch());
            $calls = json_decode($client->rawCommand($fixture->nodes[0], 'FIXTURE'), true);
            $this->assertEquals(1, $calls[0]['UNWATCH']);
            $this->assertEquals(1, $calls[0]['CLUSTER SLOTS']);
        } finally {
            if ($client) $client->close();
            $fixture->close();
        }
    }





    public function testTopologyErrorsDoNotReplayScriptsOrDirectedCommands() {
        $error = 'READONLY returned by the script';
        $fixture = new ClusterTopologyFixture(function ($nodes) use ($error) {
            return [['CLUSTER SLOTS' => [[[0, 16383, $nodes[0]]]],
                     'EVAL' => [['error' => $error]], 'INCR' => [['error' => $error]]]];
        });
        $class = $this->getRedisClusterClass();
        $client = null;
        try {
            $client = new $class(null, [implode(':', $fixture->nodes[0])], 1, 1);
            $this->assertFalse($client->eval("return redis.error_reply('READONLY returned by the script')", ['{foo}'], 1));
            $this->assertEquals($error, $client->getLastError());
            $this->assertFalse($client->rawCommand($fixture->nodes[0], 'INCR', '{foo}'));
            $calls = json_decode($client->rawCommand($fixture->nodes[0], 'FIXTURE'), true);
            $this->assertEquals(1, $calls[0]['EVAL']);
            $this->assertEquals(1, $calls[0]['INCR']);
            $this->assertEquals(1, $calls[0]['CLUSTER SLOTS']);
        } finally {
            if ($client) $client->close();
            $fixture->close();
        }
    }

    public function testTopologyDoesNotReplayLostWriteReply() {
        $fixture = new ClusterTopologyFixture(function ($nodes) {
            return [['CLUSTER SLOTS' => [[[0, 16383, $nodes[0]]]],
                     'INCR' => [['raw' => '', 'close' => true]], 'GET' => ['1']]];
        });
        $class = $this->getRedisClusterClass();
        $client = null;
        try {
            $client = new $class(null, [implode(':', $fixture->nodes[0])], 1, 1);
            $this->assertThrowsMatch($client, function ($c) { $c->incr('{foo}'); });
            $this->assertEquals('1', $client->get('{foo}'));
            $calls = json_decode($client->rawCommand($fixture->nodes[0], 'FIXTURE'), true);
            $this->assertEquals(1, $calls[0]['INCR']);
        } finally {
            if ($client) $client->close();
            $fixture->close();
        }
    }

    public function testTopologyRefreshPreservesConnections() {
        $fixture = new ClusterTopologyFixture(function ($nodes) {
            $before = [[0, 8191, $nodes[1]], [8192, 16383, $nodes[0]]];
            $after = [[0, 16383, $nodes[1]]];
            return [
                ['CLUSTER SLOTS' => [$before, $after],
                 'GET' => [['error' => "MOVED 12182 {$nodes[1][0]}:{$nodes[1][1]}"]]],
                ['CLUSTER SLOTS' => [$after], 'GET' => ['value']],
            ];
        });
        $class = $this->getRedisClusterClass();
        $client = null;
        try {
            $client = new $class(null, [implode(':', $fixture->nodes[0])], 1, 1);
            $id = $client->rawCommand($fixture->nodes[1], 'CLIENT', 'ID');
            $this->assertEquals('value', $client->get('{foo}'));
            $this->assertEquals($id, $client->rawCommand($fixture->nodes[1], 'CLIENT', 'ID'));
        } finally {
            if ($client) $client->close();
            $fixture->close();
        }
    }

    public function testTopologyRefreshPreservesSharedPersistentStream() {
        $pooling = ini_get('redis.pconnect.pooling_enabled');
        ini_set('redis.pconnect.pooling_enabled', '0');
        $fixture = new ClusterTopologyFixture(function ($nodes) {
            $before = [[0, 16383, $nodes[0]]];
            $after = [[0, 16383, $nodes[1]]];
            return [
                ['CLUSTER SLOTS' => [$before, $before, $after],
                 'GET' => ['value', 'value',
                           ['error' => "MOVED 12182 {$nodes[1][0]}:{$nodes[1][1]}"], 'old-value',
                           ['error' => "MOVED 12182 {$nodes[1][0]}:{$nodes[1][1]}"]]],
                ['CLUSTER SLOTS' => [$after], 'GET' => ['new-value']],
            ];
        });
        $class = $this->getRedisClusterClass();
        $first = $second = null;
        try {
            $seed = implode(':', $fixture->nodes[0]);
            $first = new $class(null, [$seed], 1, 1, true);
            $this->assertEquals('value', $first->get('{foo}'));
            $second = new $class(null, [$seed], 1, 1, true);
            $this->assertEquals('value', $second->get('{foo}'));
            $this->assertEquals(
                $first->rawCommand($fixture->nodes[0], 'CLIENT', 'ID'),
                $second->rawCommand($fixture->nodes[0], 'CLIENT', 'ID'));
            $this->assertEquals('new-value', $first->get('{foo}'));
            $this->assertEquals('old-value', $second->get('{bar}'));
            $this->assertEquals('new-value', $second->get('{foo}'));
            $this->assertEquals('new-value', $first->get('{bar}'));
        } finally {
            /* Release shared persistent streams through normal destruction. */
            unset($first, $second);
            $fixture->close();
            ini_set('redis.pconnect.pooling_enabled', $pooling);
        }
    }

    public function testTopologyDiscoveryAcceptsEmptyEndpoints() {
        foreach ([null, ''] as $host) {
            $fixture = new ClusterTopologyFixture(function ($nodes) use ($host) {
                return [['CLUSTER SLOTS' => [[[0, 16383, [$host, $nodes[0][1]]]]], 'GET' => ['value']]];
            });
            $class = $this->getRedisClusterClass();
            $client = null;
            try {
                $client = new $class(null, [implode(':', $fixture->nodes[0])], 1, 1);
                $this->assertEquals('value', $client->get('{foo}'));
            } finally {
                if ($client) $client->close();
                $fixture->close();
            }
        }
    }

    public function testTopologyRecoveryWithAuthenticatedCache() {
        $cache = ini_get('redis.clusters.cache_slots');
        ini_set('redis.clusters.cache_slots', '1');
        try {
            foreach ([['secret'], ['cluster-user', 'secret']] as $auth) {
                $fixture = new ClusterTopologyFixture(function ($nodes) use ($auth) {
                    $before = [[0, 16383, $nodes[0]]];
                    $after = [[0, 16383, $nodes[1]]];
                    return [
                        ['__auth' => $auth, 'CLUSTER SLOTS' => [$before, $after],
                         'GET' => [['error' => "MOVED 12182 {$nodes[1][0]}:{$nodes[1][1]}"]]],
                        ['__auth' => $auth, 'CLUSTER SLOTS' => [$after], 'GET' => ['value']],
                    ];
                });
                $class = $this->getRedisClusterClass();
                $client = null;
                try {
                    $seed = implode(':', $fixture->nodes[0]);
                    $credential = count($auth) === 1 ? $auth[0] : $auth;
                    $first = new $class(null, [$seed], 1, 1, false, $credential);
                    $first->close();
                    unset($first);
                    $client = new $class(null, [$seed], 1, 1, true, $credential);
                    $this->assertEquals('value', $client->get('{foo}'));
                    $this->assertEquals('value', $client->get('{bar}'));
                } finally {
                    if ($client) $client->close();
                    $fixture->close();
                }
            }
        } finally {
            ini_set('redis.clusters.cache_slots', $cache);
        }
    }

    public function testTopologyRefreshIsBoundedPerCommand() {
        $fixture = new ClusterTopologyFixture(function ($nodes) {
            return [
                ['CLUSTER SLOTS' => [[[0, 16383, $nodes[0]]], [[0, 16383, $nodes[1]]]],
                 'GET' => [['error' => "MOVED 12182 {$nodes[1][0]}:{$nodes[1][1]}"]]],
                ['CLUSTER SLOTS' => [[[0, 16383, $nodes[1]]]],
                 'GET' => [['error' => "MOVED 12182 {$nodes[2][0]}:{$nodes[2][1]}"]]],
                ['GET' => ['value']],
            ];
        });
        $class = $this->getRedisClusterClass();
        $client = null;
        try {
            $client = new $class(null, [implode(':', $fixture->nodes[0])], 1, 1);
            $this->assertEquals('value', $client->get('{foo}'));
            $peer = new Redis;
            $peer->connect(...$fixture->nodes[2]);
            $calls = json_decode($peer->rawCommand('FIXTURE'), true);
            $peer->close();
            $this->assertEquals(1, $calls[0]['CLUSTER SLOTS']);
            $this->assertEquals(1, $calls[1]['CLUSTER SLOTS']);
            $this->assertFalse(isset($calls[2]['CLUSTER SLOTS']));
        } finally {
            if ($client) $client->close();
            $fixture->close();
        }
    }

    public function testTopologyMovedDoesNotReplayMulti() {
        $fixture = new ClusterTopologyFixture(function ($nodes) {
            return [['CLUSTER SLOTS' => [[[0, 16383, $nodes[0]]]],
                     'GET' => [['error' => "MOVED 12182 {$nodes[1][0]}:{$nodes[1][1]}"]]]];
        });
        $class = $this->getRedisClusterClass();
        $client = null;
        try {
            $client = new $class(null, [implode(':', $fixture->nodes[0])], 1, 1);
            $client->multi();
            $this->assertThrowsMatch($client, function ($c) { $c->get('{foo}'); }, '/MULTI/');
            $this->assertTrue($client->discard());
            $calls = json_decode($client->rawCommand($fixture->nodes[0], 'FIXTURE'), true);
            $this->assertEquals(1, $calls[0]['GET']);
            $this->assertEquals(1, $calls[0]['CLUSTER SLOTS']);
        } finally {
            if ($client) $client->close();
            $fixture->close();
        }
    }

    protected function getRedisClusterClass(): string { return RedisCluster::class; }

    public function testServerInfo() { $this->markTestSkipped(); }
    public function testServerInfoOldRedis() { $this->markTestSkipped(); }

    /* Tests we'll skip all together in the context of RedisCluster.  The
     * RedisCluster class doesn't implement specialized (non-redis) commands
     * such as sortAsc, or sortDesc and other commands such as SELECT are
     * simply invalid in Redis Cluster */
    public function testPipelinePublish() { $this->markTestSkipped(); }
    public function testSortAsc()  { $this->markTestSkipped(); }
    public function testSortDesc() { $this->markTestSkipped(); }
    public function testWait()     { $this->markTestSkipped(); }
    public function testSelect()   { $this->markTestSkipped(); }
    public function testReconnectSelect() { $this->markTestSkipped(); }
    public function testMultipleConnect() { $this->markTestSkipped(); }
    public function testDoublePipeNoOp() { $this->markTestSkipped(); }
    public function testSwapDB() { $this->markTestSkipped(); }
    public function testConnectException() { $this->markTestSkipped(); }
    public function testTlsConnect() { $this->markTestSkipped(); }
    public function testReset() { $this->markTestSkipped(); }
    public function testInvalidAuthArgs() { $this->markTestSkipped(); }
    public function testScanErrors() { $this->markTestSkipped(); }
    public function testConnectDatabaseSelect() { $this->markTestSkipped(); }

    /* These 'directed node' commands work differently in RedisCluster */
    public function testConfig() { $this->markTestSkipped(); }
    public function testFlushDB() { $this->markTestSkipped(); }
    public function testFunction() { $this->markTestSkipped(); }

    /* Session locking feature is currently not supported in in context of Redis Cluster.
       The biggest issue for this is the distribution nature of Redis cluster */
    public function testSession_lockKeyCorrect() { $this->markTestSkipped(); }
    public function testSession_lockingDisabledByDefault() { $this->markTestSkipped(); }
    public function testSession_lockReleasedOnClose() { $this->markTestSkipped(); }
    public function testSession_ttlMaxExecutionTime() { $this->markTestSkipped(); }
    public function testSession_ttlLockExpire() { $this->markTestSkipped(); }
    public function testSession_lockHoldCheckBeforeWrite_otherProcessHasLock() { $this->markTestSkipped(); }
    public function testSession_lockHoldCheckBeforeWrite_nobodyHasLock() { $this->markTestSkipped(); }
    public function testSession_correctLockRetryCount() { $this->markTestSkipped(); }
    public function testSession_defaultLockRetryCount() { $this->markTestSkipped(); }
    public function testSession_noUnlockOfOtherProcess() { $this->markTestSkipped(); }
    public function testSession_lockWaitTime() { $this->markTestSkipped(); }

    private function loadSeedsFromHostPort($host, $port) {
        try {
            $rc = new RedisCluster(NULL, ["$host:$port"], 1, 1, true, $this->getAuth());
            self::$seed_source = "Host: $host, Port: $port";
            return array_map(function($master) {
                return sprintf('%s:%s', $master[0], $master[1]);
            }, $rc->_masters());
        } catch (Exception $ex) {
            /* fallthrough */
        }

        self::$seed_messages[] = "--host=$host, --port=$port";

        return false;
    }

    private function loadSeedsFromEnv() {
        $seeds = getenv('REDIS_CLUSTER_NODES');
        if ( ! $seeds) {
            self::$seed_messages[] = "environment variable REDIS_CLUSTER_NODES ($seeds)";
            return false;
        }

        self::$seed_source = 'Environment variable REDIS_CLUSTER_NODES';
        return array_filter(explode(' ', $seeds));
    }

    private function loadSeedsFromNodeMap() {
        $nodemap_file = dirname($_SERVER['PHP_SELF']) . '/nodes/nodemap';
        if ( ! file_exists($nodemap_file)) {
            self::$seed_messages[] = "nodemap file '$nodemap_file'";
            return false;
        }

        self::$seed_source = "Nodemap file '$nodemap_file'";
        return array_filter(explode("\n", file_get_contents($nodemap_file)));
    }

    private function loadSeeds($host, $port) {
        if (($seeds = $this->loadSeedsFromNodeMap()))
            return $seeds;
        if (($seeds = $this->loadSeedsFromEnv()))
            return $seeds;
        if (($seeds = $this->loadSeedsFromHostPort($host, $port)))
            return $seeds;

        TestSuite::errorMessage("Error:  Unable to load seeds for RedisCluster tests");
        foreach (self::$seed_messages as $msg) {
            TestSuite::errorMessage("   Tried: %s", $msg);
        }

        exit(1);
    }

    /* Load our seeds on construction */
    public function __construct($host, $port, $auth) {
        parent::__construct($host, $port, $auth);

        self::$seeds = $this->loadSeeds($host, $port);
    }

    /* Override setUp to get info from a specific node */
    public function setUp() {
        $this->redis    = $this->newInstance();
        $info           = $this->redis->info(uniqid());
        $this->version  = $info['redis_version'] ?? '0.0.0';
        $this->is_keydb = $this->detectKeyDB($info);
        $this->is_valkey = $this->detectValkey($info);
        $this->valkey_version = $info['valkey_version'] ?? '0.0.0';
    }

    private function findCliExe() {
        foreach (['redis-cli', 'valkey-cli'] as $candidate) {
            $path = trim(shell_exec("command -v $candidate 2>/dev/null"));
            if (is_executable($path)) {
                return $path;
            }
        }

        return NULL;
    }

    private function getServerReply($host, $port, $cmd) {
        $cli = $this->findCliExe();
        if ( ! $cli) {
            return '(no redis-cli or valkey-cli found)';
        }

        $args = [$cli, '-h', $host, '-p', $port];

        $this->getAuthParts($user, $pass);

        if ($user) $args = array_merge($args, ['--user', $user]);
        if ($pass) $args = array_merge($args, ['-a', $pass]);

        $resp = shell_exec(implode(' ', $args) . ' ' . $cmd . ' 2>/dev/null');

        return is_string($resp) ? trim($resp) : $resp;
    }

    /* Try to gat a new RedisCluster instance. The strange logic is an attempt
       to solve a problem where this sometimes fails but only ever on GitHub
       runners. If we're not on a runner we just get a new instance. Otherwise
       we allow for two tries to get the instance. */
    private function getNewInstance() {
        if (getenv('GITHUB_ACTIONS') === 'true') {
            try {
                return new RedisCluster(NULL, self::$seeds, 30, 30, true,
                                        $this->getAuth());
            } catch (Exception $ex) {
                TestSuite::errorMessage("Failed to connect: %s", $ex->getMessage());
            }
        }

        return new RedisCluster(NULL, self::$seeds, 30, 30, true, $this->getAuth());
    }

    /* Override newInstance as we want a RedisCluster object */
    protected function newInstance() {
        try {
            return $this->getNewInstance();
        } catch (Exception $ex) {
            TestSuite::errorMessage("");
            TestSuite::errorMessage("Fatal error: %s", $ex->getMessage());
            TestSuite::errorMessage("Seeds: %s", implode(' ', self::$seeds));
            TestSuite::errorMessage("Seed source: %s", self::$seed_source);
            TestSuite::errorMessage("");

            TestSuite::errorMessage("Backtrace:");
            foreach (debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS) as $i => $frame) {
                $file = isset($frame['file']) ? basename($frame['file']) : '[internal]';
                $line = $frame['line'] ?? '?';
                $func = $frame['function'] ?? 'unknown';
                TestSuite::errorMessage("  %s:%d [%s]", $file, $line, $func);
            }

            TestSuite::errorMessage("\nServer responses:");

            /* See if we can shed some light on whether Redis is available */
            foreach (self::$seeds as $seed) {
                list($host, $port) = explode(':', $seed);

                $st = microtime(true);
                $reply = $this->getServerReply($host, $port, 'PING');
                $et = microtime(true);

                TestSuite::errorMessage("  [%s:%d] PING -> %s (%.4f)", $host,
                                        $port, var_export($reply, true),
                                        $et - $st);
            }

            exit(1);
        }
    }

    /* Overrides for RedisTest where the function signature is different.  This
     * is only true for a few commands, which by definition have to be directed
     * at a specific node */

    public function testPing() {
        for ($i = 0; $i < 20; $i++) {
            $this->assertTrue($this->redis->ping("key:$i"));
            $this->assertEquals('BEEP', $this->redis->ping("key:$i", 'BEEP'));
        }

        /* Make sure both variations work in MULTI mode */
        $this->redis->multi();
        $this->redis->ping('{ping-test}');
        $this->redis->ping('{ping-test}', 'BEEP');
        $this->assertEquals([true, 'BEEP'], $this->redis->exec());
    }

    public function testRandomKey() {
        /* Ensure some keys are present to test */
        for ($i = 0; $i < 1000; $i++) {
            if (rand(1, 2) == 1) {
                $this->redis->set("key:$i", "val:$i");
            }
        }

        for ($i = 0; $i < 1000; $i++) {
            $k = $this->redis->randomKey("key:$i");
            $this->assertEquals(1, $this->redis->exists($k));
        }
    }

    public function testEcho() {
        $this->assertEquals('hello', $this->redis->echo('echo1', 'hello'));
        $this->assertEquals('world', $this->redis->echo('echo2', 'world'));
        $this->assertEquals(' 0123 ', $this->redis->echo('echo3', " 0123 "));
    }

    public function testSortPrefix() {
        $this->redis->setOption(Redis::OPT_PREFIX, 'some-prefix:');
        $this->redis->del('some-item');
        $this->redis->sadd('some-item', 1);
        $this->redis->sadd('some-item', 2);
        $this->redis->sadd('some-item', 3);

        $this->assertEquals(['1', '2', '3'], $this->redis->sort('some-item'));

        // Kill our set/prefix
        $this->redis->del('some-item');
        $this->redis->setOption(Redis::OPT_PREFIX, '');
    }

    public function testDBSize() {
        for ($i = 0; $i < 10; $i++) {
            $key = "key:$i";
            $this->assertTrue($this->redis->flushdb($key));
            $this->redis->set($key, "val:$i");
            $this->assertEquals(1, $this->redis->dbsize($key));
        }
    }

    public function testInfo() {
        $fields = [
            "redis_version", "arch_bits", "uptime_in_seconds", "uptime_in_days",
            "connected_clients", "connected_slaves", "used_memory",
            "total_connections_received", "total_commands_processed",
            "role"
        ];

        for ($i = 0; $i < 3; $i++) {
            $info = $this->redis->info($i);
            foreach ($fields as $field) {
                $this->assertArrayKey($info, $field);
            }
        }
    }

    public function testClient() {
        $key = 'key-' . rand(1, 100);

        $this->assertTrue($this->redis->client($key, 'setname', 'cluster_tests'));

        $clients = $this->redis->client($key, 'list');
        $this->assertIsArray($clients);

        /* Find us in the list */
        $addr = NULL;
        foreach ($clients as $client) {
            if ($client['name'] == 'cluster_tests') {
                $addr = $client['addr'];
                break;
            }
        }

        /* We should be in there */
        $this->assertIsString($addr);

        /* Kill our own client! */
        $this->assertTrue($this->redis->client($key, 'kill', $addr));
    }

    public function testTime() {
        [$sec, $usec] = $this->redis->time(uniqid());
        $this->assertEquals(strval(intval($sec)), strval($sec));
        $this->assertEquals(strval(intval($usec)), strval($usec));
    }

    public function testScan() {
        $key_count = 0;
        $scan_count = 0;

        /* Have scan retry for us */
        $this->redis->setOption(Redis::OPT_SCAN, Redis::SCAN_RETRY);

        /* Iterate over our masters, scanning each one */
        foreach ($this->redis->_masters() as $master) {
            /* Grab the number of keys we have */
            $key_count += $this->redis->dbsize($master);

            /* Scan the keys here */
            $it = NULL;
            while ($keys = $this->redis->scan($it, $master)) {
                $scan_count += count($keys);
            }
        }

        /* Our total key count should match */
        $this->assertEquals($scan_count, $key_count);
    }

    public function testScanPrefix() {
        $prefixes = ['prefix-a:', 'prefix-b:'];
        $id = uniqid();

        $arr_keys = [];
        foreach ($prefixes as $prefix) {
            $this->redis->setOption(Redis::OPT_PREFIX, $prefix);
            $this->redis->set($id, "LOLWUT");
            $arr_keys[$prefix] = $id;
        }

        $this->redis->setOption(Redis::OPT_SCAN, Redis::SCAN_RETRY);
        $this->redis->setOption(Redis::OPT_SCAN, Redis::SCAN_PREFIX);

        foreach ($prefixes as $prefix) {
            $prefix_keys = [];
            $this->redis->setOption(Redis::OPT_PREFIX, $prefix);

            foreach ($this->redis->_masters() as $master) {
                $it = NULL;
                while ($keys = $this->redis->scan($it, $master, "*$id*")) {
                    foreach ($keys as $key) {
                        $prefix_keys[$prefix] = $key;
                    }
                }
            }

            $this->assertIsArray($prefix_keys, 1);
            $this->assertArrayKey($prefix_keys, $prefix);
        }

        $this->redis->setOption(Redis::OPT_SCAN, Redis::SCAN_NOPREFIX);

        $scan_keys = [];

        foreach ($this->redis->_masters() as $master) {
            $it = NULL;
            while ($keys = $this->redis->scan($it, $master, "*$id*")) {
                foreach ($keys as $key) {
                    $scan_keys[] = $key;
                }
            }
        }

        /* We should now have both prefixs' keys */
        foreach ($arr_keys as $prefix => $id) {
            $this->assertInArray("{$prefix}{$id}", $scan_keys);
        }
    }

    // Run some simple tests against the PUBSUB command.  This is problematic, as we
    // can't be sure what's going on in the instance, but we can do some things.
    public function testPubSub() {
        // PUBSUB CHANNELS ...
        $result = $this->redis->pubsub("somekey", "channels", "*");
        $this->assertIsArray($result);
        $result = $this->redis->pubsub("somekey", "channels");
        $this->assertIsArray($result);

        // PUBSUB NUMSUB

        $c1 = '{pubsub}-' . rand(1, 100);
        $c2 = '{pubsub}-' . rand(1, 100);

        $result = $this->redis->pubsub("{pubsub}", "numsub", $c1, $c2);

        // Should get an array back, with two elements
        $this->assertIsArray($result);
        $this->assertEquals(4, count($result));

        $zipped = [];
        for ($i = 0; $i <= count($result) / 2; $i += 2) {
            $zipped[$result[$i]] = $result[$i+1];
        }
        $result = $zipped;

        // Make sure the elements are correct, and have zero counts
        foreach([$c1,$c2] as $channel) {
            $this->assertArrayKey($result, $channel);
            $this->assertEquals(0, $result[$channel]);
        }

        // PUBSUB NUMPAT
        $result = $this->redis->pubsub("somekey", "numpat");
        $this->assertIsInt($result);

        // Invalid call
        $this->assertFalse($this->redis->pubsub("somekey", "notacommand"));
    }

    /* Unlike Redis proper, MsetNX won't always totally fail if all keys can't
     * be set, but rather will only fail per-node when that is the case */
    public function testMSetNX() {
        /* All of these keys should get set */
        $this->redis->del('x', 'y', 'z');
        $ret = $this->redis->msetnx(['x'=>'a', 'y'=>'b', 'z'=>'c']);
        $this->assertIsArray($ret);
        $this->assertEquals(array_sum($ret),count($ret));

        /* Delete one key */
        $this->redis->del('x');
        $ret = $this->redis->msetnx(['x'=>'a', 'y'=>'b', 'z'=>'c']);
        $this->assertIsArray($ret);
        $this->assertEquals(1, array_sum($ret));

        $this->assertFalse($this->redis->msetnx([])); // set ø → FALSE
    }

    /* Slowlog needs to take a key or [ip, port], to direct it to a node */
    public function testSlowlog() {
        $key = uniqid() . '-' . rand(1, 1000);

        $this->assertIsArray($this->redis->slowlog($key, 'get'));
        $this->assertIsArray($this->redis->slowlog($key, 'get', 10));
        $this->assertIsInt($this->redis->slowlog($key, 'len'));
        $this->assertTrue($this->redis->slowlog($key, 'reset'));
        $this->assertFalse(@$this->redis->slowlog($key, 'notvalid'));
    }

    /* INFO COMMANDSTATS requires a key or ip:port for node direction */
    public function testInfoCommandStats() {
        $info = $this->redis->info(uniqid(), "COMMANDSTATS");

        $this->assertIsArray($info);
        if (is_array($info)) {
            foreach($info as $k => $value) {
                $this->assertStringContains('cmdstat_', $k);
            }
        }
    }

    /* RedisCluster will always respond with an array, even if transactions
     * failed, because the commands could be coming from multiple nodes */
    public function testFailedTransactions() {
        $this->redis->set('x', 42);

        // failed transaction
        $this->redis->watch('x');

        $r = $this->newInstance(); // new instance, modifying `x'.
        $r->incr('x');

        // This transaction should fail because the other client changed 'x'
        $ret = $this->redis->multi()->get('x')->exec();
        $this->assertEquals([false], $ret);
        // watch and unwatch
        $this->redis->watch('x');
        $r->incr('x'); // other instance
        $this->redis->unwatch(); // cancel transaction watch

        // This should succeed as the watch has been cancelled
        $ret = $this->redis->multi()->get('x')->exec();
        $this->assertEquals(['44'], $ret);
    }

    public function testDiscard() {
        $this->redis->multi();
        $this->redis->set('pipecount', 'over9000');
        $this->redis->get('pipecount');

        $this->assertTrue($this->redis->discard());
    }

    /* RedisCluster::script() is a 'raw' command, which requires a key such that
     * we can direct it to a given node */
    public function testScript() {
        $key = uniqid() . '-' . rand(1, 1000);

        // Flush any scripts we have
        $this->assertTrue($this->redis->script($key, 'flush'));

        // Silly scripts to test against
        $s1_src = 'return 1';
        $s1_sha = sha1($s1_src);
        $s2_src = 'return 2';
        $s2_sha = sha1($s2_src);
        $s3_src = 'return 3';
        $s3_sha = sha1($s3_src);

        // None should exist
        $result = $this->redis->script($key, 'exists', $s1_sha, $s2_sha, $s3_sha);
        $this->assertIsArray($result, 3);
        $this->assertTrue(is_array($result) && count(array_filter($result)) == 0);

        // Load them up
        $this->assertEquals($s1_sha, $this->redis->script($key, 'load', $s1_src));
        $this->assertEquals($s2_sha, $this->redis->script($key, 'load', $s2_src));
        $this->assertEquals($s3_sha, $this->redis->script($key, 'load', $s3_src));

        // They should all exist
        $result = $this->redis->script($key, 'exists', $s1_sha, $s2_sha, $s3_sha);
        $this->assertTrue(is_array($result) && count(array_filter($result)) == 3);
    }

    /* RedisCluster::EVALSHA needs a 'key' to let us know which node we want to
     * direct the command at */
    public function testEvalSHA() {
        $key = uniqid() . '-' . rand(1, 1000);

        // Flush any loaded scripts
        $this->redis->script($key, 'flush');

        // Non existent script (but proper sha1), and a random (not) sha1 string
        $this->assertFalse($this->redis->evalsha(sha1(uniqid()),[$key], 1));
        $this->assertFalse($this->redis->evalsha('some-random-data'),[$key], 1);

        // Load a script
        $cb  = uniqid(); // To ensure the script is new
        $scr = "local cb='$cb' return 1";
        $sha = sha1($scr);

        // Run it when it doesn't exist, run it with eval, and then run it with sha1
        $this->assertFalse($this->redis->evalsha($scr,[$key], 1));
        $this->assertEquals(1, $this->redis->eval($scr,[$key], 1));
        $this->assertEquals(1, $this->redis->evalsha($sha,[$key], 1));
    }

    public function testEvalBulkResponse() {
        $key1 = uniqid() . '-' . rand(1, 1000) . '{hash}';
        $key2 = uniqid() . '-' . rand(1, 1000) . '{hash}';

        $this->redis->script($key1, 'flush');
        $this->redis->script($key2, 'flush');

        $scr = "return {KEYS[1],KEYS[2]}";

        $result = $this->redis->eval($scr,[$key1, $key2], 2);

        $this->assertEquals($key1, $result[0]);
        $this->assertEquals($key2, $result[1]);
    }

    public function testEvalBulkResponseMulti() {
        $key1 = uniqid() . '-' . rand(1, 1000) . '{hash}';
        $key2 = uniqid() . '-' . rand(1, 1000) . '{hash}';

        $this->redis->script($key1, 'flush');
        $this->redis->script($key2, 'flush');

        $scr = "return {KEYS[1],KEYS[2]}";

        $this->redis->multi();
        $this->redis->eval($scr, [$key1, $key2], 2);

        $result = $this->redis->exec();

        $this->assertEquals($key1, $result[0][0]);
        $this->assertEquals($key2, $result[0][1]);
    }

    public function testEvalBulkEmptyResponse() {
        $key1 = uniqid() . '-' . rand(1, 1000) . '{hash}';
        $key2 = uniqid() . '-' . rand(1, 1000) . '{hash}';

        $this->redis->script($key1, 'flush');
        $this->redis->script($key2, 'flush');

        $scr = "for _,key in ipairs(KEYS) do redis.call('SET', key, 'value') end";

        $result = $this->redis->eval($scr, [$key1, $key2], 2);

        $this->assertNull($result);
    }

    public function testEvalBulkEmptyResponseMulti() {
        $key1 = uniqid() . '-' . rand(1, 1000) . '{hash}';
        $key2 = uniqid() . '-' . rand(1, 1000) . '{hash}';

        $this->redis->script($key1, 'flush');
        $this->redis->script($key2, 'flush');

        $scr = "for _,key in ipairs(KEYS) do redis.call('SET', key, 'value') end";

        $this->redis->multi();
        $this->redis->eval($scr, [$key1, $key2], 2);
        $result = $this->redis->exec();

        $this->assertNull($result[0]);
    }

    /* Cluster specific introspection stuff */
    public function testIntrospection() {
        $primaries = $this->redis->_masters();
        $this->assertIsArray($primaries);

        foreach ($primaries as [$host, $port]) {
            $this->assertIsString($host);
            $this->assertIsInt($port);
        }
    }

    protected function keyTypeToString($key_type) {
        switch ($key_type) {
            case Redis::REDIS_STRING:
                return "string";
            case Redis::REDIS_SET:
                return "set";
            case Redis::REDIS_LIST:
                return "list";
            case Redis::REDIS_ZSET:
                return "zset";
            case Redis::REDIS_HASH:
                return "hash";
            case Redis::REDIS_STREAM:
                return "stream";
            case Redis::REDIS_VECTORSET:
                return "vectorset";
            default:
                return "unknown($key_type)";
        }

    }

    protected function genKeyName($key_index, $key_type) {
        return sprintf('%s-%s', $this->keyTypeToString($key_type), $key_index);
    }

    protected function setKeyVals($key_index, $key_type, &$arr_ref) {
        $key = $this->genKeyName($key_index, $key_type);

        $this->redis->del($key);

        switch ($key_type) {
            case Redis::REDIS_STRING:
                $value = "$key-value";
                $this->redis->set($key, $value);
                break;
            case Redis::REDIS_SET:
                $value = [
                    "$key-mem1", "$key-mem2", "$key-mem3",
                    "$key-mem4", "$key-mem5", "$key-mem6"
                ];
                $args = $value;
                array_unshift($args, $key);
                call_user_func_array([$this->redis, 'sadd'], $args);
                break;
            case Redis::REDIS_HASH:
                $value = [
                    "$key-mem1" => "$key-val1",
                    "$key-mem2" => "$key-val2",
                    "$key-mem3" => "$key-val3"
                ];
                $this->redis->hmset($key, $value);
                break;
            case Redis::REDIS_LIST:
                $value = [
                    "$key-ele1", "$key-ele2", "$key-ele3",
                    "$key-ele4", "$key-ele5", "$key-ele6"
                ];
                $args = $value;
                array_unshift($args, $key);
                call_user_func_array([$this->redis, 'rpush'], $args);
                break;
            case Redis::REDIS_ZSET:
                $score = 1;
                $value = [
                    "$key-mem1" => 1, "$key-mem2" => 2,
                    "$key-mem3" => 3, "$key-mem3" => 3
                ];
                foreach ($value as $mem => $score) {
                    $this->redis->zadd($key, $score, $mem);
                }
                break;
        }

        /* Update our reference array so we can verify values */
        $arr_ref[$key] = $value;

        return $key;
    }

    /* Verify that our ZSET values are identical */
    protected function checkZSetEquality($a, $b) {
        /* If the count is off, the array keys are different or the sums are
         * different, we know there is something off */
        $boo_diff = count($a) != count($b) ||
            count(array_diff(array_keys($a), array_keys($b))) != 0 ||
            array_sum($a) != array_sum($b);

        if ($boo_diff) {
            $this->assertEquals($a, $b);
            return;
        }
    }

    protected function checkKeyValue($key, $key_type, $value) {
        switch ($key_type) {
            case Redis::REDIS_STRING:
                $this->assertEquals($value, $this->redis->get($key));
                break;
            case Redis::REDIS_SET:
                $arr_r_values = $this->redis->sMembers($key);
                $arr_l_values = $value;
                sort($arr_r_values);
                sort($arr_l_values);
                $this->assertEquals($arr_r_values, $arr_l_values);
                break;
            case Redis::REDIS_LIST:
                $this->assertEquals($value, $this->redis->lrange($key, 0, -1));
                break;
            case Redis::REDIS_HASH:
                $this->assertEquals($value, $this->redis->hgetall($key));
                break;
            case Redis::REDIS_ZSET:
                $this->checkZSetEquality($value, $this->redis->zrange($key, 0, -1, true));
                break;
            default:
                throw new Exception("Unknown type " . $key_type);
        }
    }

    /* Test automatic load distributor */
    public function testFailOver() {
        $value_ref = [];
        $type_ref  = [];

        /* Set a bunch of keys of various redis types*/
        for ($i = 0; $i < 200; $i++) {
            foreach ($this->redis_types as $type) {
                $key = $this->setKeyVals($i, $type, $value_ref);
                $type_ref[$key] = $type;
            }
        }

        /* Iterate over failover options */
        foreach ($this->failover_types as $failover_type) {
            $this->redis->setOption(RedisCluster::OPT_SLAVE_FAILOVER, $failover_type);

            foreach ($value_ref as $key => $value) {
                $this->checkKeyValue($key, $type_ref[$key], $value);
            }

            break;
        }
    }

    /* Test a 'raw' command */
    public function testRawCommand() {
        $this->redis->rawCommand('mykey', 'set', 'mykey', 'my-value');
        $this->assertEquals('my-value', $this->redis->get('mykey'));

        $this->redis->del('mylist');
        $this->redis->rpush('mylist', 'A', 'B', 'C', 'D');
        $this->assertEquals(['A', 'B', 'C', 'D'], $this->redis->lrange('mylist', 0, -1));
    }

    protected function rawCommandArray($key, $args) {
        array_unshift($args, $key);
        return call_user_func_array([$this->redis, 'rawCommand'], $args);
    }

    /* Test that rawCommand and EVAL can be configured to return simple string values */
    public function testReplyLiteral() {
        $this->redis->setOption(Redis::OPT_REPLY_LITERAL, false);
        $this->assertTrue($this->redis->rawCommand('foo', 'set', 'foo', 'bar'));
        $this->assertTrue($this->redis->eval("return redis.call('set', KEYS[1], 'bar')", ['foo'], 1));

        $rv = $this->redis->eval("return {redis.call('set', KEYS[1], 'bar'), redis.call('ping')}", ['foo'], 1);
        $this->assertEquals([true, true], $rv);

        $this->redis->setOption(Redis::OPT_REPLY_LITERAL, true);
        $this->assertEquals('OK', $this->redis->rawCommand('foo', 'set', 'foo', 'bar'));
        $this->assertEquals('OK', $this->redis->eval("return redis.call('set', KEYS[1], 'bar')", ['foo'], 1));

        $rv = $this->redis->eval("return {redis.call('set', KEYS[1], 'bar'), redis.call('ping')}", ['foo'], 1);
        $this->assertEquals(['OK', 'PONG'], $rv);

        // Reset
        $this->redis->setOption(Redis::OPT_REPLY_LITERAL, false);
    }

    /* Redis and RedisCluster use the same handler for the ACL command but verify we can direct
       the command to a specific node. */
    public function testAcl() {
        if ( ! $this->minVersionCheck("6.0"))
            $this->markTestSkipped();

        $this->assertInArray('default', $this->redis->acl('foo', 'USERS'));
    }

    public function testSession()
    {
        @ini_set('session.save_handler', 'rediscluster');
        @ini_set('session.save_path', $this->sessionSavePath() . '&failover=error');

        if ( ! @session_start())
            $this->markTestSkipped();

        session_write_close();

        $this->assertKeyExists($this->sessionPrefix() . session_id());
    }


    /* Test that we are able to use the slot cache without issues */
    public function testSlotCache() {
        ini_set('redis.clusters.cache_slots', 1);

        $pong = 0;
        for ($i = 0; $i < 10; $i++) {
            $new_client = $this->newInstance();
            $pong += $new_client->ping("key:$i");
        }

        $this->assertEquals($pong, $i);

        ini_set('redis.clusters.cache_slots', 0);
    }

    /* Regression test for connection pool liveness checks */
    public function testConnectionPool() {
        $prev_value = ini_get('redis.pconnect.pooling_enabled');
        ini_set('redis.pconnect.pooling_enabled', 1);

        $pong = 0;
        for ($i = 0; $i < 10; $i++) {
            $new_client = $this->newInstance();
            $pong += $new_client->ping("key:$i");
        }

        $this->assertEquals($pong, $i);
        ini_set('redis.pconnect.pooling_enabled', $prev_value);
    }

    protected function sessionPrefix(): string {
        return 'PHPREDIS_CLUSTER_SESSION:';
    }

    protected function sessionSaveHandler(): string {
        return 'rediscluster';
    }

    /**
     * @inheritdoc
     */
    protected function sessionSavePath(): string {
        return implode('&', array_map(function ($host) {
            return 'seed[]=' . $host;
        }, self::$seeds)) . '&' . $this->getAuthFragment();
    }

    /* Test correct handling of null multibulk replies */
    public function testNullArray() {
        $key = "key:arr";
        $this->redis->del($key);

        foreach ([false => [], true => NULL] as $opt => $test) {
            $this->redis->setOption(Redis::OPT_NULL_MULTIBULK_AS_NULL, $opt);

            $r = $this->redis->rawCommand($key, "BLPOP", $key, .05);
            $this->assertEquals($test, $r);

            $this->redis->multi();
            $this->redis->rawCommand($key, "BLPOP", $key, .05);
            $r = $this->redis->exec();
            $this->assertEquals([$test], $r);
        }

        $this->redis->setOption(Redis::OPT_NULL_MULTIBULK_AS_NULL, false);
    }

    protected function execWaitAOF() {
        return $this->redis->waitaof(uniqid(), 0, 0, 0);
    }
}
?>

<?php
/* Usage: php -d extension=swoole -d extension=redis.so clusterTopologyPool.php /path/to/emscore/src */
require __DIR__ . '/ClusterTopologyFixture.php';
$root = $argv[1] ?? throw new RuntimeException('Pass the emscore src directory');
spl_autoload_register(function($class) use($root) {
    if (str_starts_with($class, 'EmsCore\\')) require $root . '/' . str_replace('\\', '/', substr($class, 8)) . '.php';
});
use EmsCore\Base\Pool\Connections\RedisClusterConnectionFactory;
use EmsCore\SwoolePool\Pool\ConnectionPool;
use EmsCore\SwoolePool\Pool\PoolConfig;
$fixture = new ClusterTopologyFixture(function($n) {
    return [['CLUSTER SLOTS'=>[[[0,16383,$n[0]]]],'GET'=>[['raw'=>"$10\r\nabc",'close'=>true],'clean']]];
});
Swoole\Runtime::enableCoroutine(SWOOLE_HOOK_ALL);
try {
    Swoole\Coroutine\run(function() use($fixture) {
        $pool = new ConnectionPool(new RedisClusterConnectionFactory(
            seeds:[implode(':',$fixture->nodes[0])], timeout:.2, readTimeout:.2, persistent:false),
            new PoolConfig(size:2,minIdle:0,heartbeatIntervalSec:60));
        try {
            $healthy = $pool->borrow();
            $healthyClient = $healthy->getCluster();
            try {
                $pool->run(fn($conn)=>$conn->getCluster()->get('{foo}'));
                throw new RuntimeException('Expected read failure');
            } catch (RedisException|RedisClusterException $expected) {}
            if ($healthyClient->get('{foo}') !== 'clean') throw new RuntimeException('Healthy pool member damaged');
            if ($pool->run(fn($conn)=>$conn->getCluster()->get('{foo}')) !== 'clean') throw new RuntimeException('Replacement member failed');
            $pool->return($healthy);
            echo "PASS Swoole pool isolates failed member and borrows replacement\n";
        } finally { $pool->close(); }
    });
} finally { Swoole\Runtime::enableCoroutine(0); $fixture->close(); }

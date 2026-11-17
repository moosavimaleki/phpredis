<?php
require __DIR__ . '/ClusterTopologyFixture.php';
function check($ok, $message) { if (!$ok) throw new RuntimeException($message); }
function fails(callable $fn) { try { $fn(); } catch (RedisException|RedisClusterException $e) { return; } throw new RuntimeException('Expected exception'); }
function scenario($name, callable $configure, callable $test) {
    $fixture = new ClusterTopologyFixture($configure);
    try {
        $c = new RedisCluster(null, [implode(':', $fixture->nodes[0])], .2, .2);
        $test($c, $fixture->nodes);
        $c->close();
        unset($c);
        echo "PASS $name\n";
    } finally { $fixture->close(); }
}
scenario('MOVED refreshes all slots and primary registry', function($n) {
    return [['CLUSTER SLOTS'=>[[[0,16383,$n[0]]]], 'GET'=>[['error'=>"MOVED 12182 {$n[1][0]}:{$n[1][1]}"]]],
        ['CLUSTER SLOTS'=>[[[0,16383,$n[1]]]], 'GET'=>['fresh']]];
}, function($c,$n) {
    check($c->get('{foo}')==='fresh','redirect');
    check($c->get('{bar}')==='fresh','second slot');
    check($c->_masters()===[$n[1]],'registry');
});
foreach (['overlap','range','host','port','stale','hole'] as $bad) {
    scenario("invalid/stale refresh: $bad", function($n) use($bad) {
        $old=[[0,16383,$n[0]]];
        $maps=['overlap'=>[[0,8191,$n[1]],[8191,16383,$n[1]]], 'range'=>[[0,16384,$n[1]]],
            'host'=>[[0,16383,["bad\0host",1234]]], 'port'=>[[0,16383,['127.0.0.1',65536]]],
            'stale'=>$old,'hole'=>[[0,8191,$n[1]]]];
        return [['CLUSTER SLOTS'=>[$old,$maps[$bad]],'GET'=>[['error'=>"MOVED 12182 {$n[1][0]}:{$n[1][1]}"],'old']],
            ['CLUSTER SLOTS'=>[$maps[$bad]],'GET'=>['new']]];
    },function($c,$n) use($bad) {
        fails(fn()=>$c->get('{foo}'));
        if ($bad==='hole') fails(fn()=>$c->get('{foo}'));
        else check($c->get('{bar}')==='old','old routing preserved');
    });
}
foreach (['MOVED','ASK'] as $redirect) {
    scenario("WATCH rejects $redirect",function($n) use($redirect) {
        return [['CLUSTER SLOTS'=>[[[0,16383,$n[0]]]],'WATCH'=>[['error'=>"$redirect 12182 {$n[1][0]}:{$n[1][1]}"]]]];
    },function($c,$n) { fails(fn()=>$c->watch('{foo}')); check($c->_masters()===[$n[0]],'WATCH remapped'); });
}
scenario('active WATCH blocks remap and remains unwatchable',function($n) {
    return [['CLUSTER SLOTS'=>[[[0,8191,$n[0]],[8192,16383,$n[1]]]]],
        ['GET'=>[['error'=>"MOVED 12182 {$n[2][0]}:{$n[2][1]}"]]]];
},function($c,$n) { check($c->watch('{bar}'),'watch'); fails(fn()=>$c->get('{foo}')); check($c->unwatch(),'unwatch'); });
scenario('ASK stays temporary under replica distribution',function($n) {
    return [['CLUSTER SLOTS'=>[[[0,16383,$n[0],$n[2]]]],'GET'=>[['error'=>"ASK 12182 {$n[1][0]}:{$n[1][1]}"]]],
        ['GET'=>['temporary']],['GET'=>[['error'=>"ASK 12182 {$n[1][0]}:{$n[1][1]}"]]]];
},function($c,$n) {
    $c->setOption(RedisCluster::OPT_SLAVE_FAILOVER,RedisCluster::FAILOVER_DISTRIBUTE_SLAVES);
    check($c->get('{foo}')==='temporary','ASK payload'); check($c->_masters()===[$n[0]],'ASK registry');
    $calls=json_decode($c->rawCommand($n[0],'FIXTURE'),true);
    check(($calls[1]['ASKING']??0)===1,'ASKING destination');
});
scenario('lost write response never replays payload',function($n) {
    return [['CLUSTER SLOTS'=>[[[0,8191,$n[1]],[8192,16383,$n[0]]]],'INCR'=>[['raw'=>'','close'=>true]]]];
},function($c,$n) {
    fails(fn()=>$c->incr('{foo}'));
    $calls=json_decode($c->rawCommand($n[1],'FIXTURE'),true);
    check(($calls[0]['INCR']??0)===1 && ($calls[1]['INCR']??0)===0,'write replay');
});
scenario('truncated bulk isolates dirty stream',function($n) {
    return [['CLUSTER SLOTS'=>[[[0,16383,$n[0]]]],'GET'=>[['raw'=>"$10\r\nabc",'close'=>true],'clean']]];
},function($c,$n) { fails(fn()=>$c->get('{foo}')); check($c->get('{foo}')==='clean','dirty stream reused'); });
scenario('two pre-send connection failures refresh from known node',function($n) {
    return [['CLUSTER SLOTS'=>[[[0,8191,$n[1]],[8192,16383,$n[0]]]],'STOP'=>[['stop'=>true]]],
        ['CLUSTER SLOTS'=>[[[0,16383,$n[1]]]],'GET'=>['recovered']]];
},function($c,$n) {
    $c->rawCommand($n[0],'STOP'); $c->close();
    check($c->get('{foo}')==='recovered','connection failure refresh'); check($c->_masters()===[$n[1]],'new registry');
});

scenario('MULTI refuses MOVED without refreshing',function($n) {
    return [['CLUSTER SLOTS'=>[[[0,16383,$n[0]]]],'GET'=>[['error'=>"MOVED 12182 {$n[1][0]}:{$n[1][1]}"]]]];
},function($c,$n) { $c->multi(); fails(fn()=>$c->get('{foo}')); $c->discard(); check($c->_masters()===[$n[0]],'MULTI remapped'); });
scenario('WATCH is primary-only after a replica GET',function($n) {
    return [['CLUSTER SLOTS'=>[[[0,16383,$n[0],$n[1]]]]],['GET'=>['replica']]];
},function($c,$n) {
    $c->setOption(RedisCluster::OPT_SLAVE_FAILOVER,RedisCluster::FAILOVER_DISTRIBUTE_SLAVES);
    check($c->get('{foo}')==='replica','replica read'); check($c->watch('{foo}'),'watch'); check($c->unwatch(),'unwatch');
    $calls=json_decode($c->rawCommand($n[0],'FIXTURE'),true);
    check(($calls[0]['WATCH']??0)===1 && ($calls[1]['WATCH']??0)===0,'WATCH sent to replica');
});
scenario('explicit SCAN never switches endpoint',function($n) {
    return [['CLUSTER SLOTS'=>[[[0,8191,$n[0]],[8192,16383,$n[1]]]],'SCAN'=>[['raw'=>'','close'=>true]]]];
},function($c,$n) {
    $cursor=null; fails(function() use($c,$n,&$cursor) { $c->scan($cursor,$n[0]); });
    $calls=json_decode($c->rawCommand($n[1],'FIXTURE'),true);
    check(($calls[1]['SCAN']??0)===0,'SCAN switched node');
});
scenario('empty MOVED host uses responding host',function($n) {
    return [['CLUSTER SLOTS'=>[[[0,16383,$n[0]]]],'GET'=>[['error'=>"MOVED 12182 :{$n[1][1]}"]]],
        ['CLUSTER SLOTS'=>[[[0,16383,['',$n[1][1]]]]],'GET'=>['fresh']]];
},function($c,$n) { check($c->get('{foo}')==='fresh','empty endpoint'); });

ini_set('redis.clusters.cache_slots','1');
$fixture = new ClusterTopologyFixture(function($n) {
    $old=[[0,8191,$n[1]],[8192,16383,$n[0]]];
    $new=[[0,16383,$n[1]]];
    return [['__auth'=>['secret'],'CLUSTER SLOTS'=>[$old],'GET'=>[['error'=>"MOVED 12182 {$n[1][0]}:{$n[1][1]}"]]],
        ['__auth'=>['secret'],'CLUSTER SLOTS'=>[$new],'GET'=>['authenticated']]];
});
try {
    $args=[null,[implode(':',$fixture->nodes[0])],.2,.2,false,'secret'];
    $first=new RedisCluster(...$args); $first->close(); unset($first);
    $cached=new RedisCluster(...$args);
    check($cached->get('{foo}')==='authenticated','cached authenticated refresh');
    check($cached->_masters()===[$fixture->nodes[1]],'cached registry');
    $cached->close(); unset($cached);
    echo "PASS authenticated cache-hit refresh\n";
} finally { $fixture->close(); ini_set('redis.clusters.cache_slots','0'); }

## 说明

此分支由[swoole/php-cp-1.5.0](https://github.com/swoole/php-cp/tree/1.5.0)派生。

swoole/php-cp-1.5.0针对php7编译时使用了[swoole_serialize](https://github.com/swoole/swoole_serialize)做变量序列化，但是运行`pdo_connect_pool_PDOStatement::setFetchMode(PDO::FETCH_OBJ)`时worker会崩溃退出（应该是对象序列化出问题）。

本分支主要工作是将swoole_serialize替换成[msgpack-php](https://github.com/msgpack/msgpack-php)。后续swoole/php-cp若是修复了序列化问题，请忽略本分支。

本分支只在php7环境下编译测试，php5环境请使用[swoole/php-cp](https://github.com/swoole/php-cp)。

## php-cp(php-connect-pool),redis和pdo的本地代理 [![Build Status](https://secure.travis-ci.org/swoole/php-cp.svg?branch=master)](http://travis-ci.org/swoole/php-cp)

[中文简介] http://blog.sina.com.cn/s/blog_9eaa0f400102v9fd.html

提供连接池，读写分离，负载均衡，慢查询日志，大数据块日志等功能

## 要求

- PHP 5.3 + (no zts)
- linux 2.6+
- pdo and redis extension install


## 使用Docker安装

可以使用Docker编译，需要在项目的根目录下运行:

1. 根据自己的配置，复制 `config.ini.example` 文件为 `pool.ini` 文件，修改 `pool.ini` 文件
2. `docker build -t php-cp .`


## 技术特性:

- 提供了release方法，在每次fetch数据后(redis的get set) 调用，将连接放回到池子里面，避免其他耗时操作导致的db层连接数过高问题。
- 提供最大最小连接数配置支持。
- 连接自动ping 数据库， 防止压力小长时间不请求导致的gone away问题
- 根据压力自动获取（最大到最大连接数）或者释放（释放最小到最小连接数）池子里面的连接。
- 做了大量优化虽然请求经过代理进程转发但基本没有性能损耗.
- 当池子里面的连接被占用没了，接下来的挣钱连接的进程将会排队，直到持有连接的进程release连接.
- 使用透明化，相对于传统的pdo和redis操作，只需要修改new的类名，以及适当时机release连接即可（可以集成到db层框架）
- 支持pdo的读写分离和从库的负载均衡。
- 支持cli模式下的pdo和redis异步查询。
- 支持慢查询日志(max_hold_time_to_log)以及大的数据块(max_data_size_to_log)日志功能。


## 提示
- 请求结束（rshutdown/mshutdown阶段）会调用自动调用release，不要依赖于这个release，否则连接利用率会很低
- 关于异常：
pdoProxy和原生Pdo不一样的一点是 默认pdo是静默模式 不抛异常  pdoProxy是抛异常的（且是用Exception类抛出的 不是PDOException）
- pool_server 必须以root用户启动
- redis不支持pub/sub方法
- 当你用完一个连接后（例如：fetchAll调用结束），请调用release来马上释放连接到池子里面(如果事务需要在事务commit或者rollback后release)，如果不想改业务代码可以在框架层每次fetch（或者get/set）用完之后调用release方法。

## 集成好的框架
- yii请参考项目中的frame_example
- redis请参考项目中的frame_example
- ci 请参考此项目 https://github.com/ethenoscar2011/codeigniter-phpcp
- thinkphp 请参考 http://git.oschina.net/xavier007/THINKPHP_phpcp_driver
- discuz 请参考 https://github.com/xluohome/php-cp-for-discuz

## 安装使用
* 安装扩展 安装步骤跟其它PHP扩展无差别

```
$ phpize && ./configure && make && make install //如果报phpize命令找不到请安装php-devel包
$ echo "extension=connect_pool.so" >> php.ini
//如果PHP启用了目录配置 上一步骤可以换为下面的方式 配置目录可以通过 php --info|grep 'Scan'获取
$ echo "extension=connect_pool.so" > /etc/php.d/20-connection_pool.ini
```

* 初始化配置(一次性)
```
$ cp ./config.ini.example /etc/pool.ini //根据需求修改配置内容
$ mkdir -m 755 /var/log/php-connection-pool //创建日志目录 目录文件夹不存在或没权限会导致日志写不起
$ chmod +x ./pool_server //x权限git已经设置 为稳妥再设置一次 pool_server为php脚本 可自行修改
$ [ -f /bin/env ] || sudo ln -s /usr/bin/env /bin/env  //deb系的系统(如debian、ubuntu)env的路径为/usr/bin/env做软链接兼容处理
$ cp ./pool_server /usr/local/bin/pool_server
```

* 日常运维使用
```
$ pool_server start //启动服务 如果配置文件的daemonize开启则后台运行 否则为前台运行 Ctrl+c结束服务
$ pool_server stop //停止服务
$ pool_server restart //重启服务
$ pool_server status //查看服务状态
```

* 日常开发使用  
  将该项目源码加入IDE的外部库中， 即可有代码提示, 切记不要加入php配置的include path中

``` php
<?php
/* * ****************don't use pool(不用连接池 最原始的方式)************************ */
$obj = new Redis();
$rs = $obj->connect("192.168.20.130");
$obj->select(5);
$obj->set("test", '1111');
var_dump($obj->get("test"));

$obj = new PDO('mysql:host=192.168.20.130;dbname=test1', "admin", "admin");
$rs = $obj->query("show tables");
var_dump($rs->fetchAll());

//*****************use pool（使用了连接池）*********************************/
$obj = new redisProxy();
$rs = $obj->connect("192.168.20.130");
$obj->select(5);
$obj->set("test", '1111');
var_dump($obj->get("test"));
$obj->release();

$obj1 = new pdoProxy('mysql:host=192.168.20.131;dbname=db1', "admin", "admin");
$rs = $obj1->query("show tables");
var_dump($rs->fetchAll());
$obj1->release();


/* * ****************异步 pdo和redis操作**********************************************
 * 依赖 swoole的event函数
 */
include './asyncClass.php';
$obj = new asyncRedisProxy();
$obj->connect("127.0.0.1", "6379");
$obj->set("a", 11111, function($obj, $ret) {
    $obj->get("a", function($obj, $data) {
        var_dump($data);
        $obj->release(); //release to con pool
    });
});


$obj2 = new asyncPdoProxy('mysql:host=192.168.1.19;dbname=mz_db', "public_user", "1qa2ws3ed");
$obj2->query("select 1 from mz_user where user_id=299", function($obj, $stmt) {
    $arr = $stmt->fetchAll();
    var_dump($arr);
    $obj->query("select 2 from mz_user where user_id=299", function($obj, $stmt) {
        $arr = $stmt->fetchAll();
        var_dump($arr);
        $obj->release(); //release to con pool
    });
});


$obj3 = new asyncPdoProxy('mysql:host=192.168.1.19;dbname=mz_db', "public_user", "1qa2ws3ed");
$obj3->exec("insert into t1(name) values('111111')", function($obj, $data) {
    var_dump($data);
    $obj->release(); ////release to con pool
});


$obj4 = new asyncPdoProxy('mysql:host=192.168.1.19;dbname=mz_db', "public_user", "1qa2ws3ed");
$stmt = $obj4->prepare("select * from mz_account where user_id=:user_id");
$stmt->bindParam(':user_id', "311");
$stmt->execute(function($stmt, $ret) {
    $data = $stmt->fetchAll();
    var_dump($data);
    $stmt->release();
});

//*******************use master slave(最新版本支持了读写分离和从库的负载均衡 用法如下)***********************/
$config = array(
    'master' => array(
        'data_source' => "mysql:host=192.168.1.19;dbname=db1",
        'username' => "public_user",
        'pwd' => "1qa2ws3ed",
        'options' => array(
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_TIMEOUT => 3,
            PDO::ATTR_CASE => PDO::CASE_UPPER,
        ),
    ),
    'slave' => array(
        "0" => array(
            'data_source' => "mysql:host=192.168.1.20;dbname=db2",
            'username' => "public_user",
            'pwd' => "1qa2ws3ed",
            'options' => array(
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_TIMEOUT => 3,
                PDO::ATTR_CASE => PDO::CASE_UPPER,
            ),
        ),
        "1" => array(
            'data_source' => "mysql:host=192.168.1.21;dbname=db3",
            'username' => "public_user",
            'pwd' => "1qa2ws3ed",
            'options' => array(
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_TIMEOUT => 3,
                PDO::ATTR_CASE => PDO::CASE_LOWER,
            ),
        ),
    ),
);
/* * *************************"select"和"show"开头的语句 走随机从库********** */
$obj1 = new pdoProxy($config);
$rs = $obj1->query("select * from test limit 1");
var_dump($rs->fetchAll()); //走随机从库
$obj1->release();

/* * **************************读强行走主库*************************** */
$obj1->enable_slave = false;
$rs = $obj1->query("select * from test limit 1");
var_dump($rs->fetchAll()); //读主库
$obj1->release();

/* * *************************除了"select"和"show"开头的语句 都走主库********** */
$sql = "insert into `test` (tid) values (5)";
$rs = $obj1->exec($sql); //走主库
$obj1->release();
```

## 测试

运行命令

`php tests/RunTest.php --host 172.17.0.2 --class RedisTest --test test_set_get`

 执行测试，该命令接受三个参数：

+ --host 设置主机地址，可选，默认值为"127.0.0.1"
+ --class 设置要运行的测试类名称，可选，默认值为"RedisTest"
+ --test 设置要运行的测试函数名称，可选，如果不设置则执行所有测试

## contact us
- http://weibo.com/u/2661945152
- 83212019@qq.com
- qq群号 538716391

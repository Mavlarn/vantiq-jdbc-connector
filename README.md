# vantiq-jdbc-connector
jdbc-connector for VANTIQ


## Usage
1. register JDBC Source using vantiq client tool 
2. create a source in VANTIQ, like test_jdbc_source
3. start MySQL server, create related database and table.
4. start the connector

## register
Create a config file named *amqpSource.json*:
```
{
   "name" : "JDBCSource",
   "baseType" : "EXTENSION",
   "verticle" : "service:extensionSource",
   "config" : {}
}
```

And run:
```
vantiq -s <profileName> load sourceimpls amqpSource.json
```

## Create source
In VANTIQ, you should see a new Source type named *JDBCSource*, create a new source with this type, and config:
```json
{
   "jdbcConfig": {
      "username": "root",
      "password": "123456",
      "dbURL": "jdbc:mysql://localhost/test1?useSSL=false"
   }
}
```
上面是最简单的配置方式，这样配置以后，就可以通过这个source在vantiq中对数据库进行读写。

除此以外，还有2种配置方式：
1. Load table到VANTIQ
```json
{
   "jdbcConfig": {
      "username": "root",
      "password": "123456",
      "dbURL": "jdbc:mysql://localhost/test1?useSSL=false",
      "loadTable": "employee",
      "loadInterval": 100,
      "loadSize": 10
   }
}
```
通过这个配置，可以将数据库中employee表的数据load到vantiq，每次取10条，每次取数据的间隔是100毫秒。表中的数据会通过数据流的方式发送到VANTIQ。

2. 定时查询某个表
```json
{
   "jdbcConfig": {
      "username": "root",
      "password": "123456",
      "dbURL": "jdbc:mysql://localhost/test1?useSSL=false&serverTimezone=UTC",
      "pollTime": 3000,
      "pollQuery": "SELECT * FROM employee WHERE updateTime > CURRENT_TIMESTAMP - INTERVAL 3 SECOND"
   }
}
```
通过这个配置，实际上就是定时每3000毫秒运行一个SQL，将这个SQL执行的结果通过数据流的形式发送到VANTIQ上。


## Package and Start connector
At first, package the connector with:
```bash
# package
mvn package -Dmaven.test.skip=true 
```

在运行之前，在当前目录，准备一个config.json文件，它是connector的配置，内容如下：
```json
{
    "vantiqUrl": "http://localhost:8080",
    "token": "<the token>",
    "sourceName": "test_mysql_source"
}
```
这个里面的test_mysql_source，跟VANTIQ里面创建的souce名字对应。

然后就可以运行：
```bash
java -jar target/jdbc-connector-1.0-SNAPSHOT-spring-boot.jar
```


阿里工作的时候是使用Blink进行流数据处理和计算，通过编写sql实现Blink的计算job，开发简单高效，产品易用。目前想尝试实现Flink产品化，类似Blink这种产品。


   1.使用SQL为统一开发规范，SQL语言的好处是：声明式，易理解，稳定可靠，自动优化。如果采用API开发的话，最大的问题是对于job调优依赖程序员经验，比较困难，同时API开发方式侵入性太强(数据安全，集群安全等)。而sql可以自动调优，避免这两个问题的产生。
   
   
   2.目前实现思路：
   
   
    用户输入sql（ddl,dml,query）  -> ddl对应为Flink的source和sink


                                -> dml的insert into对应将对应数据加载到 sink 
                           
                           
                                -> query数据处理和计算
                           
                           
    --> 封装为api对应Flink的Job:env.sqlQuery/env.sqlUpdate;table.writeToSink;
    
    
    --> JobGraph和对应job提交; 
    
    
二.开发过程


   1.SqlConvertService是将用户sql语句解析为不同类型，source,sink,view,dml（目前只支持insert into）  
   
   SqlParserImpl SQL的解析  
     
   Validator  验证器  
   
   Planner    计划解析
   
   2.FlinkJobImpl是实现Flink的Source和Sink，以及JobGraph
   
   3.JobGraph的提交和执行,如StandaloneClusterClient.submitJob或者YarnClusterClient.runDetached    
   
   
三.代码关注

[apache flink](https://github.com/apache/flink)


[apache calcite](https://github.com/apache/calcite)


[uber AthenaX](https://github.com/uber/AthenaX)


[DTStack flinkStreamSQL](https://github.com/DTStack/flinkStreamSQL)

四.样例
```sql
CREATE FUNCTION demouf AS 
      'pingle.wang.api.sql.function.DemoUDF' 
USING JAR 'hdfs://flink/udf/jedis.jar',
      JAR 'hdfs://flink/udf/customudf.jar';
      
      
CREATE TABLE kafka_source (
      department VARCHAR,
      `date` DATE,
      amount float, 
      proctime timestamp
      ) 
with (
      type=kafka,
      'flink.parallelism'=1,
      'kafka.topic'=topic,
      'kafka.group.id'=flinks,
      'kafka.enable.auto.commit'=true,
      'kafka.bootstrap.servers'='localhost:9092'
);


CREATE TABLE mysql_sink (
      department VARCHAR,
      `date` DATE,
      amount float, 
      PRIMARY KEY (`date`,amount)
      ) 
with (
      type=mysql,
      'mysql.connection'='localhost:3306',
      'mysql.db.name'=flink,
      'mysql.batch.size'=0,
      'mysql.table.name'=flink_table,
      'mysql.user'=root,
      'mysql.pass'=root
);


CREATE VIEW view_select AS 
      SELECT `date`, 
              amount 
      FROM kafka_source 
      GROUP BY 
            `date`,
            amount
      ;


INSERT INTO mysql_sink 
       SELECT 
          `date`, 
          sum(amount) 
       FROM view_select 
       GROUP BY 
          `date`
      ;
```

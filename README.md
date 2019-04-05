一.背景
    
   阿里工作的时候是使用Blink进行流数据处理和计算，通过编写sql实现Blink的计算job，开发简单高效，产品易用。
   目前尝试实现Flink产品化，类似Blink。使用SQL为统一开发规范，SQL语言的好处是：声明式，易理解，稳定可靠，自动优化。
   如果采用API开发的话，最大的问题是对于job调优依赖程序员经验，比较困难，同时API开发方式侵入性太强(数据安全，集群安全等)，而sql可以自动调优，避免这种问题的产生。
   
二.实现思路：
   
    用户输入sql（ddl,query,dml）  -> ddl对应为Flink的source和sink
                           
                           
                                -> query/dml的insert into数据处理和计算
                           
                           
    --> 封装为对应Flink的Job:env.sqlQuery/env.sqlUpdate
    
    
    --> JobGraph和对应job提交，StandaloneClusterClient.submitJob或者YarnClusterClient.runDetached

三.发布版本：
    
   [v3.0.0](https://github.com/ambition119/FlinkSQL/tree/v3.0.0)   待开发
         使用最新发布版本的flink, 流批处理一体化实现
  
  
   [v2.0.0](https://github.com/ambition119/FlinkSQL/tree/v2.0.0)   2019年4月
   
        blink-client 接口定义
        blink-sql    stream和batch table的sql解析
        blink-libraries 自定义source, sink, side开发
        blink-batch  BatchTableSource和BatchTableSink
        blink-stream StreamTableSource和StreamTableSink
        blink-job  batch/stream job 提交
   
   SQL书写语法参考Flink issues和对应提供的doc:
        [SQL DDL ISSUE](https://issues.apache.org/jira/browse/FLINK-8039),
        [SQL DDL DOC](https://docs.google.com/document/d/1TTP-GCC8wSsibJaSUyFZ_5NBAHYEB1FVmPpP7RgDGBA/edit?usp=sharing)。
       
   [新特性](/doc/v2.0.0.md)
        
        1. 抽取sql层被流和批使用,SQL参考flink issues和对应提供的doc
        2. 增加批处理开发
        3. 增加维表功能
        4. 升级flink版本为1.7.x
           
四.样例

batch sql示例：
```sql
CREATE FUNCTION demouf AS 'ambition.api.sql.function.DemoUDF' 
LIBRARY 'hdfs://flink/udf/jedis.jar','hdfs://flink/udf/customudf.jar';

CREATE SOURCE TABLE json_source (
      id int, 
      name varchar, 
      `date` date , 
      age int
) 
with (
      type=json,
      'file.path'='file:///FlinkSQL/blink-job/src/test/resources/demo.json'
);

CREATE SINK TABLE csv_sink (
      `date` date, 
      total_age int
) 
with (
      type=csv,
      'file.path'='file:///FlinkSQL/blink-job/src/test/resources/demo_out.csv'
);

CREATE VIEW view_select as  
      SELECT `date`, age 
      FROM json_source 
      GROUP BY `date`,age;
  
INSERT INTO csv_sink 
      SELECT `date`, sum(age) as total_age
      FROM view_select 
      GROUP BY `date`;

```
stream sql 示例：
```sql
CREATE FUNCTION demouf AS 
      'ambition.api.sql.function.DemoUDF' 
LIBRARY 'hdfs://flink/udf/jedis.jar','hdfs://flink/udf/customudf.jar';
      
CREATE SOURCE TABLE kafka_source (
      `date` varchar,
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

CREATE SINK TABLE mysql_sink (
      `date` varchar, 
      total_amount float, 
      PRIMARY KEY (`date`)
      ) 
with (
      type=mysql,
      'mysql.connection'='localhost:3306',
      'mysql.db.name'=flink,
      'mysql.batch.size'=10,
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
          sum(amount) as total_amount
       FROM view_select 
       GROUP BY 
          `date`
      ;
```

五.代码关注

[apache flink](https://github.com/apache/flink)


[apache calcite](https://github.com/apache/calcite)


[uber AthenaX](https://github.com/uber/AthenaX)


[DTStack flinkStreamSQL](https://github.com/DTStack/flinkStreamSQL)  
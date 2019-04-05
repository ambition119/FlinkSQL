/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ambition.blink.stream.sql;

public interface SqlContent {
  String sqls = "CREATE FUNCTION demouf AS \n"
      + "      'ambition.api.sql.function.DemoUDF' \n"
      + "LIBRARY 'hdfs://flink/udf/jedis.jar','hdfs://flink/udf/customudf.jar';\n"
      + "      \n"
      + "CREATE SOURCE TABLE kafka_source (\n"
      + "      `date` varchar,\n"
      + "      amount float, \n"
      + "      proctime timestamp\n"
      + "      ) \n"
      + "with (\n"
      + "      type=kafka,\n"
      + "      'flink.parallelism'=1,\n"
      + "      'kafka.topic'=topic,\n"
      + "      'kafka.group.id'=flinks,\n"
      + "      'kafka.enable.auto.commit'=true,\n"
      + "      'kafka.bootstrap.servers'='localhost:9092'\n"
      + ");\n"
      + "\n"
      + "CREATE SINK TABLE mysql_sink (\n"
      + "      `date` varchar, \n"
      + "      amount float, \n"
      + "      PRIMARY KEY (`date`)\n"
      + "      ) \n"
      + "with (\n"
      + "      type=mysql,\n"
      + "      'mysql.connection'='localhost:3306',\n"
      + "      'mysql.db.name'=flink,\n"
      + "      'mysql.batch.size'=0,\n"
      + "      'mysql.table.name'=flink_table,\n"
      + "      'mysql.user'=root,\n"
      + "      'mysql.pass'=root\n"
      + ");\n"
      + "\n"
      + "CREATE VIEW view_select AS \n"
      + "      SELECT `date`, \n"
      + "              amount \n"
      + "      FROM kafka_source \n"
      + "      GROUP BY \n"
      + "            `date`,\n"
      + "            amount\n"
      + "      ;\n"
      + "\n"
      + "\n"
      + "INSERT INTO mysql_sink \n"
      + "       SELECT \n"
      + "          `date`, \n"
      + "          sum(amount) \n"
      + "       FROM view_select \n"
      + "       GROUP BY \n"
      + "          `date`\n"
      + "      ;";
}

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

package ambition.blink.sql;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.junit.Assert;
import org.junit.Test;

/**
 * @Date: 2020/1/2
 */
public class SqlUtilsTest {

  @Test
  public void run(){
    System.out.println(System.currentTimeMillis());
  }

  @Test
  public void parseSqlTest() throws Exception {
    String table = "CREATE TABLE tbl1 (\n" +
        "  a bigint,\n" +
        "  h varchar, \n" +
        "  g as 2 * (a + 1), \n" +
        "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'), \n" +
        "  b varchar,\n" +
        "  proc as PROCTIME(), \n" +
        "  PRIMARY KEY (a, b)\n" +
        ")\n" +
        "with (\n" +
        "    'connector' = 'kafka', \n" +
        "    'kafka.topic' = 'log.test'\n" +
        ")\n";
    SqlNodeList nodeList = SqlUtils.parseSql(table);
    for(SqlNode sqlNode : nodeList){
      Assert.assertNotNull(sqlNode);
    }

    String watermark = "CREATE TABLE tbl1 (\n" +
        "  ts timestamp(3),\n" +
        "  id varchar, \n" +
        "  watermark FOR ts AS ts - interval '3' second\n" +
        ")\n" +
        "  with (\n" +
        "    'connector' = 'kafka', \n" +
        "    'kafka.topic' = 'log.test'\n" +
        ")\n";
    SqlNodeList nodeList1 = SqlUtils.parseSql(watermark);
    for(SqlNode sqlNode : nodeList1){
      Assert.assertNotNull(sqlNode);
    }

    String fun = "create function function1 as 'org.apache.fink.function.function1' language java";
    SqlNodeList nodeList2 = SqlUtils.parseSql(fun);
    for(SqlNode sqlNode : nodeList2){
      Assert.assertNotNull(sqlNode);
    }

    String insert = "insert into emp(x,y) partition (x='ab', y='bc') select * from emp";
    SqlNodeList nodeList3 = SqlUtils.parseSql(insert);
    for(SqlNode sqlNode : nodeList3){
      Assert.assertNotNull(sqlNode);
    }
  }
}

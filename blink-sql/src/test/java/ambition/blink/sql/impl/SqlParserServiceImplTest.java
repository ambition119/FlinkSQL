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

package ambition.blink.sql.impl;

import ambition.blink.sql.SqlParserService;
import java.util.List;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.junit.Assert;
import org.junit.Test;

/**
 * @Date: 2020/1/2
 */
public class SqlParserServiceImplTest {
  private SqlParserService service = new SqlParserServiceImpl();

  @Test
  public void sqlFunctionParserTest() throws Exception {
    String fun = "create temporary function function1 as 'org.apache.fink.function.function1' language java";
    List<SqlCreateFunction> list = service.sqlFunctionParser(fun);
    Assert.assertEquals(list.size(), 1);
  }

  @Test
  public void sqlTableParserTest() throws Exception {
    String table = "CREATE TABLE tbl1 (\n" +
        "  `a` bigint,\n" +
        "  `date` date,\n" +
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
    List<SqlCreateTable> list = service.sqlTableParser(table);
    Assert.assertEquals(list.size(), 1);
  }

  @Test
  public void sqlViewParserTest() throws Exception {
    String view = "create view view_select as  " +
        "SELECT " +
        "`date`, " +
        "amount " +
        "FROM " +
        "kafak_source " +
        "group by `date`,amount;";

    List<SqlCreateView> list = service.sqlViewParser(view);
    Assert.assertEquals(list.size(), 1);
  }

  @Test
  public void sqlDmlParserTest() throws Exception {
    String insert = "insert into emp(x,y) partition (x='ab', y='bc') select * from emp";
    List<RichSqlInsert> list = service.sqlDmlParser(insert);
    Assert.assertEquals(list.size(), 1);
  }

}

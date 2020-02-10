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

import ambition.blink.sql.SqlConstant;
import ambition.blink.sql.SqlConvertService;
import ambition.blink.sql.SqlParserService;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.junit.Assert;
import org.junit.Test;

public class SqlConvertServiceImplTest {
  private SqlConvertService sqlService =  new SqlConvertServiceImpl();
  private SqlParserService sqlParserService =  new SqlParserServiceImpl();

  @Test
  public void sqlConvertTest() throws Exception {
    String sqls = "CREATE FUNCTION " +
        "demouf " +
        "AS " +
        "'ambition.api.sql.function.DemoUDF';" +

        "CREATE TABLE kafak_source (" +
        "name varchar, " +
        "amount float, " +
        "`date` date," +
        "watermark for `date` AS withOffset(`date`,1000) " +
        ") " +
        "with (" +
        " 'connector' = 'kafka',\n" +
        " 'kafka.topic' = 'log.test'\n" +
        ");" +

        "CREATE TABLE mysql_sink (" +
        "`date` date, " +
        "amount float, " +
        "PRIMARY KEY (`date`,amount)) " +
        "with (" +
        "  'connector' = 'mysql',\n" +
        "  'kafka.topic' = 'log.test'\n" +
        ");" +

        "create view view_select as  " +
        "SELECT " +
        "`date`, " +
        "amount " +
        "FROM " +
        "kafak_source " +
        "group by `date`,amount;" +

        "insert into mysql_sink " +
        "SELECT " +
        "`date`, " +
        "sum(amount) " +
        "FROM " +
        "view_select " +
        "group by `date`;";

    Map<String, List<String>> map = sqlService.sqlConvert(sqls);
    Assert.assertEquals(map.size(), 4);

    // insert into
    List<String> insertList = map.get(SqlConstant.INSERT);
    Map<String, RichSqlInsert> dmlSqlNodes = new HashMap<>();
    for (String sql : insertList) {
      List<RichSqlInsert> richSqlInserts = sqlParserService.sqlDmlParser(sql);
      if (CollectionUtils.isNotEmpty(richSqlInserts)) {
        for (RichSqlInsert richSqlInsert: richSqlInserts) {
          SqlNode table = richSqlInsert.getTargetTable();
          dmlSqlNodes.put(table.toString(), richSqlInsert);
        }
      }
    }

    //table
    List<String> tableList = map.get(SqlConstant.TABLE);
    Map<String, SqlCreateTable> tableSqlNodes = new HashMap<>();
    for (String sql : tableList) {
      List<SqlCreateTable> sqlCreateTables = sqlParserService.sqlTableParser(sql);
      if (CollectionUtils.isNotEmpty(sqlCreateTables)) {
        for (SqlCreateTable sqlCreateTable : sqlCreateTables) {
          tableSqlNodes.put(sqlCreateTable.getTableName().toString(), sqlCreateTable);
        }
      }
    }

    MapDifference<String, ? extends SqlCall> difference = Maps
        .difference(dmlSqlNodes, tableSqlNodes);

    System.out.println(difference);

    Map<String, ? extends SqlCall> sourceMap = difference.entriesOnlyOnRight();
    MapDifference<String, ? extends SqlCall> tableDifference = Maps.difference(sourceMap, tableSqlNodes);
    Map<String, ? extends SqlCall> sinkMap = tableDifference.entriesOnlyOnRight();

    //处理source
    for (String sourceTableName: sourceMap.keySet()) {
      SqlCall sqlCall = sourceMap.get(sourceTableName);
      if (sqlCall instanceof SqlCreateTable) {
        SqlNodeList columnList = ((SqlCreateTable) sqlCall).getColumnList();
        System.out.println(columnList);
        System.out.println(sqlCall.toString());
      }
    }
  }

}

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
import ambition.blink.sql.SqlUtils;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.dml.RichSqlInsert;

/**
 * @Date: 2020/1/2
 */
public class SqlParserServiceImpl implements SqlParserService {

  @Override
  public List<SqlCreateFunction> sqlFunctionParser(String funSql) throws Exception {
    List<SqlCreateFunction> result = new LinkedList<>();
    SqlNodeList nodeList = SqlUtils.parseSql(funSql);
    for (SqlNode sqlNode : nodeList) {
      if (sqlNode instanceof SqlCreateFunction) {
        result.add((SqlCreateFunction) sqlNode);
      }
    }
    return result;
  }

  @Override
  public List<SqlCreateTable> sqlTableParser(String ddlSql) throws Exception {
    List<SqlCreateTable> result = new LinkedList<>();
    SqlNodeList nodeList = SqlUtils.parseSql(ddlSql);
    for (SqlNode sqlNode : nodeList) {
      if (sqlNode instanceof SqlCreateTable) {
        result.add((SqlCreateTable) sqlNode);

        SqlNodeList columnList = ((SqlCreateTable) sqlNode).getColumnList();

        List<SqlTableColumn> tableColumnList = new ArrayList<>();
        List<SqlBasicCall> basicCallList = new ArrayList<>();
        Map<String,String> tableColumnInfo = new LinkedHashMap<>();
        List<String> basicCallInfo = new ArrayList<>();

        for (SqlNode sqlNode1: columnList) {
          if (sqlNode1 instanceof  SqlTableColumn) {
            tableColumnList.add((SqlTableColumn) sqlNode1);

            SqlIdentifier name = ((SqlTableColumn) sqlNode1).getName();
            SqlDataTypeSpec type = ((SqlTableColumn) sqlNode1).getType();
            tableColumnInfo.put(name.toString(), type.toString());
          }

          if (sqlNode1 instanceof  SqlBasicCall) {
            basicCallList.add((SqlBasicCall) sqlNode1);
            System.out.println(sqlNode1.toString());
            basicCallInfo.add(sqlNode1.toString());
          }
        }
      }
    }
    return result;
  }

  @Override
  public List<SqlCreateView> sqlViewParser(String viewSql) throws Exception {
    List<SqlCreateView> result = new LinkedList<>();
    SqlNodeList nodeList = SqlUtils.parseSql(viewSql);
    for (SqlNode sqlNode : nodeList) {
      if (sqlNode instanceof SqlCreateView) {
        result.add((SqlCreateView) sqlNode);
      }
    }
    return result;
  }

  @Override
  public List<RichSqlInsert> sqlDmlParser(String insertSql) throws Exception {
    List<RichSqlInsert> result = new LinkedList<>();
    SqlNodeList nodeList = SqlUtils.parseSql(insertSql);
    for (SqlNode sqlNode : nodeList) {
      if (sqlNode instanceof RichSqlInsert) {
        result.add((RichSqlInsert) sqlNode);
      }
    }
    return result;
  }

}

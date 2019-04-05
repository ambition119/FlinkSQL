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

package ambition.blink.sql.parser;

import static org.apache.calcite.sql.SqlKind.IDENTIFIER;

import ambition.blink.sql.SqlConstant;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import ambition.blink.common.table.ViewOrInsertInfo;

public class InsertSqlParser{

  public static InsertSqlParser newInstance(){
    InsertSqlParser parser = new InsertSqlParser();
    return parser;
  }

  public ViewOrInsertInfo parseSql(String sql) {
    SqlParser.Config config = SqlParser
        .configBuilder()
        .setLex(Lex.JAVA)
        .setCaseSensitive(false)
        .build();
    if (sql.endsWith(SqlConstant.SQL_END_FLAG)) {
      sql = sql.substring(0, sql.length() - 1);
    }
    SqlParser sqlParser = SqlParser.create(sql,config);

    SqlNode sqlNode = null;
    try {
      sqlNode = sqlParser.parseStmt();
    } catch (SqlParseException e) {
      throw new RuntimeException("sql parse exception", e);
    }

    SqlParseResult sqlParseResult = new SqlParseResult();
    parseNode(sqlNode, sqlParseResult);
    sqlParseResult.setExecSql(sqlNode.toString());

    ViewOrInsertInfo insertInfo = new ViewOrInsertInfo();
    insertInfo.setExecSql(sqlParseResult.execSql);
    return insertInfo;
  }

  private static void parseNode(SqlNode sqlNode, SqlParseResult sqlParseResult){
    SqlKind sqlKind = sqlNode.getKind();
    switch (sqlKind){
      case INSERT:
        SqlNode sqlTarget = ((SqlInsert)sqlNode).getTargetTable();
        SqlNode sqlSource = ((SqlInsert)sqlNode).getSource();
        sqlParseResult.addTargetTable(sqlTarget.toString());
        parseNode(sqlSource, sqlParseResult);
        break;
      case SELECT:
        SqlNode sqlFrom = ((SqlSelect)sqlNode).getFrom();
        if(sqlFrom.getKind() == IDENTIFIER){
          sqlParseResult.addSourceTable(sqlFrom.toString());
        }else{
          parseNode(sqlFrom, sqlParseResult);
        }
        break;
      case JOIN:
        SqlNode leftNode = ((SqlJoin)sqlNode).getLeft();
        SqlNode rightNode = ((SqlJoin)sqlNode).getRight();

        if(leftNode.getKind() == IDENTIFIER){
          sqlParseResult.addSourceTable(leftNode.toString());
        }else{
          parseNode(leftNode, sqlParseResult);
        }

        if(rightNode.getKind() == IDENTIFIER){
          sqlParseResult.addSourceTable(rightNode.toString());
        }else{
          parseNode(rightNode, sqlParseResult);
        }
        break;
      case AS:
        //不解析column,所以 as 相关的都是表
        SqlNode identifierNode = ((SqlBasicCall)sqlNode).getOperands()[0];
        if(identifierNode.getKind() != IDENTIFIER){
          parseNode(identifierNode, sqlParseResult);
        }else {
          sqlParseResult.addSourceTable(identifierNode.toString());
        }
        break;
      case UNION:
        SqlNode unionLeft = ((SqlBasicCall)sqlNode).getOperands()[0];
        SqlNode unionRight = ((SqlBasicCall)sqlNode).getOperands()[1];
        if(unionLeft.getKind() == IDENTIFIER){
          sqlParseResult.addSourceTable(unionLeft.toString());
        }else{
          parseNode(unionLeft, sqlParseResult);
        }
        if(unionRight.getKind() == IDENTIFIER){
          sqlParseResult.addSourceTable(unionRight.toString());
        }else{
          parseNode(unionRight, sqlParseResult);
        }
        break;
      default:
        //do nothing
        break;
    }
  }

  public static class SqlParseResult {

    private List<String> sourceTableList = new ArrayList<>();

    private List<String> targetTableList = new ArrayList<>();

    private String execSql;

    public void addSourceTable(String sourceTable){
      sourceTableList.add(sourceTable);
    }

    public void addTargetTable(String targetTable){
      targetTableList.add(targetTable);
    }

    public List<String> getSourceTableList() {
      return sourceTableList;
    }

    public List<String> getTargetTableList() {
      return targetTableList;
    }

    public String getExecSql() {
      return execSql;
    }

    public void setExecSql(String execSql) {
      this.execSql = execSql;
    }
  }
}

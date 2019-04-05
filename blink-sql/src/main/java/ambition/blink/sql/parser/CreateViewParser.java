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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import ambition.blink.common.table.ViewOrInsertInfo;

public class CreateViewParser {

  //select table tableName as select
  private static final String PATTERN_STR = "(?i)create\\s+view\\s+([^\\s]+)\\s+as\\s+select\\s+(.*)";

  private static final String EMPTY_STR = "(?i)^\\screate\\s+view\\s+(\\S+)\\s*\\((.+)\\)$";

  private static final Pattern NONEMPTYVIEW = Pattern.compile(PATTERN_STR);

  private static final Pattern EMPTYVIEW = Pattern.compile(EMPTY_STR);

  public static CreateViewParser newInstance(){
    return new CreateViewParser();
  }

  public ViewOrInsertInfo parseSql(String sql) {
    SqlParserResult result = null;
    if (NONEMPTYVIEW.matcher(sql).find()){
      Matcher matcher = NONEMPTYVIEW.matcher(sql);
      String tableName = null;
      String selectSql = null;
      if(matcher.find()) {
        tableName = matcher.group(1);
        selectSql = "select " + matcher.group(2).trim();
      }

      if (selectSql.endsWith(SqlConstant.SQL_END_FLAG)) {
        selectSql = selectSql.substring(0, selectSql.length() - 1);
      }

      SqlParser.Config config = SqlParser
          .configBuilder()
          .setLex(Lex.JAVA)
          .build();
      SqlParser sqlParser = SqlParser.create(selectSql,config);

      SqlNode sqlNode = null;
      try {
        sqlNode = sqlParser.parseStmt();
      } catch (SqlParseException e) {
        throw new RuntimeException("", e);
      }

      result = new SqlParserResult();
      parseNode(sqlNode, result);

      result.setTableName(tableName);
      String transformSelectSql = sqlNode.toString();
      result.setExecSql(transformSelectSql);
    } else {
      if (EMPTYVIEW.matcher(sql).find())
      {
        Matcher matcher = EMPTYVIEW.matcher(sql);
        String tableName = null;
        String fieldsInfoStr = null;
        if (matcher.find()){
          tableName = matcher.group(1).toUpperCase();
          fieldsInfoStr = matcher.group(2);
        }
        CreateViewParser.SqlParserResult sqlParseResult = new CreateViewParser.SqlParserResult();
        sqlParseResult.setFieldsInfoStr(fieldsInfoStr);
        sqlParseResult.setTableName(tableName);
      }

    }

    ViewOrInsertInfo viewInfo = new ViewOrInsertInfo();
    viewInfo.setTableName(result.tableName);
    viewInfo.setFieldsInfoStr(result.fieldsInfoStr);
    viewInfo.setExecSql(result.execSql);

    return viewInfo;
  }

  public static void parseNode(SqlNode sqlNode, CreateViewParser.SqlParserResult sqlParseResult){
    SqlKind sqlKind = sqlNode.getKind();
    switch (sqlKind){
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

  public static class SqlParserResult {
    private String tableName;

    private String fieldsInfoStr;

    private String execSql;

    private List<String> sourceTableList = new ArrayList<>();

    public String getTableName() {
      return tableName;
    }

    public void setTableName(String tableName) {
      this.tableName = tableName;
    }

    public String getExecSql() {
      return execSql;
    }

    public void setExecSql(String execSql) {
      this.execSql = execSql;
    }

    public String getFieldsInfoStr() {
      return fieldsInfoStr;
    }

    public void setFieldsInfoStr(String fieldsInfoStr) {
      this.fieldsInfoStr = fieldsInfoStr;
    }

    public void addSourceTable(String sourceTable){
      sourceTableList.add(sourceTable);
    }

    public List<String> getSourceTableList() {
      return sourceTableList;
    }

  }
}

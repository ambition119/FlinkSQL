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
import ambition.blink.sql.SqlService;
import ambition.blink.sql.parser.CreateFuncParser;
import ambition.blink.sql.parser.CreateViewParser;
import ambition.blink.sql.parser.InsertSqlParser;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ambition.blink.sql.parser.TableInfoParser;
import ambition.blink.common.exception.FlinkSqlException;
import ambition.blink.common.table.FunctionInfo;
import ambition.blink.common.table.TableInfo;
import ambition.blink.common.table.TableType;
import ambition.blink.common.table.ViewOrInsertInfo;
import ambition.blink.common.utils.BlinkStringUtils;
import ambition.blink.sql.parser.InputSqlPattern;

public class SqlServiceImpl implements SqlService {

  private static final Logger LOG = LoggerFactory.getLogger(SqlServiceImpl.class);

  @Override
  public Map<String, List<String>> sqlConvert(String sqlContext) throws Exception {
    if (StringUtils.isBlank(sqlContext)){
      LOG.warn("sql is null");
      throw new FlinkSqlException("input sql is null, sql not null");
    }

    List<String> list = BlinkStringUtils.splitSemiColon(sqlContext);
    if (CollectionUtil.isNullOrEmpty(list)) {
      LOG.warn("sql not use quota error");
      throw new FlinkSqlException("sql not use quota error, sql not null");
    }

    if (BlinkStringUtils.isChinese(sqlContext)){
      LOG.info("sql contain Chinese char {} ", sqlContext);
      throw new FlinkSqlException("sql contain Chinese char");
    }

    if (!sqlContext.toUpperCase().contains(SqlConstant.TABLE_SOURCE)){
      LOG.warn("sql not contain create source");
      throw new FlinkSqlException("sql not contain create source");
    }

    if (!sqlContext.toUpperCase().contains(SqlConstant.TABLE_SINK)){
      LOG.warn("sql not contain create sink");
      throw new FlinkSqlException("sql not contain create sink");
    }

    //sql拆解
    List<String> sqls = BlinkStringUtils.splitSemiColon(sqlContext);

    Map<String,List<String>> result = new HashMap<>();
    //sql顺序的一致
    List<String> commList = new LinkedList<>();
    List<String> funList = new LinkedList<>();
    List<String> viewList = new LinkedList<>();
    List<String> sourceList = new LinkedList<>();
    List<String> sideList = new LinkedList<>();
    List<String> sinkList = new LinkedList<>();


    for (String sql: sqls) {
      if (StringUtils.isNotBlank(sql)){
        //替换多余的空格
        String newSql = sql.trim().replaceAll(" +", " ").replaceAll("\\s+", " ");

        if (newSql.toUpperCase().startsWith(SqlConstant.CREATE)){
          if (newSql.toUpperCase().contains(SqlConstant.CREATE+" "+ SqlConstant.TABLE_SOURCE)){
            sourceList.add(newSql+ SqlConstant.SQL_END_FLAG);
            continue;
          }

          if (newSql.toUpperCase().contains(SqlConstant.CREATE+" "+ SqlConstant.TABLE_SINK)){
            sinkList.add(newSql+ SqlConstant.SQL_END_FLAG);
            continue;
          }

          Matcher sideMatcher = InputSqlPattern.CREATE_TABLE_PATTERN.matcher(BlinkStringUtils.getReplaceString(newSql));
          if (sideMatcher.find()){
            //SIDE_KEY信息
            TableInfo tableInfo = TableInfoParser.parser(newSql);
            TableType tableType = tableInfo.getTableType();

            if (TableType.SIDE.getValue() == tableInfo.getTableType().getValue()){
              sideList.add(newSql+ SqlConstant.SQL_END_FLAG);
            }
            continue;
          }

          Matcher funMatcher = InputSqlPattern.CREATE_FUN_PATTERN.matcher(BlinkStringUtils.getReplaceString(newSql));
          if(funMatcher.find()){
            funList.add(newSql+ SqlConstant.SQL_END_FLAG);
            continue;
          }


          Matcher viewMatcher = InputSqlPattern.CREATE_VIEW_PATTERN.matcher(BlinkStringUtils.getReplaceString(newSql));
          if(viewMatcher.find()){
            viewList.add(newSql+ SqlConstant.SQL_END_FLAG);
            continue;
          }
        }else {
          commList.add(newSql+ SqlConstant.SQL_END_FLAG);
        }

      }
    }

    if (CollectionUtils.isNotEmpty(commList)){
      result.put(SqlConstant.INSERT_INTO,commList);
    }

    if (CollectionUtils.isNotEmpty(viewList)){
      result.put(SqlConstant.TABLE_VIEW,viewList);
    }

    if (CollectionUtils.isNotEmpty(funList)){
      result.put(SqlConstant.FUNCTION,funList);
    }

    if (CollectionUtils.isNotEmpty(sourceList)){
      result.put(SqlConstant.TABLE_SOURCE,sourceList);
    }

    if (CollectionUtils.isNotEmpty(sideList)){
      result.put(SqlConstant.TABLE_SIDE,sideList);
    }

    if (CollectionUtils.isNotEmpty(sinkList)){
      result.put(SqlConstant.TABLE_SINK,sinkList);
    }

    return result;
  }

  @Override
  public FunctionInfo sqlFunctionParser(String funSql) throws Exception {
    if (StringUtils.isBlank(funSql)) {
      throw new FlinkSqlException("sql is null, must not null");
    }

    return CreateFuncParser.newInstance().parseSql(funSql);
  }

  @Override
  public TableInfo sqlTableParser(String ddlSql) throws Exception {
    if (StringUtils.isBlank(ddlSql)) {
      throw new FlinkSqlException("sql is null, must not null");
    }

    return TableInfoParser.parser(ddlSql);
  }

  @Override
  public ViewOrInsertInfo sqlViewParser(String viewSql) throws Exception {
    if (StringUtils.isBlank(viewSql)) {
      throw new FlinkSqlException("sql is null, must not null");
    }

    return CreateViewParser.newInstance().parseSql(viewSql);
  }

  @Override
  public ViewOrInsertInfo sqlDmlParser(String insertSql) throws Exception {
    if (StringUtils.isBlank(insertSql)) {
      throw new FlinkSqlException("sql is null, must not null");
    }

    return InsertSqlParser.newInstance().parseSql(insertSql);
  }
}

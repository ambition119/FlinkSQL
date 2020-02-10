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

package ambition.blink.stream.job.impl;

import ambition.blink.common.job.JobParameter;
import ambition.blink.job.JobClient;
import ambition.blink.sql.SqlConstant;
import ambition.blink.sql.SqlParserService;
import ambition.blink.sql.impl.SqlParserServiceImpl;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.Plan;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamJobClientImpl implements JobClient {
  private static final Logger LOG = LoggerFactory.getLogger(StreamJobClientImpl.class);

  private SqlParserService sqlParserService = new SqlParserServiceImpl();

  @Override
  public Plan getJobPlan(JobParameter jobParameter, Map<String, String> extParams)
      throws Exception {
    return null;
  }

  @Override
  public JobGraph getJobGraph(JobParameter jobParameter, Map<String, String> extParams)
      throws Exception {
    StreamExecutionEnvironment env = getStreamLocalEnvironmentInfo(
        jobParameter, extParams);

    return env.getStreamGraph(jobParameter.getJobName()).getJobGraph();
  }

  private StreamExecutionEnvironment getStreamLocalEnvironmentInfo(JobParameter jobParameter, Map<String, String> extParams) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(
        env,
        EnvironmentSettings.newInstance()
            // watermark is only supported in blink planner
            .useBlinkPlanner()
            .inStreamingMode()
            .build()
    );

    //config
    TableConfig config = tEnv.getConfig();
//    config.addConfiguration();
//    config.addJobParameter();

    // sqls
    Map<String, List<String>> sqls = jobParameter.getSqls();

    // function
    if (sqls.containsKey(SqlConstant.FUNCTION)) {
      List<String> funList = sqls.get(SqlConstant.FUNCTION);
      if (CollectionUtils.isNotEmpty(funList)) {
        for (String sql: funList) {
          List<SqlCreateFunction> functions = sqlParserService.sqlFunctionParser(sql);
          for (SqlCreateFunction fun: functions) {
            tEnv.sqlUpdate(fun.toString());
          }
        }
      }
    }

    // insert into
    List<String> insertList = sqls.get(SqlConstant.INSERT);
    if (CollectionUtils.isEmpty(insertList)) {
      throw new IllegalArgumentException("insert into sql is not null");
    }

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
    List<String> tableList = sqls.get(SqlConstant.TABLE);
    if (CollectionUtils.isEmpty(tableList)) {
      throw new IllegalArgumentException("create table sql is not null");
    }

    Map<String, SqlCreateTable> tableSqlNodes = new HashMap<>();
    for (String sql : tableList) {
      List<SqlCreateTable> sqlCreateTables = sqlParserService.sqlTableParser(sql);
      if (CollectionUtils.isNotEmpty(sqlCreateTables)) {
        for (SqlCreateTable sqlCreateTable : sqlCreateTables) {
            tableSqlNodes.put(sqlCreateTable.getTableName().toString(), sqlCreateTable);
        }
      }
    }

    // 通过insert into 获取 source, sink的table信息
    MapDifference<String, ? extends SqlCall> difference = Maps.difference(dmlSqlNodes, tableSqlNodes);
    //source
    Map<String, ? extends SqlCall> sourceMap = difference.entriesOnlyOnRight();
    //sink
    MapDifference<String, ? extends SqlCall> tableDifference = Maps.difference(sourceMap, tableSqlNodes);
    Map<String, ? extends SqlCall> sinkMap = tableDifference.entriesOnlyOnRight();

    //处理source
    for (String sourceTableName: sourceMap.keySet()) {
      SqlCall sqlCall = sourceMap.get(sourceTableName);
      if (sqlCall instanceof SqlCreateTable) {
        tEnv.sqlUpdate(sqlCall.toString());
      }
    }
    //处理sink
    for (String sinkTableName: sinkMap.keySet()) {
      SqlCall sqlCall = sinkMap.get(sinkTableName);
      if (sqlCall instanceof SqlCreateTable) {
        tEnv.sqlUpdate(sqlCall.toString());
      }
    }

    // view
    if (sqls.containsKey(SqlConstant.VIEW)) {
      List<String> viewList = sqls.get(SqlConstant.VIEW);
      if (CollectionUtils.isNotEmpty(viewList)) {
        for (String sql : viewList) {
          List<SqlCreateView> views = sqlParserService.sqlViewParser(sql);
          if (CollectionUtils.isNotEmpty(views)) {
            for (SqlCreateView view : views) {
              Table table = tEnv.sqlQuery(view.getQuery().toString());
              tEnv.createTemporaryView(view.getViewName().toString(), table);
            }
          }
        }
      }
    }

    for (String dml : dmlSqlNodes.keySet()) {
      RichSqlInsert richSqlInsert = dmlSqlNodes.get(dml);
      tEnv.sqlUpdate(richSqlInsert.toString());
    }

    return env;
  }

  private StreamExecutionEnvironment getStreamLocalEnvironmentInfoV2(JobParameter jobParameter, Map<String, String> extParams) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(
        env,
        EnvironmentSettings.newInstance()
            // watermark is only supported in blink planner
            .useBlinkPlanner()
            .inStreamingMode()
            .build()
    );

    //config
    TableConfig config = tEnv.getConfig();
//    config.addConfiguration();
//    config.addJobParameter();

    // sqls
    Map<String, List<String>> sqls = jobParameter.getSqls();
    // function
    if (sqls.containsKey(SqlConstant.FUNCTION)) {

    }

    //table
    List<String> tableList = sqls.get(SqlConstant.TABLE);
    if (CollectionUtils.isEmpty(tableList)) {
      throw new IllegalArgumentException("create table sql is not null");
    }

    for (String sql : tableList) {
      List<SqlCreateTable> sqlCreateTables = sqlParserService.sqlTableParser(sql);
      if (CollectionUtils.isNotEmpty(sqlCreateTables)) {
        for (SqlCreateTable sqlCreateTable : sqlCreateTables) {
          tEnv.sqlUpdate(sqlCreateTable.toString());
        }
      }
    }

    // view
    if (sqls.containsKey(SqlConstant.VIEW)) {
      List<String> viewList = sqls.get(SqlConstant.VIEW);
      if (CollectionUtils.isNotEmpty(viewList)) {
        for (String sql : viewList) {
          List<SqlCreateView> views = sqlParserService.sqlViewParser(sql);
          if (CollectionUtils.isNotEmpty(views)) {
            for (SqlCreateView view : views) {
              Table table = tEnv.sqlQuery(view.getQuery().toString());
              tEnv.createTemporaryView(view.getViewName().toString(), table);
            }
          }
        }
      }
    }

    // insert into
    List<String> insertList = sqls.get(SqlConstant.INSERT);
    if (CollectionUtils.isEmpty(insertList)) {
      throw new IllegalArgumentException("insert into sql is not null");
    }

    Map<String, RichSqlInsert> dmlSqlNodes = new HashMap<>();
    for (String sql : insertList) {
      List<RichSqlInsert> richSqlInserts = sqlParserService.sqlDmlParser(sql);
      if (CollectionUtils.isNotEmpty(richSqlInserts)) {
        for (RichSqlInsert richSqlInsert: richSqlInserts) {
          tEnv.sqlUpdate(richSqlInsert.toString());
        }
      }
    }

    return env;
  }

}

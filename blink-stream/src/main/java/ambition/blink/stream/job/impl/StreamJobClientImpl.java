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
import ambition.blink.common.table.TableInfo;
import ambition.blink.common.table.ViewOrInsertInfo;
import ambition.blink.job.JobClient;
import ambition.blink.sql.SqlConstant;
import ambition.blink.sql.SqlService;
import ambition.blink.sql.impl.SqlServiceImpl;
import ambition.blink.stream.StreamTableUtils;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamJobClientImpl implements JobClient {
  private static final Logger LOG = LoggerFactory.getLogger(StreamJobClientImpl.class);

  private SqlService sqlService = new SqlServiceImpl();

  @Override
  public Plan getJobPlan(JobParameter jobParameter, Map<String, String> extParams)
      throws Exception {

    return null;
  }

  @Override
  public JobGraph getJobGraph(JobParameter jobParameter, Map<String, String> extParams)
      throws Exception {

    return null;
  }

  public StreamGraph getStreamGraph(JobParameter jobParameter, Map<String, String> extParams)
      throws Exception {
    StreamExecutionEnvironment env = getStreamLocalEnvironmentInfo(
        jobParameter, extParams);

    return env.getStreamGraph();
  }

  private StreamExecutionEnvironment getStreamLocalEnvironmentInfo(JobParameter jobParameter, Map<String, String> extParams) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

    StreamTableEnvironment tEnv = StreamTableEnvironment.getTableEnvironment(env);

    // sqls
    Map<String, List<String>> sqls = jobParameter.getSqls();

    //udf

    // config
    StreamQueryConfig queryConfig = tEnv.queryConfig();

    // source
    List<String> sourceSqls = sqls.get(SqlConstant.TABLE_SOURCE);
    if (CollectionUtils.isEmpty(sourceSqls)) {
      throw new IllegalArgumentException("source table is not null");
    }

    //sink
    List<String> sinkSqls = sqls.get(SqlConstant.TABLE_SINK);
    if (CollectionUtils.isEmpty(sinkSqls)) {
      throw new IllegalArgumentException("sink table is not null");
    }

    for (String sql: sourceSqls) {
      TableInfo tableInfo = sqlService.sqlTableParser(sql);

      TableSource tableSource = StreamTableUtils.getTableSource(tableInfo, extParams);

      tEnv.registerTableSource(tableInfo.getTableName(), tableSource);
    }

    for (String sql: sinkSqls) {
      TableInfo tableInfo = sqlService.sqlTableParser(sql);
      Map<String, TypeInformation<?>> outputSchemaMap = tableInfo.getFlinkUseSchema();
      String[] fieldNames = new String[outputSchemaMap.size()];
      String[] fieldNamesArray = outputSchemaMap.keySet().toArray(fieldNames);

      TypeInformation[] fieldTypes = new TypeInformation[outputSchemaMap.size()];
      TypeInformation[] fieldTypesArray = outputSchemaMap.values().toArray(fieldTypes);

      TableSink tableSink = StreamTableUtils.getTableSink(tableInfo, extParams);
      tEnv.registerTableSink(tableInfo.getTableName(), fieldNamesArray, fieldTypesArray, tableSink);
    }

    //是否含有view的操作
    if (sqls.containsKey(SqlConstant.TABLE_VIEW)){
      //视图sql
      List<String> viewSqls = sqls.get(SqlConstant.TABLE_VIEW);
      if (CollectionUtils.isNotEmpty(viewSqls)){
        for (String sql : viewSqls){
          ViewOrInsertInfo viewInfo = sqlService.sqlViewParser(sql);

          String tableName = viewInfo.getTableName();
          String selectBody = viewInfo.getExecSql();

          Table viewTable = tEnv.sqlQuery(selectBody);
          tEnv.registerTable(tableName, viewTable);
          LOG.info("sql query info {}", selectBody);
        }
      }
    }

    List<String> dmlSqls = sqls.get(SqlConstant.INSERT_INTO);
    if (CollectionUtils.isNotEmpty(dmlSqls)) {
      for (String sql: dmlSqls){
        ViewOrInsertInfo insertInfo = sqlService.sqlDmlParser(sql);
        String selectBody = insertInfo.getExecSql();
        tEnv.sqlUpdate(selectBody,queryConfig);
        LOG.info("sql update info {}", selectBody);
      }
    }
    return env;
  }
}

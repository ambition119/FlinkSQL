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

package ambition.blink.job;

import ambition.blink.batch.job.impl.BatchJobClientImpl;
import ambition.blink.common.job.JobParameter;
import ambition.blink.sql.SqlService;
import ambition.blink.sql.impl.SqlServiceImpl;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.Plan;
import org.apache.flink.client.LocalExecutor;

public class BatchJobLocalRunExample {

  public static void main(String[] args) throws Exception {
    JobClient jobClient = new BatchJobClientImpl();
    JobParameter jobParameter = new JobParameter();
    SqlService sqlService = new SqlServiceImpl();
    Map<String, List<String>> map = sqlService.sqlConvert(sqls);
    jobParameter.setSqls(map);
    jobParameter.setJobName("batch_test");

    LocalExecutor executor = new LocalExecutor();
    executor.setDefaultOverwriteFiles(true);
    executor.setTaskManagerNumSlots(1);
    executor.setPrintStatusDuringExecution(false);
    executor.start();
    Plan jobPlan = jobClient.getJobPlan(jobParameter, null);
    jobPlan.setExecutionConfig(new ExecutionConfig());
    executor.executePlan(jobPlan);
    executor.stop();
  }

  private String getPath(String fileName) {
    return getClass().getClassLoader().getResource(fileName).getPath();
  }

  static String sqls =
      "CREATE SOURCE TABLE csv_source (" +
      "id int, " +
      "name varchar, " +
      "`date` date , " +
      "age int" +
      ") " +
      "with (" +
      "type=json," +
      "'file.path'='file:///FlinkSQL/blink-job/src/test/resources/demo.json'" +
      ");" +

      "CREATE SINK TABLE csv_sink (" +
      "`date` date, " +
      "age int, " +
      "PRIMARY KEY (`date`)) " +
      "with (" +
      "type=csv," +
      "'file.path'='file:///FlinkSQL/blink-job/src/test/resources/demo_out.csv'" +
      ");" +

      "create view view_select as  " +
      "SELECT " +
      "`date`, " +
      "age " +
      "FROM " +
      "csv_source " +
      "group by `date`,age;" +

      "insert " +
      "into csv_sink " +
      "SELECT " +
      "`date`, " +
      "sum(age) " +
      "FROM " +
      "view_select " +
      "group by `date`;";

}

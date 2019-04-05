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

package ambition.blink.batch.job.impl;

import ambition.blink.sql.SqlService;
import ambition.blink.sql.impl.SqlServiceImpl;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.Plan;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ambition.blink.common.job.JobParameter;
import ambition.blink.job.JobClient;
import ambition.blink.sql.SqlContent;

public class BatchJobClientImplTest {

  private JobParameter jobParameter;
  private JobClient jobClient = new BatchJobClientImpl();

  @Before
  public void init() throws Exception {
    System.out.println(SqlContent.sqls);
    jobParameter = new JobParameter();
    SqlService sqlService = new SqlServiceImpl();
    Map<String, List<String>> map = sqlService.sqlConvert(SqlContent.sqls);
    jobParameter.setSqls(map);
    jobParameter.setJobName("batch_test");
  }

  @Test
  public void getJobGraphTest() throws Exception {
    JobGraph jobGraph = jobClient.getJobGraph(jobParameter, null);
    Assert.assertNotNull(jobGraph);
  }

  @Test
  public void getJobPlanTest() throws Exception {
    Plan jobPlan = jobClient.getJobPlan(jobParameter, null);
    Assert.assertNotNull(jobPlan);
  }

}

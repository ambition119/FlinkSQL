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

import ambition.blink.common.job.JobParameter;
import ambition.blink.sql.SqlService;
import ambition.blink.sql.impl.SqlServiceImpl;
import ambition.blink.stream.job.impl.StreamJobClientImpl;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.StandaloneClusterDescriptor;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.graph.StreamGraph;

public class StreamStandaloneJobRunExample {

  public static void main(String[] args) throws Exception {
    final String configuredHostname = "localhost";
    final int configuredPort = 1234;
    final Configuration configuration = new Configuration();

    configuration.setString(JobManagerOptions.ADDRESS, configuredHostname);
    configuration.setInteger(JobManagerOptions.PORT, configuredPort);
    configuration.setString(RestOptions.ADDRESS, configuredHostname);
    configuration.setInteger(RestOptions.PORT, configuredPort);

    final DefaultCLI defaultCLI = new DefaultCLI(configuration);

    final String manualHostname = "123.123.123.123";
    final int manualPort = 4321;
    final String[] mpars = {"-m", manualHostname + ':' + manualPort};

    CommandLine commandLine = defaultCLI.parseCommandLineOptions(mpars, false);

    ClusterSpecification clusterSpecification = defaultCLI.getClusterSpecification(commandLine);

    final StandaloneClusterDescriptor clusterDescriptor = defaultCLI.createClusterDescriptor(commandLine);

    JobParameter jobParameter = new JobParameter();
    SqlService sqlService = new SqlServiceImpl();
    Map<String, List<String>> map = sqlService.sqlConvert("");
    jobParameter.setSqls(map);
    jobParameter.setJobName("streamExample");
    StreamGraph streamGraph = new StreamJobClientImpl().getStreamGraph(jobParameter, null);

    clusterDescriptor.deployJobCluster(clusterSpecification, streamGraph.getJobGraph(), true);


    final RestClusterClient<?> clusterClient = clusterDescriptor.retrieve(defaultCLI.getClusterId(commandLine));

  }



}

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

package ambition.blink.yarn.job;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Collections;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public abstract class YarnBase {

  private YarnClient yarnClient = null;


  protected static final YarnConfiguration YARN_CONFIGURATION;

  private static org.apache.flink.configuration.Configuration globalConfiguration;

  protected org.apache.flink.configuration.Configuration flinkConfiguration;

  static {
    YARN_CONFIGURATION = new YarnConfiguration();
    YARN_CONFIGURATION.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 32);
    YARN_CONFIGURATION.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 4096); // 4096 is the available memory anyways
    YARN_CONFIGURATION.setBoolean(YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME, true);
    YARN_CONFIGURATION.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
    YARN_CONFIGURATION.setInt(YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS, 2);
    YARN_CONFIGURATION.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, 4);
    YARN_CONFIGURATION.setInt(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 3600);
    YARN_CONFIGURATION.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, false);
    YARN_CONFIGURATION.setInt(YarnConfiguration.NM_VCORES, 666); // memory is overwritten in the MiniYARNCluster.
    // so we have to change the number of cores for testing.
    YARN_CONFIGURATION.setInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 20000); // 20 seconds expiry (to ensure we properly heartbeat with YARN).
  }

  protected static YarnConfiguration getYarnConfiguration() {
    return YARN_CONFIGURATION;
  }

  private static boolean isApplicationRunning(ApplicationReport app) {
    final YarnApplicationState yarnApplicationState = app.getYarnApplicationState();
    return yarnApplicationState != YarnApplicationState.FINISHED
        && app.getYarnApplicationState() != YarnApplicationState.KILLED
        && app.getYarnApplicationState() != YarnApplicationState.FAILED;
  }


  public void setupYarnClient() {
    if (yarnClient == null) {
      yarnClient = YarnClient.createYarnClient();
      yarnClient.init(getYarnConfiguration());
      yarnClient.start();
    }

    flinkConfiguration = new org.apache.flink.configuration.Configuration(globalConfiguration);
  }

  public void shutdownYarnClient() {
    yarnClient.stop();
  }

  @Nullable
  protected YarnClient getYarnClient() {
    return yarnClient;
  }

  public static YarnClusterDescriptor createClusterDescriptorWithLogging(
      final String flinkConfDir,
      final Configuration flinkConfiguration,
      final YarnConfiguration yarnConfiguration,
      final YarnClient yarnClient,
      final boolean sharedYarnClient) {
    final Configuration effectiveConfiguration = FlinkYarnSessionCli
        .setLogConfigFileInConfig(flinkConfiguration, flinkConfDir);
    return new YarnClusterDescriptor(effectiveConfiguration, yarnConfiguration, yarnClient, sharedYarnClient);
  }

  @Nonnull
  YarnClusterDescriptor createYarnClusterDescriptor(org.apache.flink.configuration.Configuration flinkConfiguration) {
    final YarnClusterDescriptor yarnClusterDescriptor = YarnTestUtils.createClusterDescriptorWithLogging(
        tempConfPathForSecureRun.getAbsolutePath(),
        flinkConfiguration,
        YARN_CONFIGURATION,
        yarnClient,
        true);
    yarnClusterDescriptor.setLocalJarPath(new Path(flinkUberjar.toURI()));
    yarnClusterDescriptor.addShipFiles(Collections.singletonList(flinkLibFolder));
    return yarnClusterDescriptor;
  }





  /**
   * Locate a file or directory.
   */
  public static File findFile(String startAt, FilenameFilter fnf) {
    File root = new File(startAt);
    String[] files = root.list();
    if (files == null) {
      return null;
    }
    for (String file : files) {
      File f = new File(startAt + File.separator + file);
      if (f.isDirectory()) {
        File r = findFile(f.getAbsolutePath(), fnf);
        if (r != null) {
          return r;
        }
      } else if (fnf.accept(f.getParentFile(), f.getName())) {
        return f;
      }
    }
    return null;
  }

}

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

import java.io.File;
import java.io.FilenameFilter;
import java.nio.file.Paths;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO
 */
public class YarnJobMain {

  private static final Logger LOG = LoggerFactory.getLogger(YarnJobMain.class);

  static {
    System.setProperty("user.home", "/");
    System.setProperty("HADOOP_CONFIG","/FlinkSQL/conf");
  }

  /**
   * Uberjar (fat jar) file of Flink.
   */
  protected static File flinkUberjar;

  /**
   * lib/ folder of the flink distribution.
   */
  protected static File flinkLibFolder;

  protected static File flinkShadedHadoopDir;

  public void submit() {
    // set the home directory to a temp directory. Flink on YARN is using the home dir to distribute the file
    String homeDirPath = System.getProperty("user.home", "/");
    File homeDir = new File(homeDirPath);
    String uberjarStartLoc = "..";
    LOG.info("Trying to locate uberjar in {}", new File(uberjarStartLoc).getAbsolutePath());
    flinkUberjar = findFile(uberjarStartLoc, new RootDirFilenameFilter());
    String flinkDistRootDir = flinkUberjar.getParentFile().getParent();
    flinkLibFolder = flinkUberjar.getParentFile(); // the uberjar is located in lib/
    // the hadoop jar was copied into the target/shaded-hadoop directory during the build
    flinkShadedHadoopDir = Paths.get("target/shaded-hadoop").toFile();
    LOG.info("Flink flinkLibFolder not found", flinkLibFolder);
    LOG.info("lib folder not found", flinkLibFolder.exists());
    LOG.info("lib folder not found", flinkLibFolder.isDirectory());

    if (!flinkUberjar.exists()) {
      LOG.info("Unable to locate yarn-uberjar.jar");
    }


    flinkUberjar = findFile(uberjarStartLoc, new RootDirFilenameFilter());

    File flinkConfDirPath = findFile(flinkDistRootDir, new ContainsName(new String[]{"flink-conf.yaml"}));

    final String confDirPath = flinkConfDirPath.getParentFile().getAbsolutePath();
    GlobalConfiguration.loadConfiguration(confDirPath);



    Configuration flinkConfiguration = new Configuration();
    flinkConfiguration.setString(YarnConfigOptions.APPLICATION_ATTEMPTS, "10");
//    flinkConfiguration.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
//    flinkConfiguration.setString(HighAvailabilityOptions.HA_STORAGE_PATH, storageDir);
//    flinkConfiguration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zkServer.getConnectString());
//    flinkConfiguration.setInteger(HighAvailabilityOptions.ZOOKEEPER_SESSION_TIMEOUT, 1000);

    flinkConfiguration.setString(ConfigConstants.RESTART_STRATEGY, "fixed-delay");
    flinkConfiguration.setInteger(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, Integer.MAX_VALUE);

    final int minMemory = 100;
    flinkConfiguration.setInteger(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_MIN, minMemory);

    YarnConfiguration yarnConfiguration = getYarnConfiguration();
    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(yarnConfiguration);
    yarnClient.start();

    final YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(
        flinkConfiguration,
        yarnConfiguration,
        CliFrontend.getConfigurationDirectoryFromEnv(),
        yarnClient,
        true);
    yarnClusterDescriptor.setLocalJarPath(new Path(flinkUberjar.toURI()));
//    yarnClusterDescriptor.addShipFiles(Collections.singletonList(flinkLibFolder));

  }

  /**
   * Filter to find root dir of the flink-yarn dist.
   */
  public static class RootDirFilenameFilter implements FilenameFilter {
    @Override
    public boolean accept(File dir, String name) {
      return name.startsWith("flink-dist") && name.endsWith(".jar") && dir.toString().contains("/lib");
    }
  }

  private YarnConfiguration getYarnConfiguration() {
    YarnConfiguration yarnConfiguration = new YarnConfiguration();
    yarnConfiguration.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 32);
    yarnConfiguration.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 4096); // 4096 is the available memory anyways
    yarnConfiguration.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
    yarnConfiguration.setBoolean(YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME, true);
    yarnConfiguration.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
    yarnConfiguration.setInt(YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS, 2);
    yarnConfiguration.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, 4);
    yarnConfiguration.setInt(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 3600);
    yarnConfiguration.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, false);
    yarnConfiguration.setInt(YarnConfiguration.NM_VCORES, 666); // memory is overwritten in the MiniYARNCluster.
    // so we have to change the number of cores for testing.
    yarnConfiguration.setInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 20000); // 20 seconds expiry (to ensure we properly heartbeat with YARN).
    return yarnConfiguration;
  }


  /**
   * Locate a file or directory.
   */
  private static File findFile(String startAt, FilenameFilter fnf) {
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

  /**
   * A simple {@link FilenameFilter} that only accepts files if their name contains every string in the array passed
   * to the constructor.
   */
  public static class ContainsName implements FilenameFilter {
    private String[] names;
    private String excludeInPath = null;

    /**
     * @param names which have to be included in the filename.
     */
    public ContainsName(String[] names) {
      this.names = names;
    }

    public ContainsName(String[] names, String excludeInPath) {
      this.names = names;
      this.excludeInPath = excludeInPath;
    }

    @Override
    public boolean accept(File dir, String name) {
      if (excludeInPath == null) {
        for (String n: names) {
          if (!name.contains(n)) {
            return false;
          }
        }
        return true;
      } else {
        for (String n: names) {
          if (!name.contains(n)) {
            return false;
          }
        }
        return !dir.toString().contains(excludeInPath);
      }
    }
  }
}

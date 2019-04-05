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

package ambition.blink.batch;

import java.util.ArrayList;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.PlanExecutor;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.jobgraph.JobGraph;

import java.net.URL;
import java.util.Collection;

@Public
public class BatchLocalEnvironment extends ExecutionEnvironment {
  private Collection<Path> jarFiles;
  private Collection<URL> classPaths;

  private static int defaultLocalDop = Runtime.getRuntime().availableProcessors();

  /** The user-defined configuration for the local execution. */
  private final Configuration configuration;

  /** Create lazily upon first use. */
  private PlanExecutor executor;

  /** In case we keep the executor alive for sessions, this reaper shuts it down eventually.
   * The reaper's finalize method triggers the executor shutdown. */
  @SuppressWarnings("all")
  private ExecutorReaper executorReaper;

  /**
   * Creates a new local environment.
   */
  public BatchLocalEnvironment() {
    this(new Configuration());
  }

  public BatchLocalEnvironment(Collection<Path> jarFiles,
      Collection<URL> classPaths) {
    this(new Configuration());
    this.jarFiles = jarFiles;
    this.classPaths = classPaths;
  }

  /**
   * Creates a new local environment that configures its local executor with the given configuration.
   *
   * @param config The configuration used to configure the local executor.
   */
  public BatchLocalEnvironment(Configuration config) {
    if (!ExecutionEnvironment.areExplicitEnvironmentsAllowed()) {
      throw new InvalidProgramException(
          "The BatchLocalEnvironment cannot be instantiated when running in a pre-defined context " +
              "(such as Command Line Client, Scala Shell, or TestEnvironment)");
    }
    this.configuration = config == null ? new Configuration() : config;
  }

  // --------------------------------------------------------------------------------------------

  @Override
  public JobExecutionResult execute(String jobName) throws Exception {
    if (executor == null) {
      startNewSession();
    }

    Plan p = createProgramPlan(jobName);

    // Session management is disabled, revert this commit to enable
    //p.setJobId(jobID);
    //p.setSessionTimeout(sessionTimeout);

    JobExecutionResult result = executor.executePlan(p);

    this.lastJobExecutionResult = result;
    return result;
  }

  @Override
  public String getExecutionPlan() throws Exception {
    Plan p = createProgramPlan(null, false);

    // make sure that we do not start an executor in any case here.
    // if one runs, fine, of not, we only create the class but disregard immediately afterwards
    if (executor != null) {
      return executor.getOptimizerPlanAsJSON(p);
    }
    else {
      PlanExecutor tempExecutor = PlanExecutor.createLocalExecutor(configuration);
      return tempExecutor.getOptimizerPlanAsJSON(p);
    }
  }

  @Override
  @PublicEvolving
  public void startNewSession() throws Exception {
    if (executor != null) {
      // we need to end the previous session
      executor.stop();
      // create also a new JobID
      jobID = JobID.generate();
    }

    // create a new local executor
    executor = PlanExecutor.createLocalExecutor(configuration);
    executor.setPrintStatusDuringExecution(getConfig().isSysoutLoggingEnabled());

    // if we have a session, start the mini cluster eagerly to have it available across sessions
    if (getSessionTimeout() > 0) {
      executor.start();

      // also install the reaper that will shut it down eventually
      executorReaper = new ExecutorReaper(executor);
    }
  }

  public static BatchLocalEnvironment createBatchLocalEnvironment() {
    return createBatchLocalEnvironment(new Configuration(), defaultLocalDop);
  }

  private static BatchLocalEnvironment createBatchLocalEnvironment(Configuration configuration, int defaultParallelism) {
    final BatchLocalEnvironment batchLocalEnvironment = new BatchLocalEnvironment(configuration);

    if (defaultParallelism > 0) {
      batchLocalEnvironment.setParallelism(defaultParallelism);
    }

    return batchLocalEnvironment;
  }

  public JobGraph getJobGraph(String jobName) {
    OptimizedPlan op = compileProgram(jobName);

    JobGraphGenerator jgg = new JobGraphGenerator();
    JobGraph jobGraph = jgg.compileJobGraph(op);

    if (CollectionUtils.isNotEmpty(jarFiles)) {
      for (Path jarFile: jarFiles) {
        jobGraph.addJar(jarFile);
      }
    }

    if (CollectionUtils.isNotEmpty(classPaths)) {
      jobGraph.setClasspaths(new ArrayList<>(classPaths));
    }

    return jobGraph;
  }


  public Plan getJobPlan(String jobName){
    Plan p = createProgramPlan(jobName);
    return p;
  }

  private OptimizedPlan compileProgram(String jobName) {
    Plan p = createProgramPlan(jobName);

    Optimizer pc = new Optimizer(new DataStatistics(), new Configuration());
    return pc.compile(p);
  }


  // ------------------------------------------------------------------------
  //  Utilities
  // ------------------------------------------------------------------------

  @Override
  public String toString() {
    return "Local Environment (parallelism = " + (getParallelism() == ExecutionConfig.PARALLELISM_DEFAULT ? "default" : getParallelism())
        + ") : " + getIdString();
  }

  // ------------------------------------------------------------------------
  //  Reaping the local executor when in session mode
  // ------------------------------------------------------------------------

  /**
   * This thread shuts down the local executor.
   *
   * <p><b>IMPORTANT:</b> This must be a static inner class to hold no reference to the outer class.
   * Otherwise, the outer class could never become garbage collectible while this thread runs.
   */
  private static class ShutdownThread extends Thread {

    private final Object monitor = new Object();

    private final PlanExecutor executor;

    private volatile boolean triggered = false;

    ShutdownThread(PlanExecutor executor) {
      super("Local cluster reaper");
      setDaemon(true);
      setPriority(Thread.MIN_PRIORITY);

      this.executor = executor;
    }

    @Override
    public void run() {
      synchronized (monitor) {
        while (!triggered) {
          try {
            monitor.wait();
          }
          catch (InterruptedException e) {
            // should never happen
          }
        }
      }

      try {
        executor.stop();
      }
      catch (Throwable t) {
        System.err.println("Cluster reaper caught exception during shutdown");
        t.printStackTrace();
      }
    }

    void trigger() {
      triggered = true;
      synchronized (monitor) {
        monitor.notifyAll();
      }
    }

  }

  /**
   * A class that, upon finalization, shuts down the local mini cluster by triggering the reaper
   * thread.
   */
  private static class ExecutorReaper {

    private final ShutdownThread shutdownThread;

    ExecutorReaper(PlanExecutor executor) {
      this.shutdownThread = new ShutdownThread(executor);
      this.shutdownThread.start();
    }

    @Override
    protected void finalize() throws Throwable {
      super.finalize();
      shutdownThread.trigger();
    }
  }
}

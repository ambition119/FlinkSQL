package pingle.wang.client.job;

import org.apache.flink.api.common.Plan;
import org.apache.flink.runtime.jobgraph.JobGraph;
import pingle.wang.common.job.JobParameter;

import java.util.Map;

/**
 * @Author: wpl
 */
public interface JobClient {

   Plan getJobPlan(JobParameter jobParameter, Map<String,String> extParams) throws Exception;

   JobGraph getJobGraph(JobParameter jobParameter, Map<String,String> extParams) throws Exception;
}

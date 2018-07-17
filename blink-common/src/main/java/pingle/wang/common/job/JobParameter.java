package pingle.wang.common.job;

import java.util.List;
import java.util.Map;

/**
 * @Author: wpl
 */
public class JobParameter {
   private String jobName;
   private Map<String, List<String>> sqls;

   public JobParameter() {
   }

   public JobParameter(String jobName, Map<String, List<String>> sqls) {
      this.jobName = jobName;
      this.sqls = sqls;
   }

   public String getJobName() {
      return jobName;
   }

   public void setJobName(String jobName) {
      this.jobName = jobName;
   }

   public Map<String, List<String>> getSqls() {
      return sqls;
   }

   public void setSqls(Map<String, List<String>> sqls) {
      this.sqls = sqls;
   }
}

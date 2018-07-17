package ambition.client.job;

import java.util.Map;

/**
 * @Author: wpl
 */
public interface FlinkJob {

    /**
     * 封装的Flink job
     * @param sqls sql内容
     * @param props 配置信息
     * @return
     */
    CompilationResult getFlinkJob(String sqls,Map<String,String> props) throws Throwable;
}

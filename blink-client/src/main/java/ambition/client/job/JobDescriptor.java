package ambition.client.job;

import org.apache.flink.table.sources.TableSource;
import ambition.client.table.FlinkTableSink;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @Author: wpl
 */
public class JobDescriptor implements Serializable {
    private  static final long serialVersionUID = -1;
    private  Map<String, String> userDefineFunctions;
    private  Map<String, TableSource> flinkTableSources;
    private  List<FlinkTableSink> flinkTableSinks;
    private  int parallelism;
    private  Map<String, String> extraProps;
    private  Map<String,Map<String,String>> sqls;

    public JobDescriptor() {
    }

    public JobDescriptor(Map<String, String> userDefineFunctions, Map<String, TableSource> flinkTableSources, List<FlinkTableSink> flinkTableSinks, int parallelism, Map<String, String> extraProps, Map<String, Map<String, String>> sqls) {
        this.userDefineFunctions = userDefineFunctions;
        this.flinkTableSources = flinkTableSources;
        this.flinkTableSinks = flinkTableSinks;
        this.parallelism = parallelism;
        this.extraProps = extraProps;
        this.sqls = sqls;
    }

    public Map<String, String> getUserDefineFunctions() {
        return userDefineFunctions;
    }

    public void setUserDefineFunctions(Map<String, String> userDefineFunctions) {
        this.userDefineFunctions = userDefineFunctions;
    }

    public Map<String, TableSource> getFlinkTableSources() {
        return flinkTableSources;
    }

    public void setFlinkTableSources(Map<String, TableSource> flinkTableSources) {
        this.flinkTableSources = flinkTableSources;
    }

    public List<FlinkTableSink> getFlinkTableSinks() {
        return flinkTableSinks;
    }

    public void setFlinkTableSinks(List<FlinkTableSink> flinkTableSinks) {
        this.flinkTableSinks = flinkTableSinks;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public Map<String, String> getExtraProps() {
        return extraProps;
    }

    public void setExtraProps(Map<String, String> extraProps) {
        this.extraProps = extraProps;
    }

    public Map<String, Map<String, String>> getSqls() {
        return sqls;
    }

    public void setSqls(Map<String, Map<String, String>> sqls) {
        this.sqls = sqls;
    }

    byte[] serialize() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream os = new ObjectOutputStream(bos)) {
            os.writeObject(this);
        } catch (IOException e) {
            return null;
        }
        return bos.toByteArray();
    }
}

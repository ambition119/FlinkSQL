package ambition.client.job;

import ambition.client.common.sql.SqlConstant;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ambition.client.table.FlinkTableSink;
import ambition.client.table.FlinkTableSinkProvider;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.*;

/**
 * @Author: wpl
 */
public class JobCompiler {
    private static final Logger logger = LoggerFactory.getLogger(JobCompiler.class);
    private  StreamTableEnvironment env;
    private  JobDescriptor job;

    public JobCompiler() {
    }

    public JobCompiler(StreamTableEnvironment env, JobDescriptor job) {
        this.env = env;
        this.job = job;
    }

    public static void main(String[] args) throws IOException {

        CompilationResult res = null;
        try {
            JobDescriptor job = getJobConf(System.in);
            res = compileJob(job);
        } catch (Throwable e) {
            res = new CompilationResult();
            res.setRemoteThrowable(e);
        }

        try (OutputStream out = chooseOutputStream(args)) {
            out.write(res.serialize());
        }
    }

    public static CompilationResult compileJob(JobDescriptor job) {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment();

        StreamTableEnvironment env = StreamTableEnvironment.getTableEnvironment(execEnv);
        CompilationResult res = new CompilationResult();

        try {
            res.setJobGraph(new JobCompiler(env, job).getJobGraph());
        } catch (IOException e) {
            res.setRemoteThrowable(e);
        }

        return res;
    }

    private static OutputStream chooseOutputStream(String[] args) throws IOException {
        if (args.length > 0) {
            int port = Integer.parseInt(args[0]);
            Socket sock = new Socket();
            sock.connect(new InetSocketAddress(InetAddress.getLocalHost(), port));
            return sock.getOutputStream();
        } else {
            return System.out;
        }
    }

    JobGraph getJobGraph() throws IOException {
        StreamExecutionEnvironment exeEnv = env.execEnv();

        Map<String,String> extraProps = job.getExtraProps();
        for (String key: extraProps.keySet()){
            if (key.startsWith("watermarks_")){
                logger.info("-----------------> " + key);
                exeEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
                extraProps.remove(key);
            }
        }


        //1.设置flink的参数等信息
        Map<String,String> jobProps = new HashMap<>();
        jobProps.putAll(extraProps);
        ParameterTool parameter = ParameterTool.fromMap(jobProps);
        exeEnv.getConfig().setGlobalJobParameters(parameter);

        //2.Source和Sink注册
        this.registerUdfs()
            .registerInputTableSource()
            .registerOutputTableSink();


        //3.sql操作
        Map<String, Map<String, String>> sqls = job.getSqls();

        //3.1含有view的操作
        if (sqls.containsKey(SqlConstant.VIEW)){
            //视图名，对应查询
            Map<String, String> viewMap = sqls.get(SqlConstant.VIEW);
            Set<String> viewNames = viewMap.keySet();
            for (String name:viewNames) {
                String sql = viewMap.get(name);
                Table table = env.sqlQuery(sql);
                logger.info("sql view query  is -----> "+sql);
                env.registerTable(name,table);
            }
        }

        //3.2 insert inot操作
        if (sqls.containsKey(SqlConstant.INSERT_INTO)){
            Map<String, String> updateMap = sqls.get(SqlConstant.INSERT_INTO);
            Collection<String> values = updateMap.values();
            for (String sql:values){
                logger.info("sql insert into  is -----> "+sql);
                env.sqlUpdate(sql);
            }
        }

        StreamGraph streamGraph = exeEnv.getStreamGraph();
        return streamGraph.getJobGraph();
    }

    private JobCompiler registerOutputTableSink() {
        List<FlinkTableSink> tableSinks = job.getFlinkTableSinks();
        if (CollectionUtils.isNotEmpty(tableSinks)){
            for (FlinkTableSink sink:tableSinks) {
                String typeName = sink.getTypeName();
                try {
                    //这里类型判断
                    if(typeName.equalsIgnoreCase(TableSinkType.APPEND.getSinkType())){
                        env.registerTableSink(
                                sink.getTableName(),
                                sink.getSchema().getColumnNames(),
                                sink.getSchema().getTypes(),
                                getAppendOutputTable(sink.getTableSink().getTable(sink.getTableName())));
                    }

                    if(typeName.equalsIgnoreCase(TableSinkType.UPSERT.getSinkType())){
                        env.registerTableSink(
                                sink.getTableName(),
                                sink.getSchema().getColumnNames(),
                                sink.getSchema().getTypes(),
                                getUpsertOutputTable(sink.getTableSink().getTable(sink.getTableName())));
                    }

                    if(typeName.equalsIgnoreCase(TableSinkType.BATCH.getSinkType())){
                        env.registerTableSink(
                                sink.getTableName(),
                                sink.getSchema().getColumnNames(),
                                sink.getSchema().getTypes(),
                                getBatchTableSink(sink.getTableSink().getTable(sink.getTableName())));
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return this;
    }

    private BatchTableSink<Row> getBatchTableSink(
            ExternalCatalogTable output) throws IOException {
        FlinkTableSinkProvider c = TableSinkProviderRegistry.getProvider(output);
        Preconditions.checkNotNull(c, "Cannot find output connectors for " + output);
        return c.getBatchTableSink(output);
    }

    private UpsertStreamTableSink<Row> getUpsertOutputTable(
            ExternalCatalogTable output) throws IOException {
        FlinkTableSinkProvider c = TableSinkProviderRegistry.getProvider(output);
        Preconditions.checkNotNull(c, "Cannot find output connectors for " + output);
        return c.getUpsertStreamTableSink(output);
    }

    private AppendStreamTableSink<Row> getAppendOutputTable(
            ExternalCatalogTable output) throws IOException {
        FlinkTableSinkProvider c = TableSinkProviderRegistry.getProvider(output);
        Preconditions.checkNotNull(c, "Cannot find output connectors for " + output);
        return c.getAppendStreamTableSink(output);
    }

    private JobCompiler registerInputTableSource() {
        for (Map.Entry<String, TableSource> e : job.getFlinkTableSources().entrySet() ){
            logger.info("Registering source table {}", e.getKey());
            env.registerTableSource(e.getKey(),e.getValue());
        }
        return this;
    }

    private JobCompiler registerUdfs() {
        for (Map.Entry<String, String> e : job.getUserDefineFunctions().entrySet()) {
            final String name = e.getKey();
            String clazzName = e.getValue();
            logger.info("udf name = " + clazzName);
            final Object udf;
            try {
                Class<?> clazz = Class.forName(clazzName);
                udf = clazz.newInstance();
            } catch (ClassNotFoundException | IllegalAccessException | InstantiationException ex) {
                throw new IllegalArgumentException("Invalid UDF " + name, ex);
            }

            if (udf instanceof ScalarFunction) {
                env.registerFunction(name, (ScalarFunction) udf);
            } else if (udf instanceof TableFunction) {
                env.registerFunction(name, (TableFunction<?>) udf);
            } else if (udf instanceof AggregateFunction) {
                env.registerFunction(name, (AggregateFunction<?, ?>) udf);
            } else {
                logger.warn("Unknown UDF {} was found.", clazzName);
            }
        }
        return this;
    }

    static JobDescriptor getJobConf(InputStream is) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(is)) {
            return (JobDescriptor) ois.readObject();
        }
    }
}

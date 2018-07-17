package ambition.flink.job;

import com.google.common.base.Joiner;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ambition.client.job.CompilationResult;
import ambition.client.job.FlinkJob;
import ambition.flink.job.impl.FlinkJobImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: wpl
 */
public class FlinkJobTest {
    private String sqlContext;
    private FlinkJob flinkJob;

    @Test
    public void testGetFlinkJob() throws Throwable {
        Map<String, String> map = new HashMap<>();
        CompilationResult flinkJob = this.flinkJob.getFlinkJob(sqlContext, map);
        JobGraph jobGraph = flinkJob.getJobGraph();
        Assert.assertNotNull(jobGraph);
    }


    @Before
    public void init(){
//        CREATE Function解析更改calcite源代码实现，参加[CALCITE-2663]https://issues.apache.org/jira/browse/CALCITE-2663
        String udf =    "CREATE FUNCTION " +
                "demouf " +
                "AS " +
                "'pingle.wang.api.sql.function.DemoUDF' " +
                "USING " +
                "JAR 'hdfs://flink/udf/jedis.jar'," +
                "JAR 'hdfs://flink/udf/customudf.jar';";

        String source = "CREATE TABLE kafka_source (\n" +
              "      department VARCHAR,\n" +
              "      `date` DATE,\n" +
              "      amount float, \n" +
              "      proctime timestamp\n" +
              "      ) \n" +
              "with (\n" +
              "      type=kafka,\n" +
              "      'flink.parallelism'=1,\n" +
              "      'kafka.topic'=topic,\n" +
              "      'kafka.group.id'=flinks,\n" +
              "      'kafka.enable.auto.commit'=true,\n" +
              "      'kafka.bootstrap.servers'='localhost:9092'\n" +
              "); ";

        String sink = "CREATE TABLE mysql_sink (\n" +
              "      `date` DATE,\n" +
              "      amount float, \n" +
              "      PRIMARY KEY (`date`,amount)\n" +
              "      ) \n" +
              "with (\n" +
              "      type=mysql,\n" +
              "      'mysql.connection'='localhost:3306',\n" +
              "      'mysql.db.name'=flink,\n" +
              "      'mysql.batch.size'=0,\n" +
              "      'mysql.table.name'=flink_table,\n" +
              "      'mysql.user'=root,\n" +
              "      'mysql.pass'=root\n" +
              "); ";

        String view = "CREATE VIEW view_select AS \n" +
              "      SELECT `date`, \n" +
              "              amount \n" +
              "      FROM kafka_source \n" +
              "      GROUP BY \n" +
              "            `date`,\n" +
              "            amount\n" +
              "      ; ";


        String result = "INSERT INTO mysql_sink \n" +
              "       SELECT \n" +
              "          `date`, \n" +
              "          sum(amount) \n" +
              "       FROM view_select \n" +
              "       GROUP BY \n" +
              "          `date`\n" +
              "      ;";

        sqlContext =
                Joiner.on("").join(
                        source,sink,view, result
                );

        System.out.println(sqlContext);
        flinkJob = new FlinkJobImpl();
    }
}

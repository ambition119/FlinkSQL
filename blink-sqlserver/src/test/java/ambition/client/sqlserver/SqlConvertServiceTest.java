package ambition.client.sqlserver;

import com.google.common.base.Joiner;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import org.junit.Assert;


/**
 * @Author: wpl
 */
public class SqlConvertServiceTest {
    private String sqlContext;
    private SqlConvertService sqlConvertService;

    @Test
    public void testGetCreateSourceSqlInfo() throws Exception {
        Map<String, String> sourceSqlInfo = sqlConvertService.getCreateSourceSqlInfo(sqlContext);
        System.out.println(sourceSqlInfo);
        Assert.assertEquals(sourceSqlInfo.size(),1);
    }

    @Test
    public void testGetCreateSinkSqlInfo() throws Exception {
        Map<String, String> sinkSqlInfo = sqlConvertService.getCreateSinkSqlInfo(sqlContext);
        Assert.assertEquals(sinkSqlInfo.size(),1);
    }

    @Test
    public void testTransfromSqlClassify() throws Exception {
        Map<String, List<String>> map = sqlConvertService.transfromSqlClassify(sqlContext);
        Assert.assertNotNull(map);
    }

    @Before
    public void init(){
        String udf =    "CREATE FUNCTION " +
                "demouf " +
                "AS " +
                "'pingle.wang.api.sql.function.DemoUDF' " +
                "USING " +
                "JAR 'hdfs://flink/udf/jedis.jar'," +
                "JAR 'hdfs://flink/udf/customudf.jar';";

        String source = "CREATE TABLE kafak_source (" +
                "`date` date, " +
                "amount float, " +
                "date timestamp," +
                "watermark for date AS withOffset(date,1000) " +
                ") " +
                "with (" +
                "type=kafka," +
                "'flink.parallelism'=1," +
                "'kafka.topic'='topic'," +
                "'kafka.group.id'='flinks'," +
                "'kafka.enable.auto.commit'=true," +
                "'kafka.bootstrap.servers'='localhost:9092'" +
                ");";

        String sink = "CREATE TABLE mysql_sink (" +
                "`date` date, " +
                "amount float, " +
                "PRIMARY KEY (`date`,amount)) " +
                "with (" +
                "type=mysql," +
                "'mysql.connection'='localhost:3306'," +
                "'mysql.db.name'=flink," +
                "'mysql.batch.size'=0," +
                "'mysql.table.name'=flink_table," +
                "'mysql.user'=root," +
                "'mysql.pass'=root" +
                ");";

        String view = "create view view_select as  " +
                "SELECT " +
                "`date`, " +
                "amount " +
                "FROM " +
                "kafak_source " +
                "group by `date`,amount;";


        String result = "insert " +
                "into mysql_sink " +
                "SELECT " +
                "`date`, " +
                "sum(amount) " +
                "FROM " +
                "view_select " +
                "group by `date`;";

        sqlContext =
                Joiner.on("").join(
                        udf,source,sink,view, result
                );

        System.out.println(sqlContext);

        sqlConvertService = new SqlConvertServiceImpl();
    }
}

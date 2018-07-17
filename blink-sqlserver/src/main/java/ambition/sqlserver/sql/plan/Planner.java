package ambition.sqlserver.sql.plan;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.sources.TableSource;
import ambition.client.common.sql.SqlConstant;
import ambition.client.job.CompilationResult;
import ambition.client.job.JobCompiler;
import ambition.client.job.JobDescriptor;
import ambition.client.sqlserver.SqlConvertServiceImpl;
import ambition.client.table.FlinkTableSink;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.*;

/**
 * @Author: wpl
 */
public class Planner {
    private Map<String, TableSource> tableSourceMap;
    private List<FlinkTableSink> flinkTableSinks;
    private Map<String, String>  jobProps;

    public Planner( Map<String, TableSource> tableSourceMap, List<FlinkTableSink> flinkTableSinks, Map<String, String> jobProps) {
        this.tableSourceMap = tableSourceMap;
        this.flinkTableSinks = flinkTableSinks;
        this.jobProps = jobProps;
    }

    public CompilationResult sqlPlanner(Map<String,List<String>> funMap,Map<String,Map<String,String>> sqls, int parallelism) throws Throwable {
        Validator validator = new Validator();

        //方法
        if(funMap.containsKey(SqlConstant.FUNCTION)){
            List<String> list = funMap.get(SqlConstant.FUNCTION);
            for (String sql:list) {
                SqlNode stmts = parse(sql);
                validator.validateFunction(stmts);
            }
        }

        //视图
        if(sqls.containsKey(SqlConstant.VIEW)){
            //视图名，对应查询
            Map<String, String> viewMap = sqls.get(SqlConstant.VIEW);
            Collection<String> views =viewMap.values();
            for (String sql:views) {
                SqlNode stmts = parse(sql);
                validator.validateViewQuery(stmts);
            }
        }

        //dml
        if (sqls.containsKey(SqlConstant.INSERT_INTO)) {
            Map<String, String> updateMap = sqls.get(SqlConstant.INSERT_INTO);
            Collection<String> values = updateMap.values();
            for (String sql : values) {
                SqlNode stmts = parse(sql);
                validator.validateDml(stmts);
            }
        }

        JobDescriptor job = new JobDescriptor(
                    validator.userDefinedFunctions(),
                    tableSourceMap,
                    flinkTableSinks,
                    parallelism,
                    jobProps,
                    sqls
                );

        CompilationResult res = JobCompiler.compileJob(job);

        res.setAdditionalJars(validator.additionalResources());
        if (res.getRemoteThrowable() != null) {
            throw res.getRemoteThrowable();
        }
        return res;
    }

    @VisibleForTesting
    public SqlNode parse(String sql) throws Exception {
        InputStream stream  = new ByteArrayInputStream(sql.getBytes());
        SqlConvertServiceImpl service = new SqlConvertServiceImpl();
        SqlDdlParserImpl parserImpl = service.getSqlDdlParserImpl(stream);

        return parserImpl.parseSqlStmtEof();
//        return parserImpl.SqlStmtsEof();
    }
}

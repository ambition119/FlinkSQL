package ambition.flink.job.impl;

import ambition.client.job.CompilationResult;
import ambition.client.job.FlinkJob;
import ambition.client.job.TableCatalogProviderRegistry;
import ambition.client.job.TableSinkType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.TableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ambition.client.common.job.FlinkJobException;
import ambition.client.common.sql.SqlConstant;
import ambition.client.common.sql.SqlInputException;
import ambition.client.common.sql.SqlParserResDescriptor;
import ambition.client.sqlserver.SqlConvertService;
import ambition.client.sqlserver.SqlConvertServiceImpl;
import ambition.client.table.FlinkTableCatalog;
import ambition.client.table.FlinkTableSink;
import ambition.client.table.FlinkTableCatalogProvider;
import ambition.sqlserver.flink.job.FlinkJobConstant;
import ambition.sqlserver.sql.plan.Planner;

import java.util.*;

import static ambition.client.common.sql.SqlConstant.INDEXE;
import static ambition.sqlserver.flink.job.FlinkJobConstant.JOB_FLAG;

/**
 * @Author: wpl
 */
public class FlinkJobImpl implements FlinkJob {
    private static final Logger logger = LoggerFactory.getLogger(FlinkJobImpl.class);

    private Map<String, TableSource> sources = new LinkedHashMap<>();
    private List<FlinkTableSink> flinkTableSinks = new LinkedList<>();
    private Map<String, Map<String,String>> sqls = new LinkedHashMap<>();

    private Map<String, List<String>> userSqls;
    private Map<String, String> sqlInfoMap;
    private Map<String,List<String>> funMap;

    private Map<String, String> jobCommonProps = new HashMap<>();

    private SqlConvertService sqlConvertService = new SqlConvertServiceImpl();

    @Override
    public CompilationResult getFlinkJob(String sqlContext, Map<String, String> jobProps) throws Throwable {

        if (StringUtils.isBlank(sqlContext)){
            logger.warn("sql is null");
            throw new SqlInputException("sql not null   ");
        }

        //source和sink
        this.getUserSqls(sqlContext)
            .getTableSources(sqlContext)
            .getTableSinks(sqlContext)
            .getFunctionSqls()
            .getExecuteSqls();

        if (null != jobProps){
            jobCommonProps.putAll(jobProps);
        }


        CompilationResult result = null;
        Planner planner = new Planner(sources, flinkTableSinks, jobCommonProps);

        if (jobCommonProps.containsKey(FlinkJobConstant.PARALLELISM)){
            result = planner.sqlPlanner(funMap,
                    sqls,
                    Integer.valueOf(jobCommonProps.getOrDefault(FlinkJobConstant.PARALLELISM, "2")));
        }else {
            result = planner.sqlPlanner(funMap,
                    sqls,
                    Runtime.getRuntime().availableProcessors());
        }

        //设置flink的任务信息
        result.setName("Flink Job");
        result.setMemorysize(1024);
        result.setNumSlots(2);
        result.setNumContainers(3);
        result.setQueue("default");

        return result;
    }

    private FlinkJobImpl getFunctionSqls(){
        funMap = new LinkedHashMap<>();
        List<String> funtionSqls = userSqls.get(SqlConstant.FUNCTION);
        if (CollectionUtils.isNotEmpty(funtionSqls)){
            funMap.put(SqlConstant.FUNCTION,funtionSqls);
        }
        return this;
    }

    private FlinkJobImpl getExecuteSqls() throws Exception {
        //视图sql
        Map<String,String> viewMap = new LinkedHashMap<>();
        List<String> viewSqls = userSqls.get(SqlConstant.VIEW);
        if (CollectionUtils.isNotEmpty(viewSqls)){
            for (String sql : viewSqls){
                SqlParserResDescriptor viewInfo = sqlConvertService.sqlViewParser(sql);
                String tableName = viewInfo.getTableName();
                String selectBody = viewInfo.getSqlInfo();
                viewMap.put(tableName,selectBody);
            }
            sqls.put(SqlConstant.VIEW,viewMap);
        }

        //insert into
        Map<String,String> queryMap = new LinkedHashMap<>();
        List<String> querys = userSqls.get(SqlConstant.INSERT_INTO);
            if (CollectionUtils.isNotEmpty(querys)){
            for (String sql : querys){
                SqlParserResDescriptor viewInfo = sqlConvertService.sqlInsertInotParser(sql);
                String tableName = viewInfo.getTableName();
                String selectBody = viewInfo.getSqlInfo();
                queryMap.put(tableName,selectBody);
            }
            sqls.put(SqlConstant.INSERT_INTO,queryMap);
        }

        return this;
    }



    private FlinkJobImpl getTableSources(String sqlContext) throws Exception{
        sqlInfoMap = sqlConvertService.getCreateSourceSqlInfo(sqlContext);

        if (null == sqlInfoMap){
            logger.warn("source is null");
            throw new FlinkJobException("source is null,must not null  ");
        }

        Set<String> sourceNames = sqlInfoMap.keySet();
        for (String name : sourceNames) {
            String ddlSql = sqlInfoMap.get(name);
            SqlParserResDescriptor sqlDdlParser = sqlConvertService.sqlDdlParser(ddlSql);

            String sourceType = sqlDdlParser.getSourceType();
            FlinkTableCatalogProvider sourceProvider = TableCatalogProviderRegistry.getProvider(sourceType);

            Map<String, TypeInformation<?>> schemas = sqlDdlParser.getSchemas();
            TableSchema inputSchema = new TableSchema(
                    schemas.keySet().toArray(new String[schemas.size()]),
                    schemas.values().toArray(new TypeInformation<?>[schemas.size()])
            );

            //job的参数信息内容
            Map<String, String> parms = sqlDdlParser.getParms();

            Set<String> keySet = parms.keySet();
            //1.flink开头的配置信息
            for (String key : keySet) {
                if (key.startsWith(JOB_FLAG)){
                    jobCommonProps.put(key,parms.get(key));
                }
            }

            Map<String, String> watermarks = sqlDdlParser.getWatermarks();
            if (CollectionUtils.isNotEmpty(watermarks.keySet())){
                for (String  key : watermarks.keySet()){
                    parms.put("watermarks_"+key,watermarks.get(key));
                    jobCommonProps.put("watermarks_"+key,watermarks.get(key));
                }
            }

            //TODO: 2source创建需要的配置信息，如kafka的topic等信息,目前先不管，直接传递下去
            TableSource tableSource = sourceProvider.getInputTableSource(parms, inputSchema);

            String tableName = sqlDdlParser.getTableName();
            sources.put(tableName,tableSource);
        }

        return this;
    }

    private FlinkJobImpl getTableSinks(String sqlContext) throws Exception{
        sqlInfoMap = sqlConvertService.getCreateSinkSqlInfo(sqlContext);

        if (null == sqlInfoMap){
            logger.warn("sink is null");
            throw new FlinkJobException("sink is null,must not null  ");
        }

        Set<String>  sinkNames = sqlInfoMap.keySet();
        for (String name : sinkNames) {
            String ddlSql = sqlInfoMap.get(name);
            SqlParserResDescriptor sqlDdlParser = sqlConvertService.sqlDdlParser(ddlSql);

            String sinkType = sqlDdlParser.getSourceType();
            FlinkTableCatalogProvider sinkProvider = TableCatalogProviderRegistry.getProvider(sinkType);


            Map<String, TypeInformation<?>> schemas = sqlDdlParser.getSchemas();
            TableSchema outputSchema = new TableSchema(
                    schemas.keySet().toArray(new String[schemas.size()]),
                    schemas.values().toArray(new TypeInformation<?>[schemas.size()])
            );

            Map<String, String> parms = sqlDdlParser.getParms();

            Set<String> keySet = parms.keySet();
            //1.flink开头的配置信息
            for (String key : keySet) {
                if (key.startsWith(JOB_FLAG)){
                    jobCommonProps.put(key,parms.get(key));
                }
            }
            //TODO: 2.sink创建需要的配置信息，如mysql的服务器，用户名和密码等，先不处理，传递下去
            FlinkTableCatalog tableSink = sinkProvider.getOutputCatalog(parms, outputSchema);

            String tableName = sqlDdlParser.getTableName();

            String typeName = null;
            if (parms.containsKey(INDEXE)){
                typeName= TableSinkType.UPSERT.name();
            }

            FlinkTableSink sink = new FlinkTableSink(outputSchema, tableName, tableSink, sinkType, typeName);
            flinkTableSinks.add(sink);
        }

        return this;
    }


    private FlinkJobImpl getUserSqls(String sqlContext) throws Exception {
        userSqls = sqlConvertService.transfromSqlClassify(sqlContext);
        return this;
    }

}

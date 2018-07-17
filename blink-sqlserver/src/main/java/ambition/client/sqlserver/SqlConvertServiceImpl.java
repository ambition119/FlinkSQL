package ambition.client.sqlserver;

import ambition.client.common.sql.SqlConstant;
import ambition.client.common.sql.SqlInputException;
import ambition.client.common.sql.SqlParserResDescriptor;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ambition.sqlserver.sql.parser.InputSqlPattern;
import ambition.sqlserver.sql.parser.TableInfo;
import ambition.sqlserver.sql.parser.TableInfoParser;
import ambition.util.SqlUtil;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.*;
import java.util.regex.Matcher;

import static org.apache.calcite.sql.parser.SqlParser.DEFAULT_IDENTIFIER_MAX_LENGTH;


/**
 * @Author: wpl
 */
public class SqlConvertServiceImpl implements SqlConvertService {
    private static final Logger logger = LoggerFactory.getLogger(SqlConvertServiceImpl.class);

    private final String type ="type";
    private final String flag = "=";

    public SqlConvertServiceImpl() {

    }

    @Override
    public Map<String,String> getCreateSourceSqlInfo(String sqlContext) throws Exception {
        if (StringUtils.isBlank(sqlContext)){
            logger.warn("sql is null");
            throw new SqlInputException("sql not null   ");
        }

        Map<String, String> result = new LinkedHashMap<>();

        Map<String, List<String>> listMap = transfromSqlClassify(sqlContext);
        Map<String, String> ddlSqls = getDdlSqls(listMap);
        Set<String> tableNames = ddlSqls.keySet();

        //通过Sink来获取Source
//        TODO: 优化点
        Map<String, String> sinkSqlInfo = getCreateSinkSqlInfo(sqlContext);
        Set<String> sinkNames = sinkSqlInfo.keySet();

        if (CollectionUtils.isNotEmpty(sinkNames)){
            tableNames.removeAll(sinkNames);
        }

        for (String tableName : tableNames) {
            String ddlSql = ddlSqls.get(tableName);
            result.put(tableName, ddlSql);
        }

        return result;
    }

    @Override
    public Map<String,String> getCreateSinkSqlInfo(String sqlContext) throws Exception {
        if (StringUtils.isBlank(sqlContext)){
            logger.warn("sql is null");
            throw new SqlInputException("sql not null   ");
        }

        Map<String, String> result = new LinkedHashMap<>();
        Map<String, List<String>> listMap = transfromSqlClassify(sqlContext);

        List<String> sinkNames = new LinkedList<>();
        //通过insert into判断sink
        List<String> lists = listMap.get(SqlConstant.INSERT_INTO);
        if (CollectionUtils.isNotEmpty(lists)){
            for (String str : lists){
                SqlParserResDescriptor insertInotParser = sqlInsertInotParser(str);
                sinkNames.add(insertInotParser.getTableName());
            }
        }

        Map<String, String> ddlSqls = getDdlSqls(listMap);
        for (String sinkName : sinkNames) {
            String ddlSql = ddlSqls.get(sinkName);
            result.put(sinkName,ddlSql);
        }

        return result;
    }

    @Override
    public SqlParserResDescriptor sqlDdlParser(String ddlSql) throws Exception {
        if (StringUtils.isBlank(ddlSql)){
            throw new SqlInputException("sql not null   ");
        }

        SqlParserResDescriptor sqlParserResDescriptor = new SqlParserResDescriptor();
        Map<String,String> parms = new LinkedHashMap<>();
        Map<String,TypeInformation<?>> schemas = new LinkedHashMap<>();

        TableInfo tableInfo = TableInfoParser.parser(ddlSql);
        //with解析
        parms.putAll(tableInfo.getProps());
        logger.info("the with properties are : "+ parms.toString());

        String primaryKey = tableInfo.getPrimarys();
        if(StringUtils.isNotBlank(primaryKey)){
            logger.info("primary key is : "+ primaryKey);
            parms.put(SqlConstant.INDEXE,primaryKey);
        }

        String sourceType = parms.get("type");
        sqlParserResDescriptor.setSourceType(sourceType);
        logger.info("parser source type is : "+sourceType);


        Map<String, String> columnInfos = tableInfo.getSchema();
        for (String columnName: columnInfos.keySet()){
            String columnType = columnInfos.get(columnName).toLowerCase();
            schemas.put(columnName.trim().replace("`",""),getColumnType(columnType));
        }


        sqlParserResDescriptor.setTableName(tableInfo.getTableName());

        sqlParserResDescriptor.setSqlInfo(ddlSql);

        sqlParserResDescriptor.setSchemas(schemas);
        sqlParserResDescriptor.setParms(parms);

        sqlParserResDescriptor.setVirtuals(tableInfo.getVirtuals());
        sqlParserResDescriptor.setWatermarks(tableInfo.getWatermarks());

        if (parms.containsKey(type)){
            sqlParserResDescriptor.setSourceType(parms.get(type));
        }

        return sqlParserResDescriptor;
    }

    @Override
    public SqlParserResDescriptor sqlViewParser(String viewSql) throws Exception {
        if (null == viewSql){
            throw new SqlInputException("sql not null   ");
        }
        SqlParserResDescriptor sqlParserResDescriptor = new SqlParserResDescriptor();

        SqlNode sqlNode = parseDdl(viewSql);
        if (sqlNode instanceof SqlCreateView){
            SqlCreateView node = (SqlCreateView) sqlNode;

            Field fieldName = node.getClass().getDeclaredField("name");
            fieldName.setAccessible(true);
            sqlParserResDescriptor.setTableName(fieldName.get(node).toString());

            Field fieldQuery = node.getClass().getDeclaredField("query");
            fieldQuery.setAccessible(true);
            sqlParserResDescriptor.setSqlInfo(fieldQuery.get(node).toString());
        }

        return sqlParserResDescriptor;
    }

    @Override
    public SqlParserResDescriptor sqlInsertInotParser(String insertSql) throws Exception {
        if (null == insertSql){
            throw new SqlInputException("sql not null   ");
        }

        SqlParserResDescriptor sqlParserResDescriptor = new SqlParserResDescriptor();

        SqlNode sqlNode = parseQueryOrDml(insertSql);
        if (sqlNode instanceof SqlInsert){
            SqlInsert node = (SqlInsert) sqlNode;
            sqlParserResDescriptor.setTableName(node.getTargetTable().toString());
            sqlParserResDescriptor.setSqlInfo(node.toString());
        }

        return sqlParserResDescriptor;
    }

    @Override
    public Map<String, List<String>> transfromSqlClassify(String sqlContext) throws Exception {
        if (StringUtils.isBlank(sqlContext)){
            logger.warn("sql is null");
            throw new SqlInputException("sql not null   ");
        }

        logger.warn("user input sql is : "+sqlContext);

        //sql拆解
        List<String> sqls = splitSemiColon(sqlContext);
        if (CollectionUtils.isEmpty(sqls)){
            logger.warn("sqls is null");
            throw new SqlInputException("sqls is null");
        }

        Map<String,List<String>> result = new HashMap<>();
        //sql顺序的一致
        List<String> funList = new LinkedList<>();
        List<String> tableList = new LinkedList<>();
        List<String> viewList = new LinkedList<>();
        List<String> insertList = new LinkedList<>();

        for (String sql: sqls) {
            if (StringUtils.isNotBlank(sql)){
                //替换多余的空格
                String newSql = sql.trim().replaceAll(" +", " ").replaceAll("\\s+", " ");

                if (newSql.toUpperCase().startsWith(SqlConstant.CREATE)){
                    if (newSql.toUpperCase().contains(SqlConstant.TABLE)){
                        tableList.add(newSql+ SqlConstant.SQL_END_FLAG);
                    }

                    Matcher funMatcher = InputSqlPattern.CREATE_FUN_PATTERN.matcher(SqlUtil.getReplaceSql(newSql));
                    if(funMatcher.find()){
                        funList.add(newSql+ SqlConstant.SQL_END_FLAG);
                    }

                    Matcher viewMatcher = InputSqlPattern.CREATE_VIEW_PATTERN.matcher(SqlUtil.getReplaceSql(newSql));
                    if(viewMatcher.find()){
                        viewList.add(newSql);
                    }
                }else {
                    insertList.add(newSql+ SqlConstant.SQL_END_FLAG);
                }

            }
        }

        if (CollectionUtils.isNotEmpty(funList)){
            result.put(SqlConstant.FUNCTION,funList);
        }

        if (CollectionUtils.isNotEmpty(insertList)){
            result.put(SqlConstant.INSERT_INTO,insertList);
        }

        if (CollectionUtils.isNotEmpty(viewList)){
            result.put(SqlConstant.VIEW,viewList);
        }

        if (CollectionUtils.isNotEmpty(tableList)){
            result.put(SqlConstant.TABLE,tableList);
        }

        return result;
    }

    private Map<String,String> getDdlSqls(Map<String, List<String>> listMap) throws Exception {
        Map<String, String> tableMap = new HashMap<>();
        List<String> tables = listMap.get(SqlConstant.TABLE);
        if (CollectionUtils.isNotEmpty(tables)){
            for (String str : tables){
                SqlParserResDescriptor sqlDdlParser = sqlDdlParser(str);
                tableMap.put(sqlDdlParser.getTableName(),str);
            }
        }
        return tableMap;
    }

    private List<String> splitSemiColon(String sqlContext) {
        boolean inQuotes = false;
        boolean escape = false;

        List<String> ret = new ArrayList<>();

        char quoteChar = '"';
        int beginIndex = 0;
        for (int index = 0; index < sqlContext.length(); index++) {
            char c = sqlContext.charAt(index);
            switch (c) {
                case ';':
                    if (!inQuotes) {
                        ret.add(sqlContext.substring(beginIndex, index));
                        beginIndex = index + 1;
                    }
                    break;
                case '"':
                case '\'':
                    if (!escape) {
                        if (!inQuotes) {
                            quoteChar = c;
                            inQuotes = !inQuotes;
                        } else {
                            if (c == quoteChar) {
                                inQuotes = !inQuotes;
                            }
                        }
                    }
                    break;
                default:
                    break;
            }

            if (escape) {
                escape = false;
            } else if (c == '\\') {
                escape = true;
            }
        }

        if (beginIndex < sqlContext.length()) {
            ret.add(sqlContext.substring(beginIndex));
        }

        return ret;
    }


    /**
     * 解析ddl中的sql语法
     * create Function解析更改calcite源代码实现，参加[CALCITE-2663]https://issues.apache.org/jira/browse/CALCITE-2663
     * @param sql
     * @return
     * @throws Exception
     */
    public SqlNode parseDdl(String sql) throws Exception {
        // Keep the SQL syntax consistent with Flink
        sql = sql.replaceAll("\n","");

        logger.info("----->" +sql);

        InputStream stream  = new ByteArrayInputStream(sql.getBytes());
        SqlDdlParserImpl impl = getSqlDdlParserImpl(stream);
        return impl.parseSqlStmtEof();
    }

    /**
     * 解析dml中的sql语法
     * @param sql
     * @return
     * @throws Exception
     */
    public SqlNode parseQueryOrDml(String sql) throws Exception {
        sql = sql.replaceAll("\n","");
        InputStream stream  = new ByteArrayInputStream(sql.getBytes());
        SqlDdlParserImpl impl = getSqlDdlParserImpl(stream);
        return impl.SqlQueryOrDml();
    }

    public SqlDdlParserImpl getSqlDdlParserImpl(InputStream stream){
        SqlDdlParserImpl impl = new SqlDdlParserImpl(stream);
        impl.switchTo("BTID");
        impl.setTabSize(1);
        impl.setQuotedCasing(Lex.JAVA.quotedCasing);
        impl.setUnquotedCasing(Lex.JAVA.unquotedCasing);
        impl.setIdentifierMaxLength(DEFAULT_IDENTIFIER_MAX_LENGTH);
        return impl;
    }



    private TypeInformation<?> getColumnType(String type){
        TypeInformation<?> basicTypeInfo = null ;
        switch (type) {
            case "char":
                basicTypeInfo=BasicTypeInfo.CHAR_TYPE_INFO;
                break;
            case "varchar":
                basicTypeInfo=BasicTypeInfo.STRING_TYPE_INFO;
                break;
            case "boolean":
                basicTypeInfo=BasicTypeInfo.BOOLEAN_TYPE_INFO;
                break;
            case "int":
                basicTypeInfo=BasicTypeInfo.INT_TYPE_INFO;
                break;
            case "bigint":
                basicTypeInfo=BasicTypeInfo.LONG_TYPE_INFO;
                break;
            case "float":
                basicTypeInfo=BasicTypeInfo.FLOAT_TYPE_INFO;
                break;
            case "decimal":
                basicTypeInfo=BasicTypeInfo.BIG_DEC_TYPE_INFO;
                break;
            case "double":
                basicTypeInfo=BasicTypeInfo.DOUBLE_TYPE_INFO;
                break;
            case "byte":
                basicTypeInfo=BasicTypeInfo.BYTE_TYPE_INFO;
                break;
            case "short":
                basicTypeInfo=BasicTypeInfo.SHORT_TYPE_INFO;
                break;
            case "date":
                basicTypeInfo=SqlTimeTypeInfo.DATE;
                break;
            case "time":
                basicTypeInfo=SqlTimeTypeInfo.TIME;
                break;
            case "timestamp":
                basicTypeInfo=SqlTimeTypeInfo.TIMESTAMP;
                break;
            default:
                break;
        }
        return basicTypeInfo;
    }
}

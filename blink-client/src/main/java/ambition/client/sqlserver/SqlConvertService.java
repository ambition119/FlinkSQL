package ambition.client.sqlserver;

import ambition.client.common.sql.SqlParserResDescriptor;

import java.util.List;
import java.util.Map;

/**
 * @Author: wpl
 */
public interface SqlConvertService {
    /**
     * @param sqlContext    sql语句集合
     * @return sql语句分类对应的集合
     * @throws Exception
     */
    Map<String,List<String>> transfromSqlClassify(String sqlContext) throws Exception;

    /**
     * ddl语句解析
     * @param ddlSql
     * @return
     * @throws Exception
     */
    SqlParserResDescriptor sqlDdlParser(String ddlSql) throws Exception;

    /**
     * sql中视图sql解析
     * @param viewSql
     * @return sql的封装信息类
     * @throws Exception
     */
    SqlParserResDescriptor sqlViewParser(String viewSql) throws Exception;

    /**
     * sql中的insert into内容
     * @param insertSql
     * @return
     * @throws Exception
     */
    SqlParserResDescriptor sqlInsertInotParser(String insertSql) throws Exception;


    /**
     * 将用户输入的sql中获取封装source的信息
     * @return
     * @throws Exception
     */
    Map<String,String> getCreateSourceSqlInfo(String sqlContext) throws Exception;

    /**
     * 将用户输入的sql中获取封装source的信息
     * @return
     * @throws Exception
     */
    Map<String,String> getCreateSinkSqlInfo(String sqlContext) throws Exception;
}

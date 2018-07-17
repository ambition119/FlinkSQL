package pingle.wang.client.sql;

import pingle.wang.common.table.TableInfo;

import java.util.List;
import java.util.Map;

/**
 * @Author: wpl
 */
public interface SqlService {
   /**
    * @param sqlContext    sql语句集合
    * @return sql语句分类对应的集合
    * @throws Exception
    */
   Map<String,List<String>> sqlConvert(String sqlContext) throws Exception;

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

   /**
    * ddl语句解析
    * @param ddlSql
    * @return
    * @throws Exception
    */
   TableInfo sqlDdlParser(String ddlSql) throws Exception;

   /**
    * sql中视图sql解析
    * @param viewSql
    * @return sql的封装信息类
    * @throws Exception
    */
   TableInfo sqlViewParser(String viewSql) throws Exception;

   /**
    * sql中的insert into内容
    * @param insertSql
    * @return
    * @throws Exception
    */
   TableInfo sqlDmlParser(String insertSql) throws Exception;
}

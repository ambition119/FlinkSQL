/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ambition.blink.sql;

import ambition.blink.common.table.FunctionInfo;
import ambition.blink.common.table.TableInfo;
import ambition.blink.common.table.ViewOrInsertInfo;

import java.util.List;
import java.util.Map;

public interface SqlService {
   /**
    * @param sqlContext    sql语句集合
    * @return sql语句分类对应的集合
    * @throws Exception
    */
   Map<String,List<String>> sqlConvert(String sqlContext) throws Exception;

   /**
    * function语句解析
    * @param funSql
    * @return
    * @throws Exception
    */
   FunctionInfo sqlFunctionParser(String funSql) throws Exception;

   /**
    * ddl语句解析
    * @param ddlSql
    * @return
    * @throws Exception
    */
   TableInfo sqlTableParser(String ddlSql) throws Exception;

   /**
    * sql中视图sql解析
    * @param viewSql
    * @return sql的封装信息类
    * @throws Exception
    */
   ViewOrInsertInfo sqlViewParser(String viewSql) throws Exception;

   /**
    * sql中的insert into内容
    * @param insertSql
    * @return
    * @throws Exception
    */
   ViewOrInsertInfo sqlDmlParser(String insertSql) throws Exception;
}

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

import java.util.List;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.dml.RichSqlInsert;

/**
 * @Date: 2020/1/2
 */
public interface SqlParserService {
  /**
   * function语句解析
   * @param funSql
   * @return
   * @throws Exception
   */
  List<SqlCreateFunction> sqlFunctionParser(String funSql) throws Exception;

  /**
   * ddl语句解析
   * @param ddlSql
   * @return
   * @throws Exception
   */
  List<SqlCreateTable> sqlTableParser(String ddlSql) throws Exception;

  /**
   * sql中视图sql解析
   * @param viewSql
   * @return sql的封装信息类
   * @throws Exception
   */
  List<SqlCreateView> sqlViewParser(String viewSql) throws Exception;

  /**
   * sql中的insert into内容
   * @param insertSql
   * @return
   * @throws Exception
   */
  List<RichSqlInsert> sqlDmlParser(String insertSql) throws Exception;
}

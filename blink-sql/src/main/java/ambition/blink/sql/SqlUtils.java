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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;

/**
 * @Date: 2020/1/2
 */
public class SqlUtils {

  public static SqlNodeList parseSql(String sql) throws Exception {
    InputStream stream  = new ByteArrayInputStream(sql.getBytes());
    SqlParser sqlParser = getSqlParser(stream);

    // 仅限于解析 query 的内容，比如解析ddl会报 Encountered "<EOF>"
    // SqlNode sqlNode = sqlParser.parseStmt();
    // 解析的范围广
    SqlNodeList sqlNodeList = sqlParser.parseStmtList();
    return sqlNodeList;
  }

  public static SqlParser getSqlParser(InputStream stream) {
    Reader source = new InputStreamReader(stream,Charset.defaultCharset());

    // SqlConformance.HIVE; 如果有分区，则使用该sql标准

    return SqlParser.create(source,
        SqlParser.configBuilder()
            .setParserFactory(FlinkSqlParserImpl.FACTORY)
            .setQuoting(Quoting.BACK_TICK)
            .setUnquotedCasing(Casing.UNCHANGED)
            .setQuotedCasing(Casing.UNCHANGED)
            .setConformance(FlinkSqlConformance.HIVE)
            .build());
  }

}

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

package ambition.blink.sql.parser.ddl;

import static org.apache.calcite.sql.parser.SqlParser.DEFAULT_IDENTIFIER_MAX_LENGTH;

import ambition.blink.sql.parser.impl.BlinkSqlParserImpl;
import java.io.StringReader;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.junit.Test;

public class SqlDdlTest {

  @Test
  public void testCreateTable() throws Exception {
    String ddl = "CREATE TABLE tbl1 (\n" +
        "  a bigint,\n" +
        "  h varchar, \n" +
        "  g as 2 * (a + 1), \n" +
        "  ts timestamp, \n" +
        "  b varchar,\n" +
        "  proc as PROCTIME(), \n" +
        "  PRIMARY KEY (a, b)\n" +
        ")\n" +
        "PARTITIONED BY (a, h)\n" +
        "WITH (\n" +
        "    connector = 'kafka', \n" +
        "    kafka.topic = 'log.test'\n" +
        ")\n";
    StringReader in = new StringReader(ddl);
    BlinkSqlParserImpl impl = new BlinkSqlParserImpl(in);
    impl.switchTo("BTID");
    impl.setTabSize(1);
    impl.setQuotedCasing(Lex.JAVA.quotedCasing);
    impl.setUnquotedCasing(Lex.JAVA.unquotedCasing);
    impl.setIdentifierMaxLength(DEFAULT_IDENTIFIER_MAX_LENGTH);

    SqlNode node = impl.parseSqlStmtEof();
    if (node instanceof SqlCreateTable) {
      SqlCreateTable createTable = (SqlCreateTable) node;
      createTable.getColumnList();
      createTable.getPropertyList();
    }
  }

  @Test
  public void testCreateFunction() throws Exception {
    String ddl = "CREATE FUNCTION demouf AS 'ambition.api.sql.function.DemoUDF' USING JAR 'hdfs://flink/udf/jedis.jar, hdfs://flink/udf/customudf.jar'";

    StringReader in = new StringReader(ddl);
    BlinkSqlParserImpl impl = new BlinkSqlParserImpl(in);
    impl.switchTo("BTID");
    impl.setTabSize(1);
    impl.setQuotedCasing(Lex.JAVA.quotedCasing);
    impl.setUnquotedCasing(Lex.JAVA.unquotedCasing);
    impl.setIdentifierMaxLength(DEFAULT_IDENTIFIER_MAX_LENGTH);

    SqlNode node = impl.parseSqlStmtEof();
    if (node instanceof SqlCreateFunction) {
      SqlCreateFunction createFunction = (SqlCreateFunction) node;
      createFunction.getName();
      createFunction.getClassName();
      createFunction.getUsingList();
    }
  }
}

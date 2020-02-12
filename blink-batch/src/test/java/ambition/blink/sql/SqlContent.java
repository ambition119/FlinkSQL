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

public interface SqlContent {
  String sqls =
      "CREATE TABLE csv_source (" +
      "id int, " +
      "name varchar, " +
      "`date` date , " +
      "age int" +
      ") " +
      "with (" +
      "type=source," +
      "connect.type=json," +
      "'file.path'='file:///FlinkSQL/blink-job/src/test/resources/demo.json'" +
      ");" +

      "CREATE TABLE csv_sink (" +
      "`date` date, " +
      "age int, " +
      "PRIMARY KEY (`date`)) " +
      "with (" +
      "type=sink," +
      "connect.type=csv," +
      "'file.path'='file:///FlinkSQL/blink-job/src/test/resources/demo_out.csv'" +
      ");" +

      "create view view_select as  " +
      "SELECT " +
      "`date`, " +
      "age " +
      "FROM " +
      "csv_source " +
      "group by `date`,age;" +

      "insert " +
      "into csv_sink " +
      "SELECT " +
      "`date`, " +
      "sum(age) " +
      "FROM " +
      "view_select " +
      "group by `date`;";
}

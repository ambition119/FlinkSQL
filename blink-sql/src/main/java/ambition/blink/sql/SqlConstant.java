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

public interface SqlConstant {
  String CREATE_TABLE="(?i)^CREATE\\s+TABLE";

  String CREATE_TMP_FUNCTION="(?i)^CREATE\\s+TEMPORARY\\s+FUNCTION";
  String CREATE_FUNCTION="(?i)^CREATE\\s+FUNCTION";

  String CREATE_VIEW="(?i)^CREATE\\s+VIEW";

  String INSERT_INTO="(?i)^INSERT\\s+INTO";

  String FUNCTION="FUNCTION";
  String TABLE ="TABLE";
  String VIEW ="VIEW";
  String INSERT ="INSERT";
  String SQL_END_FLAG=";";
}

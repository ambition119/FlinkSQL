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

package ambition.blink.common.table;

import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class TableInfo {
   private String tableName;
   private TableType tableType;
   private String primaryKeys;

   //字段和类型
   private Map<String,String> inputSchema;

   //kv键值对，props信息
   private Map<String,String> props;
   private Map<String,String> virtuals ;
   private Map<String,String> watermarks;

   //字段和类型
   private Map<String,TypeInformation<?>> flinkUseSchema;

   public TableInfo() {
   }

   public String getTableName() {
      return tableName;
   }

   public void setTableName(String tableName) {
      this.tableName = tableName;
   }

   public TableType getTableType() {
      return tableType;
   }

   public void setTableType(TableType tableType) {
      this.tableType = tableType;
   }

   public String getPrimaryKeys() {
      return primaryKeys;
   }

   public void setPrimaryKeys(String primaryKeys) {
      this.primaryKeys = primaryKeys;
   }

   public Map<String, String> getInputSchema() {
      return inputSchema;
   }

   public void setInputSchema(Map<String, String> inputSchema) {
      this.inputSchema = inputSchema;
   }

   public Map<String, String> getProps() {
      return props;
   }

   public void setProps(Map<String, String> props) {
      this.props = props;
   }

   public Map<String, String> getVirtuals() {
      return virtuals;
   }

   public void setVirtuals(Map<String, String> virtuals) {
      this.virtuals = virtuals;
   }

   public Map<String, String> getWatermarks() {
      return watermarks;
   }

   public void setWatermarks(Map<String, String> watermarks) {
      this.watermarks = watermarks;
   }

   public Map<String, TypeInformation<?>> getFlinkUseSchema() {
      return flinkUseSchema;
   }

   public void setFlinkUseSchema(Map<String, TypeInformation<?>> flinkUseSchema) {
      this.flinkUseSchema = flinkUseSchema;
   }
}

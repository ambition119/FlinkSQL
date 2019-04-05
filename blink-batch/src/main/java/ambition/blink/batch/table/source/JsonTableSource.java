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

package ambition.blink.batch.table.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

public class JsonTableSource implements BatchTableSource<Row>, ProjectableTableSource<Row> {

  private String filePath;
  private String[] fieldNames;
  private TypeInformation[] fieldTypes;

  public JsonTableSource(String filePath, String[] fieldNames,
      TypeInformation[] fieldTypes) {
    this.filePath = filePath;
    this.fieldNames = fieldNames;
    this.fieldTypes = fieldTypes;
  }

  @Override
  public DataSet<Row> getDataSet(ExecutionEnvironment execEnv) {
    return execEnv.createInput(new JsonRowInputFormat(filePath, fieldNames, fieldTypes),
        getReturnType()).name(explainSource());
  }

  @Override
  public TableSource<Row> projectFields(int[] selectedFields) {
    return new JsonTableSource(filePath, fieldNames, fieldTypes);
  }

  @Override
  public TypeInformation<Row> getReturnType() {
    return new RowTypeInfo(fieldTypes , fieldNames);
  }

  @Override
  public TableSchema getTableSchema() {
    return new TableSchema(fieldNames, fieldTypes);
  }

  @Override
  public String explainSource() {
    return String.format("JsonTableSource( read fields: %s)", fieldNames.toString());
  }
}

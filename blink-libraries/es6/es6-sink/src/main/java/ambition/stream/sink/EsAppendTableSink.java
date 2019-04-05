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

package ambition.stream.sink;

import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.util.TableConnectorUtil;
import org.apache.flink.types.Row;

public class EsAppendTableSink implements AppendStreamTableSink<Row> {

  public String[] fieldNames;
  public TypeInformation<?>[] fieldTypes;
  public final Map<String, String> props;
  private int[] namePrefixIndexs;

  public EsAppendTableSink(Map<String, String> props) {
    this.props = props;
  }

  @Override
  public void emitDataStream(DataStream<Row> dataStream) {
    String indexNames = props.get(ElasticSearchConfig.indexNames);
    if (indexNames != null) {
      String[] indexNameArray = indexNames.split(",");
      namePrefixIndexs = new int[indexNameArray.length];
      for (int i = 0; i < indexNameArray.length; i++) {
        String namePrefix = indexNameArray[i];
        for (int j = 0; j < fieldNames.length; j++) {
          if (namePrefix.equalsIgnoreCase(fieldNames[j])) {
            namePrefixIndexs[i] = j;
          }
        }
      }
    }

    final EsAppendSink es = new EsAppendSink(props, namePrefixIndexs, fieldNames);
    dataStream.addSink(es).name(TableConnectorUtil
        .generateRuntimeName(this.getClass(), this.fieldNames));
  }

  @Override
  public TypeInformation<Row> getOutputType() {
    return new RowTypeInfo(getFieldTypes());
  }

  @Override
  public String[] getFieldNames() {
    return fieldNames;
  }

  @Override
  public TypeInformation<?>[] getFieldTypes() {
    return fieldTypes;
  }

  @Override
  public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
    EsAppendTableSink es = new EsAppendTableSink(props);
    es.fieldNames = fieldNames;
    es.fieldTypes = fieldTypes;
    return es;
  }
}

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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.util.TableConnectorUtil;
import org.apache.flink.types.Row;

public class EsUpsertTableSink implements UpsertStreamTableSink<Row> {
  public String[] fieldNames;
  public TypeInformation<?>[] fieldTypes;

  public String[] keys;
  public Boolean appendOnly;

  public final Map<String, String> props;

  private int[] namePrefixIndexs;
  private int[] keyIndexs;

  public EsUpsertTableSink(Map<String, String> props) {
    this.props = props;
  }

  @Override
  public void setKeyFields(String[] keys) {
    this.keys = keys;
  }

  @Override
  public void setIsAppendOnly(Boolean appendOnly) {
    this.appendOnly = appendOnly;
  }

  @Override
  public TypeInformation<Row> getRecordType() {
    return null;
  }

  @Override
  public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
    String indexNamePrefixes = props.get(ElasticSearchConfig.indexNames);
    if (indexNamePrefixes != null) {
      String[] indexNameArray = indexNamePrefixes.split(",");
      namePrefixIndexs = new int[indexNameArray.length];
      for (int i = 0; i < indexNameArray.length; i++) {
        for (int j = 0; j < fieldNames.length; j++) {
          if (indexNameArray[i].equalsIgnoreCase(fieldNames[j])) {
            namePrefixIndexs[i] = j;
          }
        }
      }
    }

    String pk = props.get("primary.key");
    if (pk != null) {
      String[] primaryKeyArray = pk.split(",");
      keyIndexs = new int[primaryKeyArray.length];
      for (int i = 0; i < primaryKeyArray.length; i++) {
        for (int j = 0; j < fieldNames.length; j++) {
          if (primaryKeyArray[i].equalsIgnoreCase(fieldNames[j])) {
            keyIndexs[i] = j;
          }
        }
      }
    }

    EsUpsertSink es = new EsUpsertSink(props, namePrefixIndexs, keyIndexs, fieldNames);
    dataStream.addSink(es).name(TableConnectorUtil
        .generateRuntimeName(this.getClass(), fieldNames));
  }

  @Override
  public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
    return new TupleTypeInfo(Types.BOOLEAN(), new RowTypeInfo(getFieldTypes()));
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
  public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
    EsUpsertTableSink es = new EsUpsertTableSink(props);
    es.fieldNames = fieldNames;
    es.fieldTypes = fieldTypes;
    return es;
  }
}


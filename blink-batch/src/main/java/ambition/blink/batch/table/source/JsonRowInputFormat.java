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

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

public class JsonRowInputFormat extends RichInputFormat<Row, InputSplit> {

  private String filePath;
  private String[] fieldNames;
  private TypeInformation[] fieldTypes;
  private boolean hasLine;
  private transient FSDataInputStream inputStream;
  private transient Scanner sc;

  private JsonNodeDeserializationSchema schema;

  public JsonRowInputFormat(String filePath, String[] fieldNames, TypeInformation[] fieldTypes) {
    this.filePath = filePath;
    this.fieldNames = fieldNames;
    this.fieldTypes = fieldTypes;
  }

  @Override
  public void configure(Configuration parameters) {
    //do nothing here
  }

  @Override
  public void open(InputSplit split) throws IOException {
    Preconditions.checkNotNull(filePath, "json file path not null");
    Path path = new Path(filePath);
    FileSystem fs = FileSystem.get(path.toUri());
    inputStream = fs.open(path);
    sc = new Scanner(inputStream, "UTF-8");
    hasLine = sc.hasNextLine();
    schema = new JsonNodeDeserializationSchema();
  }

  @Override
  public boolean reachedEnd() throws IOException {
    return !hasLine;
  }

  @Override
  public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
    return cachedStatistics;
  }

  @Override
  public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
    return new DefaultInputSplitAssigner(inputSplits);
  }

  @Override
  public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
    if (minNumSplits <= 0) {
      return new GenericInputSplit[]{new GenericInputSplit(0, 1)};
    }
    GenericInputSplit[] ret = new GenericInputSplit[minNumSplits];
    for (int i = 0; i < minNumSplits; i++) {
      ret[i] = new GenericInputSplit(i, ret.length);
    }
    return ret;
  }

  @Override
  public Row nextRecord(Row row) throws IOException {
    if (!hasLine) {
      return null;
    }

    String line = sc.nextLine();
    ObjectNode objectNode = schema.deserialize(line.getBytes());

    List<JsonNode> resultList = new LinkedList<>();
    for (String fieldName: fieldNames) {
      JsonNode value = objectNode.findValue(fieldName);
      resultList.add(value);
    }

    for (int pos = 0; pos < row.getArity(); pos++) {
      if (null != resultList.get(pos)) {
        setRowField(row, resultList, pos);
      } else {
        row.setField(pos, null);
      }
    }

    hasLine = sc.hasNextLine();
    return row;
  }

  @Override
  public void close() throws IOException {
    if (null != inputStream) {
      inputStream.close();
    }

    if (null != sc) {
      sc.close();
    }
  }

  private void setRowField(Row row, List<JsonNode> resultList, int pos) {
    String fieldType = fieldTypes[pos].toString();
    switch (fieldType) {
      case "Byte":
        row.setField(pos, Byte.valueOf(resultList.get(pos).asText()));
        break;
      case "Short":
        row.setField(pos, Short.valueOf(resultList.get(pos).asText()));
        break;
      case "Boolean":
        row.setField(pos, resultList.get(pos).asBoolean());
        break;
      case "Integer":
        row.setField(pos, resultList.get(pos).asInt());
        break;
      case "Long":
        row.setField(pos, resultList.get(pos).asLong());
        break;
      case "Float":
        row.setField(pos, Float.valueOf(resultList.get(pos).asText()));
        break;
      case "Double":
        row.setField(pos, resultList.get(pos).asDouble());
        break;
      //这里时间类型为string类型
      case "Date":
        row.setField(pos, Date.valueOf(resultList.get(pos).asText()));
        break;
      case "Time":
        row.setField(pos, Time.valueOf(resultList.get(pos).asText()));
        break;
      case "Timestamp":
        row.setField(pos, Timestamp.valueOf(resultList.get(pos).asText()));
        break;
      case "String":
        row.setField(pos, resultList.get(pos).asText());
        break;
      default:
        throw new IllegalArgumentException("no support type " + fieldType);
    }
  }
}


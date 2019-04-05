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

package ambition.blink.batch;

import ambition.blink.batch.table.source.JsonTableSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.CsvTableSource;

import java.util.Map;
import ambition.blink.common.table.TableInfo;

public class BatchTableUtils {
  static final FileSystem.WriteMode DEFAULT_WRITEMODE =FileSystem.WriteMode.OVERWRITE;
  static final int NUM_FILES =1;

  //  Only BatchTableSource can be registered in BatchTableEnvironment.
  public static BatchTableSource getBatchTableSource(TableInfo source, Map<String,String> extParams) {
    BatchTableSource result = null;
    Map<String,TypeInformation<?>> outputSchemaMap = source.getFlinkUseSchema();
    Map<String, String> kvs = source.getProps();
    String type = kvs.getOrDefault("type", "csv");

    String filePath;

    String[] fieldNames = new String[outputSchemaMap.size()];
    outputSchemaMap.keySet().toArray(fieldNames);

    TypeInformation[] fieldTypes = new TypeInformation[outputSchemaMap.size()];
    outputSchemaMap.values().toArray(fieldTypes);

    int[] selectedFields = new int[fieldNames.length];
    for (int i=0; i < fieldNames.length; i++) {
      selectedFields[i] = i;
    }

    switch (type.toUpperCase()) {
      // TXT and CSV
      case "CSV":
        filePath = kvs.getOrDefault("file.path", "/");
        String fieldDelim = kvs.getOrDefault("field.delim", CsvInputFormat.DEFAULT_FIELD_DELIMITER);
        result = new CsvTableSource(filePath, fieldNames, fieldTypes,fieldDelim,
            CsvInputFormat.DEFAULT_LINE_DELIMITER, null, false, null, false);
        ((CsvTableSource) result).projectFields(selectedFields);
        break;
      case "JSON":
        filePath = kvs.getOrDefault("file.path", "/");
        result = new JsonTableSource(filePath, fieldNames, fieldTypes);
        ((JsonTableSource) result).projectFields(selectedFields);
        break;
      default:
        break;
    }

    return result;
  }

  // Only BatchTableSink can be registered in BatchTableEnvironment.
  public static BatchTableSink getBatchTableSink(TableInfo sink, Map<String,String> extParams) {
    BatchTableSink result = null;

    Map<String, String> kvs = sink.getProps();
    String type = kvs.getOrDefault("type", "csv");

    switch (type.toUpperCase()) {
      case "CSV":
        String filePath = kvs.getOrDefault("file.path", "/");
        String fieldDelim = kvs.getOrDefault("field.delim", CsvInputFormat.DEFAULT_FIELD_DELIMITER);
        result = new CsvTableSink(filePath, fieldDelim, NUM_FILES, DEFAULT_WRITEMODE);
        break;
      default:
        break;
    }
    return result;
  }

}
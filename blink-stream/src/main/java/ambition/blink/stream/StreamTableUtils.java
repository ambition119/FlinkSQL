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

package ambition.blink.stream;

import ambition.stream.sink.EsAppendTableSink;
import ambition.stream.sink.EsUpsertTableSink;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.Kafka010TableSource;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import ambition.blink.common.table.TableInfo;

public class StreamTableUtils {

  public static TableSource getTableSource(TableInfo source, Map<String,String> extParams) {
    TableSource result = null;
    Map<String,TypeInformation<?>> outputSchemaMap = source.getFlinkUseSchema();
    Map<String, String> kvs = source.getProps();
    String type = kvs.getOrDefault("type", "kafka");

    switch (type.toUpperCase()) {
      case "KAFKA":
//          TODO
//        new Kafka010TableSource();
        break;
      default:
        break;
    }

    return result;
  }

  public static TableSink getTableSink(TableInfo sink, Map<String,String> extParams) {
    TableSink result = null;

    Map<String, String> kvs = sink.getProps();
    String type = kvs.getOrDefault("type", "kafka");

    switch (type.toUpperCase()) {
      case "KAFKA":
//        new Kafka010TableSource();
        break;
      case "ES":
        boolean containsKey = kvs.containsKey("primary.key");
        if (!containsKey) {
          result = new EsAppendTableSink(kvs);
        } else {
          result = new EsUpsertTableSink(kvs);
        }
        break;

      default:
        break;
    }
    return result;
  }

}

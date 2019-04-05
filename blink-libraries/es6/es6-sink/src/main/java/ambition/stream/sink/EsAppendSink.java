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

import java.io.Serializable;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EsAppendSink extends RichSinkFunction<Row> implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(EsAppendSink.class);
  private static final long serialVersionUID = -4688790759713002634L;

  private String[] fieldNames;
  private TransportClient client = null;
  private Settings settings;
  private Map<String, String> props;
  private String jsonSchema;
  private String indexType;
  private String indexNamePrefix;
  private String indexFlag;

  //es的index name拼凑
  private int[] namePrefixIndexes;

  public EsAppendSink(Map<String, String> props, int[] indexNamePrefixIndexes, String[] fieldNames) {
    this.props = props;
    this.namePrefixIndexes = indexNamePrefixIndexes;
    this.fieldNames = fieldNames;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    String clusterName = props.get(ElasticSearchConfig.clusterName);
    Preconditions.checkNotNull(clusterName, "es clusterName not null");

    String address = props.get(ElasticSearchConfig.address);
    Preconditions.checkNotNull(address, "es address not null");
    int port = Integer.parseInt(props.get(ElasticSearchConfig.port));
    if (port <= 0) {
      throw new IllegalArgumentException("port must is integer");
    }

    this.indexType = props.get(ElasticSearchConfig.indexType);
    Preconditions.checkNotNull(clusterName, "es index type not null");
    this.indexNamePrefix = props.get(ElasticSearchConfig.indexNamePrefix);
    Preconditions.checkNotNull(clusterName, "es index name prefix not null");
    this.jsonSchema = props.get(ElasticSearchConfig.jsonSchema);
    Preconditions.checkNotNull(clusterName, "es json schema not null");
    this.indexFlag = props.getOrDefault(ElasticSearchConfig.indexFlag, "-");

    // client的相关设置
    settings = Settings.builder()
        .put("cluster.name", clusterName)
        .build();
    try {
      TransportAddress transportAddress = new TransportAddress(InetAddress.getByName(address), port);
      client = new PreBuiltTransportClient(settings).addTransportAddress(transportAddress);
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error("create es client failed", e);
    }

    // 索引的相关设置
    try {
      byte[] jsonSchemaBytes = jsonSchema.getBytes();
      PutIndexTemplateRequestBuilder requestBuilder =
          client.admin().indices()
              .preparePutTemplate(indexNamePrefix)
              .setSource(jsonSchemaBytes, XContentType.JSON);
      requestBuilder.execute().get();
    } catch (Exception e) {
      LOG.error("put index template failed", e);
      e.printStackTrace();
    }
  }

  @Override
  public void invoke(Row element, Context context) throws Exception {
    createIndexStat(element);
  }

  @Override
  public void close() throws Exception {
    if (null != client) {
      client.close();
    }
  }

  public void createIndexStat(Row row) {
    try {
      final StringBuilder builder = new StringBuilder();
      if (null != indexNamePrefix) {
        builder.append(indexNamePrefix).append(indexFlag);
      }

      if (null != namePrefixIndexes) {
        for (int i = 0; i < namePrefixIndexes.length; i++) {
          Object field = row.getField(namePrefixIndexes[i]);
          builder.append(field).append(indexFlag);
        }
      }
      if (builder.toString().endsWith(indexFlag)) {
        builder.deleteCharAt(builder.length() - 1);
      }
      LOG.info("index value {}", builder.toString());

      Map<String, Object> map = new HashMap<>();
      for (int i = 0; i < fieldNames.length; i++) {
        map.put(fieldNames[i], row.getField(i));
      }

      client.prepareIndex(builder.toString(), indexType)
          .setSource(map).get();
    } catch (Exception e) {
      LOG.error("insert callStat to es failed", e);
      e.printStackTrace();
    }
  }

}


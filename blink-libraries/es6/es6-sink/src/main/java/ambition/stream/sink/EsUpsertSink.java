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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EsUpsertSink extends RichSinkFunction<Tuple2<Boolean, Row>> implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(EsUpsertSink.class);
  private static final long serialVersionUID = 1826745662813857678L;

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
  // update的key拼凑
  private int[] keyIndexs;

  public EsUpsertSink(Map<String, String> props, int[] namePrefixIndexes, int[] keyIndexes, String[] fieldNames) {
    this.props = props;
    this.namePrefixIndexes = namePrefixIndexes;
    this.keyIndexs = keyIndexes;
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
      client = new PreBuiltTransportClient(settings).addTransportAddress(
          new TransportAddress(InetAddress.getByName(address), port));
    } catch (Exception e) {
      LOG.error("create es client failed", e);
    }

    // 索引的相关设置
    try {
      PutIndexTemplateRequestBuilder requestBuilder =
          client.admin().indices()
              .preparePutTemplate(indexNamePrefix)
              .setSource(jsonSchema.getBytes(), XContentType.JSON);
      requestBuilder.execute().get();
    } catch (Exception e) {
      LOG.error("put index template failed", e);
    }
  }

  @Override
  public void invoke(Tuple2<Boolean, Row> element, Context context) throws Exception {
    createIndexStat(element.f1);
  }

  @Override
  public void close() throws Exception {
    if (null != client) {
      client.close();
    }
  }

  public void createIndexStat(Row row) {
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
    String index = builder.toString();
    LOG.info("index value {}", index);

    Map<String, Object> map = new HashMap<>();
    for (int i = 0; i < fieldNames.length; i++) {
      map.put(fieldNames[i], row.getField(i));
    }

    final StringBuilder sb = new StringBuilder();
    if (null != keyIndexs) {
      for (int i = 0; i < keyIndexs.length; i++) {
        Object field = row.getField(keyIndexs[i]);
        sb.append(field).append(indexFlag);
      }
    }
    if (sb.toString().endsWith(indexFlag)) {
      sb.deleteCharAt(sb.length() - 1);
    }
    LOG.info("key value {}", sb.toString());

    IndicesExistsRequest request = new IndicesExistsRequest(index);
    IndicesExistsResponse response = client.admin().indices().exists(request).actionGet();
    if (!response.isExists()) {
      try {
        client.prepareIndex(index, indexType, sb.toString()).
            setSource(map).get();
      } catch (Exception e) {
        LOG.error("insert callStat to create es index failed", e);
      }
    } else {
      try {
        UpdateRequest updateRequest = new UpdateRequest(index, indexType, sb.toString())
            .doc(map, XContentType.JSON)
            .upsert(map, XContentType.JSON);
        client.update(updateRequest).get();
      } catch (Exception e) {
        LOG.error("insert callStat to update es index failed", e);
      }
    }

  }

}
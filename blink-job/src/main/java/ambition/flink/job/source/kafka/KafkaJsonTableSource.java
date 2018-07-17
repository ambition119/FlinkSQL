package ambition.flink.job.source.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.Kafka010TableSource;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.types.Row;

import java.util.*;

/**
 * @Author: wpl
 */
public class KafkaJsonTableSource extends Kafka010TableSource {
    public KafkaJsonTableSource(TableSchema schema, Optional<String> proctimeAttribute, List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors, Optional<Map<String, String>> fieldMapping, String topic, Properties properties, DeserializationSchema<Row> deserializationSchema, StartupMode startupMode, Map<KafkaTopicPartition, Long> specificStartupOffsets) {
        super(schema, proctimeAttribute, rowtimeAttributeDescriptors, fieldMapping, topic, properties, deserializationSchema, startupMode, specificStartupOffsets);
    }
}

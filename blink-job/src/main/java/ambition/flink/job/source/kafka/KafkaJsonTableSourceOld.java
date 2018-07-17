package ambition.flink.job.source.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @Author: wpl
 */
public class KafkaJsonTableSourceOld extends Kafka010JsonTableSource {

    public KafkaJsonTableSourceOld(String topic, Properties properties, TableSchema schema, String proctime, RowtimeAttributeDescriptor rowtimeAttributeDescriptor) {
        super(topic, properties, schema, schema);
        super.setStartupMode(StartupMode.GROUP_OFFSETS);

        if (proctime != null){
            super.setProctimeAttribute(proctime);
        }

        if (rowtimeAttributeDescriptor != null) {
            super.setRowtimeAttributeDescriptor(rowtimeAttributeDescriptor);
        } else {
            super.setRowtimeAttributeDescriptors((List<RowtimeAttributeDescriptor>) Collections.EMPTY_LIST);
        }
    }

    @Override
    protected FlinkKafkaConsumerBase<Row> createKafkaConsumer(String topic, Properties properties, DeserializationSchema<Row> deserializationSchema) {

        List<String> topics = Arrays.asList(topic.split("#"));

        return new FlinkKafkaConsumer010<>(topics, deserializationSchema, properties);
    }

}

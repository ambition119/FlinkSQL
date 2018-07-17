package ambition.flink.job.provider;

import ambition.flink.job.source.kafka.KafkaJsonTableSourceOld;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchemaBuilder;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;
import ambition.client.table.FlinkTableCatalogProvider;
import ambition.flink.job.JobConstant;
import scala.Option;

import java.util.Optional;
import java.util.*;

/**
 * @Author: wpl
 */
public abstract class AbstractKafkaSourceProvider implements FlinkTableCatalogProvider {

    @Override
    public TableSource getInputTableSource(Map<String, String> props, TableSchema schema) {
        //  去掉以kafka.开始的参数
        Properties kafkaProps = getSubProperties(props,"kafka.");

        //根据source类型创建不同的tablesource,json,avro
        String source = props.get("source");

        //注意，相同的字段不能同时设置为rowtime和poctime，不同字段可以
        String proctime = null;
        if(props.containsKey(JobConstant.PROCTIME)) {
            proctime = props.getOrDefault(JobConstant.PROCTIME, JobConstant.PROCTIME);
        }

        RowtimeAttributeDescriptor rowtimeAttributeDes = null;
        for (String key: props.keySet()){
           if (key.startsWith("watermarks_")){
               String colName = key.split("watermarks_")[1];
               String interval = props.get(key);

               rowtimeAttributeDes = new RowtimeAttributeDescriptor(colName,
                       new ExistingField(colName),
                       new BoundedOutOfOrderTimestamps(Long.valueOf(interval))
               );
           }
        }

        //TODO: avro类型的tablesource

        String topic = kafkaProps.getProperty("topic");

        List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors = Collections.singletonList(
                rowtimeAttributeDes);

        final Map<String, String> fieldMapping = new HashMap<>();
        String[] columnNames = schema.getColumnNames();
        TableSchemaBuilder builder = TableSchema.builder();
        for (String columnName: columnNames) {
            fieldMapping.put(columnName, columnName);
            Option<TypeInformation<?>> type = schema.getType(columnName);
            builder.field(columnName,type.get());
        }
        TypeInformation<Row> rowTypeInformation = builder.build().toRowType();


        TypeInformation[] types = new TypeInformation[columnNames.length];
        for(int i = 0; i< columnNames.length; i++){
            String columnName = schema.getColumnName(i).get();
            TypeInformation<?> typeInformation = schema.getType(columnName).get();
            types[i] = typeInformation;
        }
        TypeInformation<Row> typeInformation = new RowTypeInfo(types, columnNames);

        final Map<KafkaTopicPartition, Long> specificOffsets = new HashMap<>();
        specificOffsets.put(new KafkaTopicPartition(topic, 0), 0l);

        Optional<String> proctimeAttribute = Optional.ofNullable(proctime);
        Optional<Map<String, String>> fieldMappingOptional = Optional.ofNullable(fieldMapping);

        //todo 新的KafkaJsonTableSource对象创建待解决
//         new KafkaJsonTableSource(schema,
//                proctimeAttribute,
//                rowtimeAttributeDescriptors,
//                fieldMappingOptional,
//                topic,
//                kafkaProps,
//                new CustomerJsonDeserialization(rowTypeInformation),
////                new CustomerJsonDeserialization(typeInformation),
//                StartupMode.SPECIFIC_OFFSETS,
//                specificOffsets);

        return new KafkaJsonTableSourceOld(topic, kafkaProps, schema, proctime, rowtimeAttributeDes);

    }

    private Properties getSubProperties(Map<String, String> props, String prefix) {
        Properties prop = new Properties();
        for (Map.Entry<String, String> e : props.entrySet()) {
            String key = e.getKey();
            if (key.startsWith(prefix)) {
                prop.put(key.substring(prefix.length()), e.getValue());
            }
        }
        return prop;
    }

}
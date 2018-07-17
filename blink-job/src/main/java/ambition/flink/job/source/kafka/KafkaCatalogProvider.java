package ambition.flink.job.source.kafka;

import org.apache.flink.table.api.TableSchema;
import ambition.client.table.FlinkTableCatalog;
import ambition.flink.job.provider.AbstractKafkaSourceProvider;

import java.util.Map;

/**
 * @Author: wpl
 */
public class KafkaCatalogProvider extends AbstractKafkaSourceProvider {

    private static final String TYPE = "kafka";

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public FlinkTableCatalog getOutputCatalog(Map<String, String> props, TableSchema schema) {
        return new KafkaTableCatalog(props,schema);
    }


}

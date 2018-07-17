package ambition.client.table;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.TableSource;

import java.util.Map;

/**
 * @Author: wpl
 */
public interface FlinkTableCatalogProvider {
    String getType();

    TableSource getInputTableSource(Map<String,String> props, TableSchema schema);

    FlinkTableCatalog getOutputCatalog(Map<String,String> props, TableSchema schema);
}

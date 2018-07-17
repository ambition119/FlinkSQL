package ambition.flink.job.source.kafka;

import org.apache.flink.table.api.CatalogNotExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import ambition.client.table.FlinkTableCatalog;
import ambition.flink.job.InputExternalCatalogTable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: wpl
 */
public class KafkaTableCatalog implements FlinkTableCatalog {
    private Map<String, String> tableProp ;
    public final TableSchema schema;

    public KafkaTableCatalog(Map<String, String> tablePro, TableSchema schema) {
        this.tableProp = tablePro;
        this.schema = schema;
    }

    @Override
    public ExternalCatalogTable getTable(String tableName) throws TableNotExistException {
        Map<String, String> sourceTableProp = new HashMap<>();
        for (HashMap.Entry<String, String> item : this.tableProp.entrySet()){
            String key = item.getKey();
            if(key.toLowerCase().startsWith("kafka.")){
                String val = item.getValue();
                sourceTableProp.put(key,val);
            }
        }
        return getExternalCatalogTable(sourceTableProp, schema);
    }

    @Override
    public List<String> listTables() {
        return Collections.emptyList();
    }

    @Override
    public ExternalCatalog getSubCatalog(String dbName) throws CatalogNotExistException {
        throw new CatalogNotExistException(dbName);
    }

    @Override
    public List<String> listSubCatalogs() {
        return Collections.emptyList();
    }

    static InputExternalCatalogTable getExternalCatalogTable(Map<String, String> props, TableSchema schema) {
        props.put("connector.type","kafka");
        return new InputExternalCatalogTable(props);
    }
}

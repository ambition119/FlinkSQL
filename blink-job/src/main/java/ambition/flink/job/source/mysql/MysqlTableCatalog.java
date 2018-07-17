package ambition.flink.job.source.mysql;

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
public class MysqlTableCatalog implements FlinkTableCatalog {

    private Map<String, String> props ;
    public  TableSchema schema;

    public MysqlTableCatalog(Map<String, String> props, TableSchema schema) {
        this.props = props;
        this.schema = schema;
    }

    @Override
    public ExternalCatalogTable getTable(String tableName) throws TableNotExistException {
        Map<String, String> tableProps = new HashMap<>();
        for (HashMap.Entry<String, String> item : this.props.entrySet()){
            String key = item.getKey();
            if(key.toLowerCase().startsWith("mysql.")){
                String val = item.getValue();
                tableProps.put(key,val);
            }
        }
        return getExternalCatalogTable(tableProps, schema);
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
        props.put("connector.type","mysql");
        return new InputExternalCatalogTable(props);
    }
}

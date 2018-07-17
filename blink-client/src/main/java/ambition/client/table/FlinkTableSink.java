package ambition.client.table;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ExternalCatalog;

/**
 * @Author: wpl
 */
public class FlinkTableSink {
    private TableSchema schema;
    private String tableName;
    private ExternalCatalog tableSink;
    //kafak,mysql
    private String sinkType;
    //append
    private String typeName;

    public FlinkTableSink() {
    }

    public FlinkTableSink(TableSchema schema, String tableName, ExternalCatalog tableSink, String sinkType, String typeName) {
        this.schema = schema;
        this.tableName = tableName;
        this.tableSink = tableSink;
        this.sinkType = sinkType;
        this.typeName = typeName;
    }

    public TableSchema getSchema() {
        return schema;
    }

    public void setSchema(TableSchema schema) {
        this.schema = schema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public ExternalCatalog getTableSink() {
        return tableSink;
    }

    public void setTableSink(ExternalCatalog tableSink) {
        this.tableSink = tableSink;
    }

    public String getSinkType() {
        return sinkType;
    }

    public void setSinkType(String sinkType) {
        this.sinkType = sinkType;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }
}

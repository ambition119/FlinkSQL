package ambition.sqlserver.sql.parser;

import java.util.Map;

/**
 * @Author: wpl
 */
public class TableInfo {
    private String tableName;
    private String tableType;
    private String primarys;
    //字段和类型
    Map<String,String> schema;
    //kv键值对，props信息
    Map<String,String> props;

    Map<String,String> virtuals ;
    Map<String,String> watermarks;

    public TableInfo() {
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public String getPrimarys() {
        return primarys;
    }

    public void setPrimarys(String primarys) {
        this.primarys = primarys;
    }

    public Map<String, String> getSchema() {
        return schema;
    }

    public void setSchema(Map<String, String> schema) {
        this.schema = schema;
    }

    public Map<String, String> getProps() {
        return props;
    }

    public void setProps(Map<String, String> props) {
        this.props = props;
    }

    public Map<String, String> getVirtuals() {
        return virtuals;
    }

    public void setVirtuals(Map<String, String> virtuals) {
        this.virtuals = virtuals;
    }

    public Map<String, String> getWatermarks() {
        return watermarks;
    }

    public void setWatermarks(Map<String, String> watermarks) {
        this.watermarks = watermarks;
    }
}

package ambition.client.common.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Author: wpl
 */
public class SqlParserResDescriptor {
    private Map<String,String> parms = new LinkedHashMap<>();
    private Map<String,TypeInformation<?>> schemas = new LinkedHashMap<>();
    private String sourceType;
    private String tableName;
    private String sqlInfo;

    //虚拟列
    Map<String,String> virtuals ;
    //proctime,rowtime信息
    Map<String,String> watermarks;

    public SqlParserResDescriptor() {
    }

    public Map<String, String> getParms() {
        return parms;
    }

    public void setParms(Map<String, String> parms) {
        this.parms = parms;
    }

    public Map<String, TypeInformation<?>> getSchemas() {
        return schemas;
    }

    public void setSchemas(Map<String, TypeInformation<?>> schemas) {
        this.schemas = schemas;
    }

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSqlInfo() {
        return sqlInfo;
    }

    public void setSqlInfo(String sqlInfo) {
        this.sqlInfo = sqlInfo;
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

    @Override
    public String toString() {
        return "SqlParserResDescriptor{" +
                "parms=" + parms +
                ", schemas=" + schemas +
                ", sourceType='" + sourceType + '\'' +
                ", tableName='" + tableName + '\'' +
                ", sqlInfo='" + sqlInfo + '\'' +
                '}';
    }
}

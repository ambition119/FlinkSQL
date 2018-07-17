package pingle.wang.common.table;

import java.util.Map;

/**
 * @Author: wpl
 */
public class TableInfo {
   private String tableName;
   private Type tableType;
   private String primarys;
   //partitioned by
   private String partitionedBy;

   //字段和类型
   private Map<String,String> schema;
   //kv键值对，props信息
   private Map<String,String> props;

   private Map<String,String> virtuals ;
   private Map<String,String> watermarks;

   public TableInfo() {
   }

   public String getTableName() {
      return tableName;
   }

   public void setTableName(String tableName) {
      this.tableName = tableName;
   }

   public Type getTableType() {
      return tableType;
   }

   public void setTableType(Type tableType) {
      this.tableType = tableType;
   }

   public String getPrimarys() {
      return primarys;
   }

   public void setPrimarys(String primarys) {
      this.primarys = primarys;
   }

   public String getPartitionedBy() {
      return partitionedBy;
   }

   public void setPartitionedBy(String partitionedBy) {
      this.partitionedBy = partitionedBy;
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

package ambition.sqlserver.sql.parser;

import com.google.common.collect.Maps;
import ambition.util.SqlUtil;
import ambition.util.StringUtil;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * @Author: wpl
 */
public class CreateTableParser {

    public static CreateTableParser newInstance(){
        return new CreateTableParser();
    }

    public boolean verify(String sql) {
        return InputSqlPattern.CREATE_TABLE_PATTERN.matcher(sql).find();
    }

    public SqlParserResult parseSql(String sql) {
        SqlParserResult result = null;
        sql = SqlUtil.getReplaceSql(sql);
        Matcher matcher = InputSqlPattern.CREATE_TABLE_PATTERN.matcher(sql);
        if(matcher.find()){
            String tableName = matcher.group(1);
            String fieldsInfoStr = matcher.group(2);
            String propsStr = matcher.group(3);
            Map<String, String> props = parseProp(propsStr);

            result = new SqlParserResult();
            result.setTableName(tableName);
            result.setFieldsInfoStr(fieldsInfoStr);
            result.setPropMap(props);

        }
        return result;
    }

    private Map parseProp(String propsStr){
        String[] strs = StringUtil.splitIgnoreQuotaBrackets(propsStr.trim(),",");
        Map<String, String> propMap = Maps.newLinkedHashMap();
        for(int i=0; i<strs.length; i++){
            List<String> ss = StringUtil.splitIgnoreQuota(strs[i], '=');
            String key = ss.get(0).trim().replaceAll("'", "").trim();
            String value = ss.get(1).trim().replaceAll("'", "").trim();
            propMap.put(key, value);
        }

        return propMap;
    }

    public class SqlParserResult{

        private String tableName;

        private String fieldsInfoStr;

        private Map<String, String> propMap;

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getFieldsInfoStr() {
            return fieldsInfoStr;
        }

        public void setFieldsInfoStr(String fieldsInfoStr) {
            this.fieldsInfoStr = fieldsInfoStr;
        }

        public Map<String, String> getPropMap() {
            return propMap;
        }

        public void setPropMap(Map<String, String> propMap) {
            this.propMap = propMap;
        }
    }
}

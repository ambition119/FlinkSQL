package ambition.sqlserver.sql.parser;

import ambition.util.SqlUtil;
import ambition.util.StringUtil;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author: wpl
 */
public class TableInfoParser {
    private static final String source = "source";
    private static final String sink = "sink";
    //维度表
    private static final String side = "side";
    private static CreateTableParser createTableParser = CreateTableParser.newInstance();

    public static TableInfo parser(String ddl){
        TableInfo tableInfo = new TableInfo();
        //字段和类型
        Map<String,String> schema = new LinkedHashMap<>();

        Map<String,String> virtuals = new LinkedHashMap<>();
        Map<String,String> watermarks = new LinkedHashMap<>();

        CreateTableParser.SqlParserResult parserResult = createTableParser.parseSql(SqlUtil.getReplaceSql(ddl));
        //字段信息
        String fieldsInfoStr = parserResult.getFieldsInfoStr();
        String[] fieldInfos = StringUtil.splitIgnoreQuotaBrackets(fieldsInfoStr.trim(),",");
        for (String fieldInfo: fieldInfos){

            //虚拟列
            Pattern VIRTUALFIELDKEY_PATTERN = InputSqlPattern.keyPatternMap.get(InputSqlPattern.VIRTUAL_KEY);
            Matcher virtual_matcher = VIRTUALFIELDKEY_PATTERN.matcher(fieldInfo.trim());
            if (virtual_matcher.find()){
                String fieldName = virtual_matcher.group(2);
                String expression = virtual_matcher.group(1).toLowerCase();

                virtuals.put(fieldName,expression);
                continue;
            }

            //WATERMARK_KEY信息
            Pattern WATERMARK_PATTERN = InputSqlPattern.keyPatternMap.get(InputSqlPattern.WATERMARK_KEY);
            Matcher watermark_matcher = WATERMARK_PATTERN.matcher(fieldInfo.trim());
            if(watermark_matcher.find()){
                String eventTimeField = watermark_matcher.group(1);
                String offset = watermark_matcher.group(3);
                watermarks.put(eventTimeField,offset);
                continue;
            }

            //PRIMARY_KEY信息
            Pattern PRIMARYKEY_PATTERN = InputSqlPattern.keyPatternMap.get(InputSqlPattern.PRIMARY_KEY);
            Matcher matcher = PRIMARYKEY_PATTERN.matcher(fieldInfo.trim());
            if(matcher.find()){
                String primaryFields = matcher.group(1);
                tableInfo.setPrimarys(primaryFields);
                continue;
            }
            //SIDE_KEY信息
            Pattern SIDE_PATTERN = InputSqlPattern.keyPatternMap.get(InputSqlPattern.SIDE_KEY);
            Matcher side_matcher = SIDE_PATTERN.matcher(fieldInfo.trim());
            if(side_matcher.find()){
                tableInfo.setTableType(side);
                continue;
            }

            //字段信息
            String[] columns = fieldInfo.trim().split(" ");
            if (columns.length == 2){
                schema.put(columns[0].trim().replace("`",""),columns[1].trim().replace("`",""));
            }
        }

        tableInfo.setSchema(schema);
        tableInfo.setTableName(parserResult.getTableName());
        tableInfo.setVirtuals(virtuals);
        tableInfo.setWatermarks(watermarks);
        //with信息
        tableInfo.setProps(parserResult.getPropMap());

        return tableInfo;
    }
}

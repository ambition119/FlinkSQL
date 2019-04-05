/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ambition.blink.sql.parser;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import ambition.blink.common.table.TableInfo;
import ambition.blink.common.table.TableType;
import ambition.blink.common.utils.BlinkStringUtils;

public class TableInfoParser {

  private static final String TABLE_SOURCE ="SOURCE";
  private static final String TABLE_SINK ="SINK";

  public static final String CREATE_SOURCE_PATTERN_STR = "(?i)create\\s+source\\s+table\\s+(\\S+)\\s*\\((.+)\\)\\s*with\\s*\\((.+)\\)";
  public static final Pattern CREATE_SOURCE_PATTERN = Pattern.compile(CREATE_SOURCE_PATTERN_STR);

  public static final String CREATE_SINK_PATTERN_STR = "(?i)create\\s+sink\\s+table\\s+(\\S+)\\s*\\((.+)\\)\\s*with\\s*\\((.+)\\)";
  public static final Pattern CREATE_SINK_PATTERN = Pattern.compile(CREATE_SINK_PATTERN_STR);

  private static CreateTableParser createTableParser = CreateTableParser.newInstance();

  public static TableInfo parser(String ddl){
    TableInfo tableInfo = new TableInfo();
    CreateTableParser.SqlParserResult parserResult = null;
    if (ddl.toUpperCase().contains(TABLE_SOURCE)){
      tableInfo.setTableType(TableType.SOURCE);
      parserResult = createTableParser.parseSql(BlinkStringUtils.getReplaceString(ddl), CREATE_SOURCE_PATTERN);
    } else if(ddl.toUpperCase().contains(TABLE_SINK)){
      tableInfo.setTableType(TableType.SINK);
      parserResult = createTableParser.parseSql(BlinkStringUtils.getReplaceString(ddl), CREATE_SINK_PATTERN);
    }

    //字段和类型
    Map<String,String> schema = new LinkedHashMap<>();
    Map<String,TypeInformation<?>> flinkUseSchema = new LinkedHashMap<>();

    Map<String,String> virtuals = new LinkedHashMap<>();
    Map<String,String> watermarks = new LinkedHashMap<>();

    //字段信息
    String fieldsInfoStr = parserResult.getFieldsInfoStr();
    //with参数信息
    Map<String, String> propMap = parserResult.getPropMap();

    String[] fieldInfos = BlinkStringUtils.splitIgnoreQuotaBrackets(fieldsInfoStr.trim(),",");
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
        tableInfo.setPrimaryKeys(primaryFields);
        propMap.put("primary.key",primaryFields);
        continue;
      }
      //SIDE_KEY信息
      Pattern SIDE_PATTERN = InputSqlPattern.keyPatternMap.get(InputSqlPattern.SIDE_KEY);
      Matcher side_matcher = SIDE_PATTERN.matcher(fieldInfo.trim());
      if(side_matcher.find()){
        tableInfo.setTableType(TableType.SIDE);
        continue;
      }

      //字段信息
      String[] columns = fieldInfo.trim().split(" ");
      if (columns.length == 2){
        schema.put(columns[0].trim().replace("`",""), columns[1].trim().replace("`",""));
        flinkUseSchema.put(columns[0].trim().replace("`",""), getColumnType(columns[1].trim().replace("`","")));
      }
    }

    tableInfo.setInputSchema(schema);
    tableInfo.setFlinkUseSchema(flinkUseSchema);
    tableInfo.setTableName(parserResult.getTableName());
    tableInfo.setVirtuals(virtuals);
    tableInfo.setWatermarks(watermarks);
    //with信息
    tableInfo.setProps(propMap);

    return tableInfo;
  }

  private static TypeInformation<?> getColumnType(String type){
    TypeInformation<?> basicTypeInfo = null ;
    switch (type) {
      case "char":
        basicTypeInfo=BasicTypeInfo.CHAR_TYPE_INFO;
        break;
      case "varchar":
        basicTypeInfo=BasicTypeInfo.STRING_TYPE_INFO;
        break;
      case "boolean":
        basicTypeInfo=BasicTypeInfo.BOOLEAN_TYPE_INFO;
        break;
      case "int":
        basicTypeInfo=BasicTypeInfo.INT_TYPE_INFO;
        break;
      case "bigint":
        basicTypeInfo=BasicTypeInfo.LONG_TYPE_INFO;
        break;
      case "float":
        basicTypeInfo=BasicTypeInfo.FLOAT_TYPE_INFO;
        break;
      case "decimal":
        basicTypeInfo=BasicTypeInfo.BIG_DEC_TYPE_INFO;
        break;
      case "double":
        basicTypeInfo=BasicTypeInfo.DOUBLE_TYPE_INFO;
        break;
      case "byte":
        basicTypeInfo=BasicTypeInfo.BYTE_TYPE_INFO;
        break;
      case "short":
        basicTypeInfo=BasicTypeInfo.SHORT_TYPE_INFO;
        break;
      case "date":
        basicTypeInfo=SqlTimeTypeInfo.DATE;
        break;
      case "time":
        basicTypeInfo=SqlTimeTypeInfo.TIME;
        break;
      case "timestamp":
        basicTypeInfo=SqlTimeTypeInfo.TIMESTAMP;
        break;
      default:
        break;
    }
    return basicTypeInfo;
  }

}

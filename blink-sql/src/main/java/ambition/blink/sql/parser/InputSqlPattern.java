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

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class InputSqlPattern {

    public static final String CREATE_TABLE_PATTERN_STR = "(?i)create\\s+table\\s+(\\S+)\\s*\\((.+)\\)\\s*with\\s*\\((.+)\\)";
    public static final Pattern CREATE_TABLE_PATTERN = Pattern.compile(CREATE_TABLE_PATTERN_STR);

    public static final String CREATE_VIEW_PATTERN_STR = "(?i)create\\s+view\\s*";
    public static final Pattern CREATE_VIEW_PATTERN = Pattern.compile(CREATE_VIEW_PATTERN_STR);

    public static final String CREATE_FUN_PATTERNSTR = "(?i)\\s*create\\s+function\\s+(\\S+)\\s+as\\s+(\\S+)\\s+library\\s+(\\S+)";
    public static final Pattern CREATE_FUN_PATTERN = Pattern.compile(CREATE_FUN_PATTERNSTR);

    public static final String PRIMARY_KEY = "primaryKey";
    //PRIMARY KEY (id)
    public static Pattern PRIMARYKEY_PATTERN = Pattern.compile("(?i)PRIMARY\\s+KEY\\s*\\((.*)\\)");
    //虚拟列，创建字段转换，如字符串时间串转换为timestamp
    //    d AS PROCTIME()
    //    d AS ROWTIME()
    public static final String VIRTUAL_KEY = "virtualFieldKey";
    public static Pattern VIRTUALFIELDKEY_PATTERN = Pattern.compile("(?i)^(\\S+\\([^\\)]+\\))\\s+AS\\s+(\\w+)$");

    public static final String WATERMARK_KEY = "waterMarkKey";
    //WATERMARK wk FOR ts as withOffset(ts, 2000)
    public static Pattern WATERMARK_PATTERN = Pattern.compile("(?i)^\\s*WATERMARK\\s+FOR\\s+(\\S+)\\s+AS\\s+withOffset\\(\\s*(\\S+)\\s*,\\s*(\\d+)\\s*\\)$");

    public static final String SIDE_KEY = "sideKey";
    //维表定义 PERIOD FOR SYSTEM_TIME
    public final static Pattern SIDE_PATTERN =Pattern.compile("(?i)^PERIOD\\s+FOR\\s+SYSTEM_TIME$");

    public static Map<String, Pattern> keyPatternMap = new HashMap<>();

    //存储字段解析正则
    static {
        keyPatternMap.put(VIRTUAL_KEY, VIRTUALFIELDKEY_PATTERN);
        keyPatternMap.put(VIRTUAL_KEY, VIRTUALFIELDKEY_PATTERN);
        keyPatternMap.put(PRIMARY_KEY, PRIMARYKEY_PATTERN);
        keyPatternMap.put(WATERMARK_KEY, WATERMARK_PATTERN);
        keyPatternMap.put(SIDE_KEY, SIDE_PATTERN);
    }
}

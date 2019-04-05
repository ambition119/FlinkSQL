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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import ambition.blink.common.table.FunctionInfo;

public class CreateFuncParser {

    private static final String funcPatternStr = "(?i)\\s*create\\s+function\\s+(\\S+)\\s+as\\s+(\\S+)\\s+library\\s+(\\S+)";

    private static final Pattern funcPattern = Pattern.compile(funcPatternStr);

    public FunctionInfo parseSql(String sql) {
        FunctionInfo result = null;
        Matcher matcher = funcPattern.matcher(sql);
        if(matcher.find()){
            String funcName = matcher.group(1);
            String className = matcher.group(2);
            String jarPath = matcher.group(3);
            result = new FunctionInfo();
            result.setName(funcName);
            result.setClassName(className);
            result.setJarPath(jarPath);
        }
        return result;
    }


    public static CreateFuncParser newInstance(){
        return new CreateFuncParser();
    }
}

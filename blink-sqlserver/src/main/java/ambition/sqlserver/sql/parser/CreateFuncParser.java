package ambition.sqlserver.sql.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author: wpl
 */
public class CreateFuncParser{

    private static final String funcPatternStr = "(?i)\\s*create\\s+(scala|table)\\s+function\\s+(\\S+)\\s+WITH\\s+(\\S+)";

    private static final Pattern funcPattern = Pattern.compile(funcPatternStr);

    public boolean verify(String sql) {
        return funcPattern.matcher(sql).find();
    }

    public SqlParserResult parseSql(String sql) {
        SqlParserResult result = null;
        Matcher matcher = funcPattern.matcher(sql);
        if (matcher.find()) {
            String type = matcher.group(1);
            String funcName = matcher.group(2);
            String className = matcher.group(3);
            result = new SqlParserResult();
            result.setType(type);
            result.setName(funcName);
            result.setClassName(className);
        }
        return result;
    }


    public static CreateFuncParser newInstance() {
        return new CreateFuncParser();
    }

    public class SqlParserResult {

        private String name;

        private String className;

        private String type;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getClassName() {
            return className;
        }

        public void setClassName(String className) {
            this.className = className;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }
}

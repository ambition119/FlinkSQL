package ambition.sqlserver.sql.plan;

import org.apache.calcite.sql.*;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.fs.Path;
import ambition.sqlserver.sql.parser.SqlCreateFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: wpl
 */
public class Validator {
    private final PropertiesConfiguration conf = new PropertiesConfiguration();
    private final ArrayList<Path> additionalResources = new ArrayList<>();
    private final HashMap<String, String> userDefinedFunctions = new HashMap<>();

    private SqlInsert statement;
    private SqlSelect sqlQuery;

    public Configuration options() {
        return conf;
    }

    public ArrayList<Path> additionalResources() {
        return additionalResources;
    }

    public Map<String, String> userDefinedFunctions() {
        return userDefinedFunctions;
    }

    public void validateViewQuery(SqlNode query) {
        extract(query);
        validateExactlyOnceViewSelect(query);
    }

    public void validateDml(SqlNode query) {
        extract(query);
        validateExactlyOnceDml(query);
    }

    public void validateFunction(SqlNode query) {
        extract(query);
    }

    /**
     * Extract options and jars from the queries.
     */
    @VisibleForTesting
    void extract(SqlNode query) {
        if (query instanceof SqlSetOption) {
            extract((SqlSetOption) query);
        } else if (query instanceof SqlCreateFunction) {
            extract((SqlCreateFunction) query);
        }
    }

    public void extract(SqlCreateFunction node) {
        if (node.jarList() == null) {
            return;
        }

        for (SqlNode n : node.jarList()) {
            additionalResources.add(new Path(unwrapConstant(n)));
        }

        String funcName = node.dbName() != null ? unwrapConstant(node.dbName()) + "." + unwrapConstant(node.funcName())
                : unwrapConstant(node.funcName());
        String clazzName = unwrapConstant(node.className());
        userDefinedFunctions.put(funcName, clazzName);
    }

    public void extract(SqlSetOption node) {
        Object value = unwrapConstant(node.getValue());
        String property = node.getName().toString();

        Preconditions.checkArgument(!"SYSTEM".equals(node.getScope()),
                "cannot set properties at the system level");
        conf.setProperty(property, value);
    }

    @VisibleForTesting
    void validateExactlyOnceViewSelect(SqlNode query) {
        if (query instanceof SqlSelect) {
            sqlQuery = (SqlSelect) query;
        }
    }

    @VisibleForTesting
    void validateExactlyOnceDml(SqlNode query) {
        if (query instanceof SqlInsert) {
            statement = (SqlInsert) query;
        }
    }


    public SqlInsert statement() {
        return statement;
    }

    public SqlSelect sqlQuery() {
        return sqlQuery;
    }

    /**
     * Unwrap a constant in the AST as a Java Object.
     *
     * <p>The Calcite validator has folded all the constants by this point.
     * Thus the function expects either a SqlLiteral or a SqlIdentifier but not a SqlCall.</p>
     */
    private static String unwrapConstant(SqlNode value) {
        if (value == null) {
            return null;
        } else if (value instanceof SqlLiteral) {
            return ((SqlLiteral) value).toValue();
        } else if (value instanceof SqlIdentifier) {
            return value.toString();
        } else {
            throw new IllegalArgumentException("Invalid constant " + value);
        }
    }
}


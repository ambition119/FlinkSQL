package ambition.sqlserver.sql.parser;

/**
 * @Author: wpl
 */
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Arrays;
import java.util.List;

public class SqlCreateFunction extends SqlCall  {
    private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("UDF", SqlKind.OTHER_DDL);
    private final SqlIdentifier dbName;
    private final SqlIdentifier funcName;
    private final SqlNode className;
    private final SqlNodeList jarList;

    public SqlCreateFunction(SqlParserPos pos, SqlIdentifier dbName,
                             SqlIdentifier funcName, SqlNode className, SqlNodeList jarList) {
        super(pos);
        this.dbName = dbName;
        this.funcName = funcName;
        this.className = className;
        this.jarList = jarList;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        UnparseUtil u = new UnparseUtil(writer, leftPrec, rightPrec);
        u.keyword("CREATE", "FUNCTION");
        if (dbName != null) {
            u.node(dbName).keyword(".");
        }
        u.node(funcName).keyword("AS").node(className);
        if (jarList != null) {
            u.keyword("USING").nodeList(jarList);
        }
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(dbName, funcName, className, jarList);
    }

    public SqlIdentifier dbName() {
        return dbName;
    }

    public SqlIdentifier funcName() {
        return funcName;
    }

    public SqlNode className() {
        return className;
    }

    public SqlNodeList jarList() {
        return jarList;
    }
}
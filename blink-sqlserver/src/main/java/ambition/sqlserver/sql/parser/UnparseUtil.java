package ambition.sqlserver.sql.parser;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;

import java.util.Arrays;

/**
 * @Author: wpl
 */
public class UnparseUtil   {
    private final SqlWriter writer;
    private final int leftPrec;
    private final int rightPrec;

    UnparseUtil(SqlWriter writer, int leftPrec, int rightPrec) {
        this.writer = writer;
        this.leftPrec = leftPrec;
        this.rightPrec = rightPrec;
    }

    UnparseUtil keyword(String... keywords) {
        Arrays.stream(keywords).forEach(writer::keyword);
        return this;
    }

    UnparseUtil node(SqlNode n) {
        n.unparse(writer, leftPrec, rightPrec);
        return this;
    }

    UnparseUtil nodeList(SqlNodeList l) {
        writer.keyword("(");
        if (l.size() > 0) {
            l.get(0).unparse(writer, leftPrec, rightPrec);
            for (int i = 1; i < l.size(); ++i) {
                writer.keyword(",");
                l.get(i).unparse(writer, leftPrec, rightPrec);
            }
        }
        writer.keyword(")");
        return this;
    }
}

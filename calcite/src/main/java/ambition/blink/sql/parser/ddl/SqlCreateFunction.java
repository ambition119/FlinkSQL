package ambition.blink.sql.parser.ddl;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * @Author: wpl
 */
public class SqlCreateFunction extends SqlCreate {
  private final SqlIdentifier name;
  private final SqlNode className;
  private final SqlNodeList usingList;

  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("CREATE FUNCTION", SqlKind.CREATE_FUNCTION);

  /** Creates a SqlCreateFunction. */
  public SqlCreateFunction(SqlParserPos pos, boolean replace,
      boolean ifNotExists, SqlIdentifier name,
      SqlNode className, SqlNodeList usingList) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.name = Objects.requireNonNull(name);
    this.className = className;
    this.usingList = Objects.requireNonNull(usingList);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec,
      int rightPrec) {
    writer.keyword(getReplace() ? "CREATE OR REPLACE" : "CREATE");
    writer.keyword("FUNCTION");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    name.unparse(writer, 0, 0);
    writer.keyword("AS");
    className.unparse(writer, 0, 0);
    if (usingList.size() > 0) {
      writer.keyword("USING");
      final SqlWriter.Frame frame =
          writer.startList(SqlWriter.FrameTypeEnum.SIMPLE);
      for (Pair<SqlLiteral, SqlLiteral> using : pairs()) {
        writer.sep(",");
        using.left.unparse(writer, 0, 0); // FILE, URL or ARCHIVE
        using.right.unparse(writer, 0, 0); // e.g. 'file:foo/bar.jar'
      }
      writer.endList(frame);
    }
  }

  @SuppressWarnings("unchecked")
  private List<Pair<SqlLiteral, SqlLiteral>> pairs() {
    return Util.pairs((List) usingList.getList());
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return Arrays.asList(name, className, usingList);
  }

  public SqlIdentifier getName() {
    return name;
  }

  public SqlNode getClassName() {
    return className;
  }

  public SqlNodeList getUsingList() {
    return usingList;
  }
}

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

package ambition.blink.sql.parser.ddl;

import static java.util.Objects.requireNonNull;

import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlTableColumn extends SqlCall {

  private SqlIdentifier name;
  private SqlDataTypeSpec type;
  private SqlCharStringLiteral comment;

  public SqlTableColumn(SqlIdentifier name,
      SqlDataTypeSpec type,
      SqlCharStringLiteral comment,
      SqlParserPos pos) {
    super(pos);
    this.name = requireNonNull(name, "Column name should not be null");
    this.type = requireNonNull(type, "Column type should not be null");
    this.comment = comment;
  }

  @Override
  public SqlOperator getOperator() {
    return null;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return null;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    this.name.unparse(writer, leftPrec, rightPrec);
    writer.print(" ");
    if (type.getTypeName() instanceof SqlIdentifier) {
      type.getTypeName().unparse(writer, leftPrec, rightPrec);
    } else {
      type.unparse(writer, leftPrec, rightPrec);
    }
    if (this.comment != null) {
      writer.print(" COMMENT ");
      this.comment.unparse(writer, leftPrec, rightPrec);
    }
  }

  public SqlIdentifier getName() {
    return name;
  }

  public void setName(SqlIdentifier name) {
    this.name = name;
  }

  public SqlDataTypeSpec getType() {
    return type;
  }

  public void setType(SqlDataTypeSpec type) {
    this.type = type;
  }

  public SqlCharStringLiteral getComment() {
    return comment;
  }

  public void setComment(SqlCharStringLiteral comment) {
    this.comment = comment;
  }
}

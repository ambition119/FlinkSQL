<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

boolean IfNotExistsOpt() :
{
}
{
    <IF> <NOT> <EXISTS> { return true; }
    |
    { return false; }
}

void TableColumn(TableCreationContext context) :
{
}
{
    (
        TableColumn2(context.columnList)
    |
        context.primaryKeyList = PrimaryKey()
    |
        UniqueKey(context.uniqueKeysList)
    |
        ComputedColumn(context)
    )
}

void ComputedColumn(TableCreationContext context) :
{
    SqlNode identifier;
    SqlNode expr;
    boolean hidden = false;
    SqlParserPos pos;
}
{
    identifier = SimpleIdentifier() {pos = getPos();}
    <AS>
    expr = Expression(ExprContext.ACCEPT_SUB_QUERY) {
        expr = SqlStdOperatorTable.AS.createCall(Span.of(identifier, expr).pos(), expr, identifier);
        context.columnList.add(expr);
    }
}

void TableColumn2(List<SqlNode> list) :
{
    SqlParserPos pos;
    SqlIdentifier name;
    SqlDataTypeSpec type;
    SqlCharStringLiteral comment = null;
}
{
    name = SimpleIdentifier()
    type = DataType()
    (
        <NULL> { type = type.withNullable(true); }
    |
        <NOT> <NULL> { type = type.withNullable(false); }
    |
        { type = type.withNullable(true); }
    )
    [ <COMMENT> <QUOTED_STRING> {
        String p = SqlParserUtil.parseString(token.image);
        comment = SqlLiteral.createCharString(p, getPos());
    }]
    {
        SqlTableColumn tableColumn = new SqlTableColumn(name, type, comment, getPos());
        list.add(tableColumn);
    }
}

SqlNodeList PrimaryKey() :
{
    List<SqlNode> pkList = new ArrayList<SqlNode>();

    SqlParserPos pos;
    SqlIdentifier columnName;
}
{
    <PRIMARY> { pos = getPos(); } <KEY> <LPAREN>
        columnName = SimpleIdentifier() { pkList.add(columnName); }
        (<COMMA> columnName = SimpleIdentifier() { pkList.add(columnName); })*
    <RPAREN>
    {
        return new SqlNodeList(pkList, pos.plus(getPos()));
    }
}

void UniqueKey(List<SqlNodeList> list) :
{
    List<SqlNode> ukList = new ArrayList<SqlNode>();
    SqlParserPos pos;
    SqlIdentifier columnName;
}
{
    <UNIQUE> { pos = getPos(); } <LPAREN>
        columnName = SimpleIdentifier() { ukList.add(columnName); }
        (<COMMA> columnName = SimpleIdentifier() { ukList.add(columnName); })*
    <RPAREN>
    {
        SqlNodeList uk = new SqlNodeList(ukList, pos.plus(getPos()));
        list.add(uk);
    }
}

SqlNode PropertyValue() :
{
    SqlIdentifier key;
    SqlNode value;
    SqlParserPos pos;
}
{
    key = CompoundIdentifier()
    { pos = getPos(); }
    <EQ> value = StringLiteral()
    {
        return new SqlProperty(key, value, getPos());
    }
}

SqlCreate SqlCreateTable(Span s, boolean replace) :
{
    final SqlParserPos startPos = s.pos();
    SqlIdentifier tableName;
    SqlNodeList primaryKeyList = null;
    List<SqlNodeList> uniqueKeysList = null;
    SqlNodeList columnList = SqlNodeList.EMPTY;
	SqlCharStringLiteral comment = null;

    SqlNodeList propertyList = null;
    SqlNodeList partitionColumns = null;
    SqlParserPos pos = startPos;
}
{
    <TABLE>

    tableName = CompoundIdentifier()
    [
        <LPAREN> { pos = getPos(); TableCreationContext ctx = new TableCreationContext();}
        TableColumn(ctx)
        (
            <COMMA> TableColumn(ctx)
        )*
        {
            pos = pos.plus(getPos());
            columnList = new SqlNodeList(ctx.columnList, pos);
            primaryKeyList = ctx.primaryKeyList;
            uniqueKeysList = ctx.uniqueKeysList;
        }
        <RPAREN>
    ]
    [ <COMMENT> <QUOTED_STRING> {
        String p = SqlParserUtil.parseString(token.image);
        comment = SqlLiteral.createCharString(p, getPos());
    }]
    [
        <PARTITIONED> <BY>
            {
                SqlNode column;
                List<SqlNode> partitionKey = new ArrayList<SqlNode>();
                pos = getPos();

            }
            <LPAREN>
            [
                column = SimpleIdentifier()
                {
                    partitionKey.add(column);
                }
                (
                    <COMMA> column = SimpleIdentifier()
                        {
                            partitionKey.add(column);
                        }
                )*
            ]
            <RPAREN>
            {
                partitionColumns = new SqlNodeList(partitionKey, pos.plus(getPos()));
            }
    ]
    [
        <WITH>
            {
                SqlNode property;
                List<SqlNode> proList = new ArrayList<SqlNode>();
                pos = getPos();
            }
            <LPAREN>
            [
                property = PropertyValue()
                {
                proList.add(property);
                }
                (
                <COMMA> property = PropertyValue()
                    {
                    proList.add(property);
                    }
                )*
            ]
            <RPAREN>
        {  propertyList = new SqlNodeList(proList, pos.plus(getPos())); }
    ]

    {
        return new SqlCreateTable(startPos.plus(getPos()),
                tableName,
                columnList,
                primaryKeyList,
                uniqueKeysList,
                propertyList,
                partitionColumns,
                comment);
    }
}

private void FunctionJarDef(SqlNodeList usingList) :
{
SqlParserPos pos;
SqlNode uri;
}
{
    ( <JAR> | <FILE> | <ARCHIVE> )
    {
        pos = getPos();
        uri = StringLiteral();
        {
              usingList.add(uri);
        }
    }
}

SqlCreate SqlCreateFunction(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    final SqlNode className;
    SqlNodeList usingList = SqlNodeList.EMPTY;
}
{
    <FUNCTION> ifNotExists = IfNotExistsOpt()
               id = CompoundIdentifier()
    <AS>
        className = StringLiteral()
    [
        <USING> {
          usingList = new SqlNodeList(getPos());
        }
        FunctionJarDef(usingList)
        (
          <COMMA>
            FunctionJarDef(usingList)
        )*
    ] {
        return new SqlCreateFunction(s.end(this), replace, ifNotExists,
                            id, className, usingList);
    }
}
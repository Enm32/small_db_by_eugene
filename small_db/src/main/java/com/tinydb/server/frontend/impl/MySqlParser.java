package com.tinydb.server.frontend.impl;

import org.apache.shardingsphere.sql.parser.autogen.MySQLStatementParser;
import org.apache.shardingsphere.sql.parser.mysql.parser.MySQLLexer;
import com.tinydb.server.frontend.IParser;
import com.tinydb.server.frontend.common.domain.query_commands.QueryData;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;


public class MySqlParser implements IParser {
   MySqlStatementVisitor sqlStatementVisitor;

    public MySqlParser(String sql) {
        MySQLLexer lexer = new MySQLLexer(CharStreams.fromString(sql));
        MySQLStatementParser parser = new MySQLStatementParser(new CommonTokenStream(lexer));

        sqlStatementVisitor = new MySqlStatementVisitor(parser);
        sqlStatementVisitor.visit(parser.execute());
    }

    @Override
    public QueryData queryCmd() {
        return (QueryData) sqlStatementVisitor.getValue();
    }

    @Override
    public Object updateCmd() {
        return sqlStatementVisitor.getValue();
    }

}

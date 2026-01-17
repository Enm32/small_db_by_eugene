package com.tinydb.server.frontend.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.shardingsphere.sql.parser.autogen.MySQLStatementBaseVisitor;
import org.apache.shardingsphere.sql.parser.autogen.MySQLStatementParser;
import com.tinydb.server.frontend.common.domain.query_clause.*;
import com.tinydb.server.frontend.common.domain.query_commands.*;
import com.tinydb.server.query_engine.common.catalog.table.TableDefinition;

public class MySqlStatementVisitor extends MySQLStatementBaseVisitor {
      private final MySQLStatementParser parser;
    private COMMAND_TYPE commandType;

    // Common
    private String tableName;
    private a_Predicate predicate;

    // Select
    private final List<String> selectFields;

    // Index
    private String indexName;
    private String indexFieldName;

    // Insert
    private final List<String> insertFields;
    private final List<d_Constant> insertValues;

    // Modify
    private c_Expression updateFieldValue;
    private String updateFieldName;


    // Create Table
    private TableDefinition schema;

    public MySqlStatementVisitor(MySQLStatementParser parser) {
        this.parser = parser;

        this.tableName = "";
        this.predicate = new a_Predicate();

        this.selectFields = new ArrayList<>();

        this.indexName = "";
        this.indexFieldName = "";

        this.insertFields = new ArrayList<>();
        this.insertValues = new ArrayList<>();

        this.updateFieldName = "";

        this.schema = new TableDefinition();
    }

    // COMMAND TYPE
    @Override
    public Object visitCreateIndex(MySQLStatementParser.CreateIndexContext ctx) {
        commandType = COMMAND_TYPE.CREATE_INDEX;
        return super.visitCreateIndex(ctx);
    }

    @Override
    public Object visitCreateTable(MySQLStatementParser.CreateTableContext ctx) {
        commandType = COMMAND_TYPE.CREATE_TABLE;
        return super.visitCreateTable(ctx);
    }


    @Override
    public Object visitDelete(MySQLStatementParser.DeleteContext ctx) {
        commandType = COMMAND_TYPE.DELETE;
        return super.visitDelete(ctx);
    }

    @Override
    public Object visitInsert(MySQLStatementParser.InsertContext ctx) {
        commandType = COMMAND_TYPE.INSERT;
        return super.visitInsert(ctx);
    }

    @Override
    public Object visitUpdate(MySQLStatementParser.UpdateContext ctx) {
        commandType = COMMAND_TYPE.MODIFY;
        return super.visitUpdate(ctx);
    }

    @Override
    public Object visitSelect(MySQLStatementParser.SelectContext ctx) {
        commandType = COMMAND_TYPE.QUERY;
        return super.visitSelect(ctx);
    }

    // Command Attributes for Query & Delete

    @Override
    public Object visitTableName(MySQLStatementParser.TableNameContext ctx) {
        this.tableName = ctx.name().getText();
        return super.visitTableName(ctx);
    }


    @Override
    public Object visitProjection(MySQLStatementParser.ProjectionContext ctx) {
        this.selectFields.add(ctx.expr().getText());
        return super.visitProjection(ctx);
    }


    @Override
    public Object visitExpr(MySQLStatementParser.ExprContext ctx) {
        if (ctx.booleanPrimary() != null && ctx.booleanPrimary().comparisonOperator() != null && ctx.booleanPrimary().comparisonOperator() != null && ctx.booleanPrimary().comparisonOperator().getText().equals("=")) {
            b_term term = getTerm(ctx.booleanPrimary());
            predicate.conjoinWith(new a_Predicate(term));
        }
        return super.visitExpr(ctx);
    }

    private b_term getTerm(MySQLStatementParser.BooleanPrimaryContext term) {
        MySQLStatementParser.BooleanPrimaryContext lhs = term.booleanPrimary();
        MySQLStatementParser.PredicateContext rhs = term.predicate();

        c_Expression lhsExp = new c_Expression(lhs.getText());
        c_Expression rhsExp = null;

        if (rhs.bitExpr() != null && rhs.bitExpr(0).simpleExpr() != null && rhs.bitExpr(0).simpleExpr().literals() != null && rhs.bitExpr(0).simpleExpr().literals().numberLiterals() != null && !rhs.bitExpr(0).simpleExpr().literals().numberLiterals().isEmpty()) {
            // Number
            Integer num = Integer.parseInt(rhs.getText());
            rhsExp = new c_Expression(new d_Constant(num));
        } else if (rhs.bitExpr() != null && rhs.bitExpr(0).simpleExpr() != null && rhs.bitExpr(0).simpleExpr().literals() != null && rhs.bitExpr(0).simpleExpr().literals().stringLiterals() != null && !rhs.bitExpr(0).simpleExpr().literals().stringLiterals().isEmpty()) {
            // String
            rhsExp = new c_Expression(new d_Constant(rhs.getText()));
        }

        return new b_term(lhsExp, rhsExp);
    }

    // Command Attributes for CreateIndex
    @Override
    public Object visitIndexName(MySQLStatementParser.IndexNameContext ctx) {
        this.indexName = ctx.getText();
        return super.visitIndexName(ctx);
    }

    @Override
    public Object visitKeyPart(MySQLStatementParser.KeyPartContext ctx) {
        this.indexFieldName = ctx.getText();
        return super.visitKeyPart(ctx);
    }

    // Command Attributes for Insert & Part of Update
    @Override
    public Object visitInsertIdentifier(MySQLStatementParser.InsertIdentifierContext ctx) {
        this.insertFields.add(ctx.getText());
        return super.visitInsertIdentifier(ctx);
    }

    @Override
    public Object visitNumberLiterals(MySQLStatementParser.NumberLiteralsContext ctx) {
        this.insertValues.add(new d_Constant(Integer.parseInt(ctx.getText())));
        this.updateFieldValue = new c_Expression(new d_Constant(Integer.parseInt(ctx.getText())));

        return super.visitNumberLiterals(ctx);
    }

    @Override
    public Object visitStringLiterals(MySQLStatementParser.StringLiteralsContext ctx) {
        this.insertValues.add(new d_Constant(ctx.getText()));
        this.updateFieldValue = new c_Expression(new d_Constant(ctx.getText()));

        return super.visitStringLiterals(ctx);
    }

    // Command Attributes for Update
    @Override
    public Object visitAssignment(MySQLStatementParser.AssignmentContext ctx) {
        this.updateFieldName = ctx.columnRef().getText();
        return super.visitAssignment(ctx);
    }


    // Command Create Table
    @Override
    public Object visitColumnDefinition(MySQLStatementParser.ColumnDefinitionContext ctx) {
        String fieldName = ctx.column_name.getText();
        String dataType = ctx.fieldDefinition().dataType().getText();
        if (dataType.equals("int")) {
            schema.addIntField(fieldName);
        } else if (dataType.startsWith("varchar")) {
            dataType = dataType.substring("varchar".length());
            dataType = dataType.replace("(", "");
            dataType = dataType.replace(")", "");
            int length = Integer.parseInt(dataType);
            schema.addStringField(fieldName, length);
        } else {
            throw new RuntimeException("Unsupported Column Type");
        }
        return super.visitColumnDefinition(ctx);
    }

    public Object getValue() {
        switch (commandType) {
            case QUERY:
                return new QueryData(selectFields, tableName, predicate);
            case DELETE:
                return new DeleteData(tableName, predicate);
            case CREATE_INDEX:
                return new CreateIndexData(indexName, tableName, indexFieldName);
            case INSERT:
                return new InsertData(tableName, insertFields, insertValues);
            case MODIFY:
                return new ModifyData(tableName, updateFieldName, updateFieldValue, predicate);
            case CREATE_TABLE:
                return new CreateTableData(tableName, schema);
        }
        return new QueryData(selectFields, tableName, predicate);
    }


    enum COMMAND_TYPE {
        QUERY, MODIFY, INSERT, DELETE, CREATE_TABLE, CREATE_INDEX
    }
}

package com.tinydb.server.frontend.common.domain.query_commands;

import com.tinydb.server.query_engine.common.catalog.table.TableDefinition;

import lombok.ToString;

@ToString
public class CreateTableData {
    private String tblname;
    private TableDefinition sch;

  
    public CreateTableData(String tblname, TableDefinition sch) {
        this.tblname = tblname;
        this.sch = sch;
    }

   
    public String tableName() {
        return tblname;
    }

  
    public TableDefinition newSchema() {
        return sch;
    }
}

package com.tinydb.server.frontend.common.domain.query_commands;

import java.util.List;

import com.tinydb.server.frontend.common.domain.query_clause.d_Constant;

import lombok.ToString;


@ToString
public class InsertData {
    private String tblname;
    private List<String> flds;
    private List<d_Constant> vals;

  
    public InsertData(String tblname, List<String> flds, List<d_Constant> vals) {
        this.tblname = tblname;
        this.flds = flds;
        this.vals = vals;
    }

   
    public String tableName() {
        return tblname;
    }

   
    public List<String> fields() {
        return flds;
    }

  
    public List<d_Constant> vals() {
        return vals;
    }
}

package com.tinydb.server.frontend.common.domain.query_commands;
import com.tinydb.server.frontend.common.domain.query_clause.a_Predicate;

import lombok.ToString;

@ToString
public class DeleteData {
    private String tblname;
    private a_Predicate pred;

    public DeleteData(String tblname, a_Predicate pred) {
        this.tblname = tblname;
        this.pred = pred;
    }

   
    public String tableName() {
        return tblname;
    }

    
    public a_Predicate pred() {
        return pred;
    }
}

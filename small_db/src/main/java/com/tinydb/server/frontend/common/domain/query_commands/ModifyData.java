package com.tinydb.server.frontend.common.domain.query_commands;



import com.tinydb.server.frontend.common.domain.query_clause.c_Expression;
import com.tinydb.server.frontend.common.domain.query_clause.a_Predicate;

import lombok.ToString;

@ToString
public class ModifyData {
 private String tblname;
    private String fldname;
    private c_Expression newval;
    private  a_Predicate pred;

  
    public ModifyData(String tblname, String fldname, c_Expression newval, a_Predicate pred) {
        this.tblname = tblname;
        this.fldname = fldname;
        this.newval = newval;
        this.pred = pred;
    }

  
    public String tableName() {
        return tblname;
    }

   
    
    public String targetField() {
        return fldname;
    }

 
    public c_Expression newValue() {
        return newval;
    }

   
    public a_Predicate pred() {
        return pred;
    }
}

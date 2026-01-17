package com.tinydb.server.frontend.common.domain.query_clause;

import com.tinydb.server.query_engine.common.catalog.table.TableDefinition;
import com.tinydb.server.storage_engine.RORecordScan;




public class c_Expression {
 private d_Constant val = null;
    private String fldname = null;

    public c_Expression(d_Constant val) {
        this.val = val;
    }

    public c_Expression(String fldname) {
        this.fldname = fldname;
    }

   
    public d_Constant evaluate(RORecordScan s) {
        return (val != null) ? val : s.getVal(fldname);
    }

   
    public boolean isFieldName() {
        return fldname != null;
    }

   
    public d_Constant asConstant() {
        return val;
    }

   
    public String asFieldName() {
        return fldname;
    }

    public boolean appliesTo(TableDefinition sch) {
        return (val != null) ? true : sch.hasField(fldname);
    }

    public String toString() {
        return (val != null) ? val.toString() : fldname;
    }
}

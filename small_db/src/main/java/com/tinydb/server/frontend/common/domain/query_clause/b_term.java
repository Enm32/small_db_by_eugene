package com.tinydb.server.frontend.common.domain.query_clause;


import com.tinydb.server.query_engine.common.catalog.table.TableDefinition;
import com.tinydb.server.storage_engine.RORecordScan;


public class b_term {
 private c_Expression lhs, rhs;

  
    public b_term(c_Expression lhs, c_Expression rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

   
    public boolean isSatisfied(RORecordScan s) {
        d_Constant lhsval = lhs.evaluate(s);
        d_Constant rhsval = rhs.evaluate(s);
        return rhsval.equals(lhsval);
    }
     
    public d_Constant equatesWithConstant(String fldname) {
        if (lhs.isFieldName() &&
                lhs.asFieldName().equals(fldname) &&
                !rhs.isFieldName())
            return rhs.asConstant();
        else if (rhs.isFieldName() &&
                rhs.asFieldName().equals(fldname) &&
                !lhs.isFieldName())
            return lhs.asConstant();
        else
            return null;
    }

   
    public String equatesWithField(String fldname) {
        if (lhs.isFieldName() &&
                lhs.asFieldName().equals(fldname) &&
                rhs.isFieldName())
            return rhs.asFieldName();
        else if (rhs.isFieldName() &&
                rhs.asFieldName().equals(fldname) &&
                lhs.isFieldName())
            return lhs.asFieldName();
        else
            return null;
    }

  
    public boolean appliesTo(TableDefinition sch) {
        return lhs.appliesTo(sch) && rhs.appliesTo(sch);
    }

    public String toString() {
        return lhs.toString() + "=" + rhs.toString();
    }
}

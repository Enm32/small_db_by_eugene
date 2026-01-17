package com.tinydb.server.frontend.common.domain.query_clause;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import com.tinydb.server.storage_engine.RORecordScan;

public class a_Predicate {
 private List<b_term> terms = new ArrayList<b_term>();

   
    public a_Predicate() {
    }

  
    public a_Predicate(b_term t) {
        terms.add(t);
    }

  
    public void conjoinWith(a_Predicate pred) {
        terms.addAll(pred.terms);
    }

  
    public boolean isSatisfied(RORecordScan s) {
        for (b_term t : terms)
            if (!t.isSatisfied(s)) return false;
        return true;
    }


    public d_Constant equatesWithConstant(String fldname) {
        for (b_term t : terms) {
            d_Constant c = t.equatesWithConstant(fldname);
            if (c != null)
                return c;
        }
        return null;
    }

    public String toString() {
        Iterator<b_term> iter = terms.iterator();
        if (!iter.hasNext()) return "";
        String result = iter.next().toString();
        while (iter.hasNext()) result += " and " + iter.next().toString();
        return result;
    }
}

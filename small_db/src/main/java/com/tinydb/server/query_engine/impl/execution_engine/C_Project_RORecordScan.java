package com.tinydb.server.query_engine.impl.execution_engine;

import java.util.List;
import com.tinydb.server.frontend.common.domain.query_clause.d_Constant;
import com.tinydb.server.storage_engine.RORecordScan;

public class C_Project_RORecordScan implements RORecordScan{
       private final RORecordScan s;
    private final List<String> fieldlist;

    public C_Project_RORecordScan(RORecordScan s, List<String> fieldlist) {
        this.s = s;
        this.fieldlist = fieldlist;
    }

    public void seekToQueryStart() {
        s.seekToQueryStart();
    }

    public boolean next() {
        return s.next();
    }

    public int getInt(String fldname) {
        if (hasField(fldname))
            return s.getInt(fldname);
        else
            throw new RuntimeException("field " + fldname + " not found.");
    }

    public String getString(String fldname) {
        if (hasField(fldname))
            return s.getString(fldname);
        else
            throw new RuntimeException("field " + fldname + " not found.");
    }

    public d_Constant getVal(String fldname) {
        if (hasField(fldname))
            return s.getVal(fldname);
        else
            throw new RuntimeException("field " + fldname + " not found.");
    }

    public boolean hasField(String fldname) {
        return fieldlist.contains(fldname);
    }

    public void close() {
        s.close();
    }


}

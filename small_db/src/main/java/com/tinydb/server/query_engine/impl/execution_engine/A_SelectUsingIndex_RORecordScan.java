package com.tinydb.server.query_engine.impl.execution_engine;

import com.tinydb.server.storage_engine.RORecordScan;
import com.tinydb.server.storage_engine.RWIndexScan;
import com.tinydb.server.storage_engine.impl.HeapRWRecordScan;
import com.tinydb.server.storage_engine.impl.RecordKey;
import com.tinydb.server.frontend.common.domain.query_clause.d_Constant;


public class A_SelectUsingIndex_RORecordScan implements RORecordScan {
    private final HeapRWRecordScan ts;
    private final RWIndexScan idx;

    private final d_Constant val;


    public A_SelectUsingIndex_RORecordScan(HeapRWRecordScan ts, RWIndexScan idx, d_Constant val) {
        this.ts = ts;
        this.idx = idx;
        this.val = val;
        seekToQueryStart();
    }

    public void seekToQueryStart() {
        idx.seek(val);
    }

    public boolean next() {
        boolean ok = idx.hasNext();
        if (ok) {
            RecordKey recordKey = idx.next();
            ts.seekTo(recordKey);
        }
        return ok;
    }


    public int getInt(String fldname) {
        return ts.getInt(fldname);
    }

    public String getString(String fldname) {
        return ts.getString(fldname);
    }


    public d_Constant getVal(String fldname) {
        return ts.getVal(fldname);
    }


    public boolean hasField(String fldname) {
        return ts.hasField(fldname);
    }


    public void close() {
        idx.close();
        ts.close();
    }
}

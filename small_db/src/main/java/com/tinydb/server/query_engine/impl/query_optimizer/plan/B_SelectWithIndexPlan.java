package com.tinydb.server.query_engine.impl.query_optimizer.plan;

import com.tinydb.server.query_engine.impl.query_optimizer.plan.Plan;
import com.tinydb.server.query_engine.common.catalog.index.IndexInfo;
import com.tinydb.server.query_engine.common.catalog.table.TableDefinition;
import com.tinydb.server.query_engine.impl.execution_engine.A_SelectUsingIndex_RORecordScan;
import com.tinydb.server.storage_engine.RORecordScan;
import com.tinydb.server.storage_engine.RWIndexScan;
import com.tinydb.server.storage_engine.impl.HeapRWRecordScan;
import com.tinydb.server.frontend.common.domain.query_clause.d_Constant;

public class B_SelectWithIndexPlan implements Plan {
    private Plan p;
    private IndexInfo ii;
    private d_Constant val;


    public B_SelectWithIndexPlan(Plan p, IndexInfo ii, d_Constant val) {
        this.p = p;
        this.ii = ii;
        this.val = val;
    }


    public RORecordScan open() {
        // throws an exception if p is not a tableplan.
        HeapRWRecordScan scan = (HeapRWRecordScan) p.open();
        RWIndexScan idx = ii.open();
        return new A_SelectUsingIndex_RORecordScan(scan, idx, val);
    }

    public TableDefinition schema() {
        return p.schema();
    }

    @Override
    public int blocksAccessed() {
        return ii.blocksAccessed() + ii.recordsOutput();
    }


}

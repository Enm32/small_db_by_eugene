package com.tinydb.server.query_engine.impl.query_optimizer.plan;

import com.tinydb.server.query_engine.impl.query_optimizer.plan.Plan;
import com.tinydb.server.query_engine.common.catalog.table.TableDefinition;
import com.tinydb.server.query_engine.impl.execution_engine.A_Select_RWRecordScan;
import com.tinydb.server.storage_engine.RORecordScan;
import  com.tinydb.server.frontend.common.domain.query_clause.a_Predicate;

public class B_SelectPlan implements Plan {
 private Plan p;
    private a_Predicate pred;

    public B_SelectPlan(Plan p, a_Predicate pred) {
        this.p = p;
        this.pred = pred;
    }

    public RORecordScan open() {
        RORecordScan s = p.open();
        return new A_Select_RWRecordScan(s, pred);
    }


    public TableDefinition schema() {
        return p.schema();
    }

    @Override
    public int blocksAccessed() {
        return p.blocksAccessed();
    }
}

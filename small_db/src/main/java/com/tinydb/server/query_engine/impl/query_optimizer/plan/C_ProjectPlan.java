package com.tinydb.server.query_engine.impl.query_optimizer.plan;

import java.util.List;
import com.tinydb.server.query_engine.common.catalog.table.TableDefinition;
import com.tinydb.server.query_engine.impl.execution_engine.C_Project_RORecordScan;
import com.tinydb.server.storage_engine.RORecordScan;
import com.tinydb.server.query_engine.impl.query_optimizer.plan.Plan;

public class C_ProjectPlan implements Plan{
    private Plan p;
    private TableDefinition tableDefinition = new TableDefinition();


    public C_ProjectPlan(Plan p, List<String> fieldlist) {
        this.p = p;
        for (String fldname : fieldlist)
            tableDefinition.add(fldname, p.schema());
    }


    public RORecordScan open() {
        RORecordScan s = p.open();
        return new C_Project_RORecordScan(s, tableDefinition.fields());
    }

    public TableDefinition schema() {
        return tableDefinition;
    }

    @Override
    public int blocksAccessed() {
        return p.blocksAccessed();
    }
}

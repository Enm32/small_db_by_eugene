package com.tinydb.server.query_engine.impl.query_optimizer.planner.a_naive;

import com.tinydb.server.frontend.common.domain.query_commands.QueryData;
import com.tinydb.server.query_engine.common.catalog.MetadataMgr;
import com.tinydb.server.query_engine.impl.query_optimizer.plan.A_TablePlan;
import com.tinydb.server.query_engine.impl.query_optimizer.plan.B_SelectPlan;
import com.tinydb.server.query_engine.impl.query_optimizer.plan.C_ProjectPlan;
import com.tinydb.server.query_engine.impl.query_optimizer.plan.Plan;
import com.tinydb.server.query_engine.impl.query_optimizer.planner.QueryPlanner;
import com.tinydb.server.storage_engine.common.transaction.Transaction;

public class BasicQueryPlanner implements QueryPlanner{
    private MetadataMgr mdm;

    public BasicQueryPlanner(MetadataMgr mdm) {
        this.mdm = mdm;
    }

    public Plan createPlan(QueryData data, Transaction tx) {
        //Step 1: Create the plan
        Plan p = new A_TablePlan(tx, data.table(), mdm);

        //Step 3: Add a selection plan for the predicate
        p = new B_SelectPlan(p, data.pred());

        //Step 4: Project on the field names
        p = new C_ProjectPlan(p, data.fields());
        return p;
    }
}

package com.tinydb.server.query_engine.impl.query_optimizer.planner.b_rule_base;

import java.util.Map;

import com.tinydb.server.frontend.common.domain.query_clause.d_Constant;
import com.tinydb.server.frontend.common.domain.query_commands.QueryData;
import com.tinydb.server.query_engine.common.catalog.MetadataMgr;
import com.tinydb.server.query_engine.common.catalog.index.IndexInfo;
import com.tinydb.server.query_engine.impl.query_optimizer.plan.A_TablePlan;
import com.tinydb.server.query_engine.impl.query_optimizer.plan.B_SelectPlan;
import com.tinydb.server.query_engine.impl.query_optimizer.plan.B_SelectWithIndexPlan;
import com.tinydb.server.query_engine.impl.query_optimizer.plan.C_ProjectPlan;
import com.tinydb.server.query_engine.impl.query_optimizer.plan.Plan;
import com.tinydb.server.query_engine.impl.query_optimizer.planner.QueryPlanner;
import com.tinydb.server.storage_engine.common.transaction.Transaction;

public class b_rule_query_planner implements QueryPlanner {
    private MetadataMgr mdm;

    public b_rule_query_planner(MetadataMgr mdm) {
        this.mdm = mdm;
    }


    public Plan createPlan(QueryData data, Transaction tx) {

        
        Plan p = new A_TablePlan(tx, data.table(), mdm);

        boolean indexFound = false;
        Map<String, IndexInfo> indexes = mdm.getIndexInfo(data.table(), tx);
        for (String columnName : indexes.keySet()) {
            d_Constant columnValue = data.pred().equatesWithConstant(columnName);
            if (columnValue != null) {
                IndexInfo columnIndexInfo = indexes.get(columnName);
                p = new B_SelectWithIndexPlan(p, columnIndexInfo, columnValue);

                indexFound = true;
                System.out.println("index on " + columnName + " used");
                break;
            }
        }

        if (!indexFound) p = new B_SelectPlan(p, data.pred());

        p = new C_ProjectPlan(p, data.fields());
        return p;
    }
}

package com.tinydb.server.query_engine.impl.query_optimizer.planner;

import com.tinydb.server.frontend.common.domain.query_commands.QueryData;
import com.tinydb.server.query_engine.impl.query_optimizer.plan.Plan;
import com.tinydb.server.storage_engine.common.transaction.Transaction;

public interface QueryPlanner {
    Plan createPlan(QueryData data, Transaction tx);
}

package com.tinydb.server.query_engine.impl.query_optimizer;

import com.tinydb.server.frontend.IParser;
import com.tinydb.server.frontend.common.domain.query_commands.CreateIndexData;
import com.tinydb.server.frontend.common.domain.query_commands.CreateTableData;
import com.tinydb.server.frontend.common.domain.query_commands.DeleteData;
import com.tinydb.server.frontend.common.domain.query_commands.InsertData;
import com.tinydb.server.frontend.common.domain.query_commands.ModifyData;
import com.tinydb.server.frontend.common.domain.query_commands.QueryData;
import com.tinydb.server.frontend.impl.MySqlParser;
import com.tinydb.server.query_engine.impl.query_optimizer.plan.Plan;
import com.tinydb.server.query_engine.impl.query_optimizer.planner.QueryPlanner;
import com.tinydb.server.query_engine.impl.query_optimizer.planner.UpdatePlanner;
import com.tinydb.server.storage_engine.common.transaction.Transaction;

public class BasicPlanner {
    private final QueryPlanner queryPlanner;
    private final UpdatePlanner updatePlanner;

    public BasicPlanner(QueryPlanner queryPlanner, UpdatePlanner updatePlanner) {
        this.queryPlanner = queryPlanner;
        this.updatePlanner = updatePlanner;
    }

    public Plan createQueryPlan(String qry, Transaction tx) {
        IParser parser = new MySqlParser(qry);
        QueryData data = parser.queryCmd();
//        System.out.println(data);
        return queryPlanner.createPlan(data, tx);
    }

    public int executeUpdate(String cmd, Transaction tx) {
        IParser parser = new MySqlParser(cmd);
        Object data = parser.updateCmd();
//        System.out.println(data);
        if (data instanceof InsertData) return updatePlanner.executeInsert((InsertData) data, tx);
        else if (data instanceof DeleteData) return updatePlanner.executeDelete((DeleteData) data, tx);
        else if (data instanceof ModifyData) return updatePlanner.executeModify((ModifyData) data, tx);
        else if (data instanceof CreateTableData) return updatePlanner.executeCreateTable((CreateTableData) data, tx);
        else if (data instanceof CreateIndexData) return updatePlanner.executeCreateIndex((CreateIndexData) data, tx);
        else return 0;
    }

}

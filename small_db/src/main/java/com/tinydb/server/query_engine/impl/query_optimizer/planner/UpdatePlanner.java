package com.tinydb.server.query_engine.impl.query_optimizer.planner;

import com.tinydb.server.frontend.common.domain.query_commands.CreateIndexData;
import com.tinydb.server.frontend.common.domain.query_commands.CreateTableData;
import com.tinydb.server.frontend.common.domain.query_commands.DeleteData;
import com.tinydb.server.frontend.common.domain.query_commands.InsertData;
import com.tinydb.server.frontend.common.domain.query_commands.ModifyData;
import com.tinydb.server.storage_engine.common.transaction.Transaction;

public interface UpdatePlanner {
          int executeCreateTable(CreateTableData data, Transaction tx);

    int executeCreateIndex(CreateIndexData data, Transaction tx);

    int executeInsert(InsertData data, Transaction tx);

    int executeModify(ModifyData data, Transaction tx);

    int executeDelete(DeleteData data, Transaction tx);

}

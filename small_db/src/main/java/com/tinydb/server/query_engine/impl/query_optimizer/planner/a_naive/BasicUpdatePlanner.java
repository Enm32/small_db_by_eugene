package com.tinydb.server.query_engine.impl.query_optimizer.planner.a_naive;

import com.tinydb.server.frontend.common.domain.query_clause.d_Constant;
import com.tinydb.server.frontend.common.domain.query_commands.CreateIndexData;
import com.tinydb.server.frontend.common.domain.query_commands.CreateTableData;
import com.tinydb.server.frontend.common.domain.query_commands.DeleteData;
import com.tinydb.server.frontend.common.domain.query_commands.InsertData;
import com.tinydb.server.frontend.common.domain.query_commands.ModifyData;
import com.tinydb.server.query_engine.common.catalog.MetadataMgr;
import com.tinydb.server.query_engine.impl.query_optimizer.plan.A_TablePlan;
import com.tinydb.server.query_engine.impl.query_optimizer.plan.B_SelectPlan;
import com.tinydb.server.query_engine.impl.query_optimizer.plan.Plan;
import com.tinydb.server.query_engine.impl.query_optimizer.planner.UpdatePlanner;
import com.tinydb.server.storage_engine.RWRecordScan;
import com.tinydb.server.storage_engine.common.transaction.Transaction;
import java.util.Iterator;

public class BasicUpdatePlanner implements UpdatePlanner{
  private MetadataMgr mdm;

    public BasicUpdatePlanner(MetadataMgr mdm) {
        this.mdm = mdm;
    }

    public int executeCreateTable(CreateTableData data, Transaction tx) {
        mdm.createTable(data.tableName(), data.newSchema(), tx);
        return 0;
    }

    public int executeCreateIndex(CreateIndexData data, Transaction tx) {
        mdm.createIndex(data.indexName(), data.tableName(), data.fieldName(), tx);
        return 0;
    }

    public int executeInsert(InsertData data, Transaction tx) {
        Plan p = new A_TablePlan(tx, data.tableName(), mdm);
        RWRecordScan scan = (RWRecordScan) p.open();
        scan.seekToInsertStart();
        Iterator<d_Constant> iter = data.vals().iterator();
        for (String fldname : data.fields()) {
            d_Constant val = iter.next();
            scan.setVal(fldname, val);
        }
        scan.close();
        return 1;
    }


    public int executeModify(ModifyData data, Transaction tx) {
        Plan p = new A_TablePlan(tx, data.tableName(), mdm);
        p = new B_SelectPlan(p, data.pred());
        RWRecordScan scan = (RWRecordScan) p.open();
        int count = 0;
        while (scan.next()) {
            d_Constant val = data.newValue().evaluate(scan);
            scan.setVal(data.targetField(), val);
            count++;
        }
        scan.close();
        return count;
    }

    public int executeDelete(DeleteData data, Transaction tx) {
        Plan p = new A_TablePlan(tx, data.tableName(), mdm);
        p = new B_SelectPlan(p, data.pred());
        RWRecordScan us = (RWRecordScan) p.open();
        int count = 0;
        while (us.next()) {
            us.delete();
            count++;
        }
        us.close();
        return count;
    }
}

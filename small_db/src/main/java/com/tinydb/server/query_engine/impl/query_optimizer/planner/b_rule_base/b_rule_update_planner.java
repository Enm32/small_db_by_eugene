package com.tinydb.server.query_engine.impl.query_optimizer.planner.b_rule_base;

import java.util.Map;
import java.util.Iterator;
import com.tinydb.server.frontend.common.domain.query_clause.d_Constant;
import com.tinydb.server.frontend.common.domain.query_commands.CreateIndexData;
import com.tinydb.server.frontend.common.domain.query_commands.CreateTableData;
import com.tinydb.server.frontend.common.domain.query_commands.DeleteData;
import com.tinydb.server.frontend.common.domain.query_commands.InsertData;
import com.tinydb.server.frontend.common.domain.query_commands.ModifyData;
import com.tinydb.server.query_engine.common.catalog.MetadataMgr;
import com.tinydb.server.query_engine.common.catalog.index.IndexInfo;
import com.tinydb.server.query_engine.impl.query_optimizer.plan.A_TablePlan;
import com.tinydb.server.query_engine.impl.query_optimizer.plan.B_SelectPlan;
import com.tinydb.server.query_engine.impl.query_optimizer.plan.Plan;
import com.tinydb.server.query_engine.impl.query_optimizer.planner.UpdatePlanner;
import com.tinydb.server.storage_engine.RWIndexScan;
import com.tinydb.server.storage_engine.RWRecordScan;
import com.tinydb.server.storage_engine.common.transaction.Transaction;
import com.tinydb.server.storage_engine.impl.RecordKey;


public class b_rule_update_planner implements UpdatePlanner {
       private MetadataMgr mdm;

    public b_rule_update_planner(MetadataMgr mdm) {
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
        String tblname = data.tableName();
        Plan p = new A_TablePlan(tx, tblname, mdm);

        // first, insert the record
        RWRecordScan scan = (RWRecordScan) p.open();
        scan.seekToInsertStart();

        // then modify each field, inserting an index record if appropriate
        RecordKey recordKey = scan.getRid();
        Map<String, IndexInfo> indexes = mdm.getIndexInfo(tblname, tx);
        Iterator<d_Constant> valIter = data.vals().iterator();
        for (String fldname : data.fields()) {
            d_Constant val = valIter.next();
            scan.setVal(fldname, val);

            IndexInfo ii = indexes.get(fldname);
            if (ii != null) {
                RWIndexScan idx = ii.open();
                idx.insert(val, recordKey);
                idx.close();
            }
        }
        scan.close();
        return 1;
    }

    public int executeDelete(DeleteData data, Transaction tx) {
        String tblname = data.tableName();
        Plan p = new A_TablePlan(tx, tblname, mdm);
        p = new B_SelectPlan(p, data.pred());
        Map<String, IndexInfo> indexes = mdm.getIndexInfo(tblname, tx);

        RWRecordScan s = (RWRecordScan) p.open();
        int count = 0;
        while (s.next()) {
            // first, delete the record's RID from every index
            RecordKey recordKey = s.getRid();
            for (String fldname : indexes.keySet()) {
                d_Constant val = s.getVal(fldname);
                RWIndexScan idx = indexes.get(fldname).open();
                idx.delete(val, recordKey);
                idx.close();
            }
            // then delete the record
            s.delete();
            count++;
        }
        s.close();
        return count;
    }

    public int executeModify(ModifyData data, Transaction tx) {
        String tblname = data.tableName();
        String fldname = data.targetField();
        Plan p = new A_TablePlan(tx, tblname, mdm);
        p = new B_SelectPlan(p, data.pred());

        IndexInfo ii = mdm.getIndexInfo(tblname, tx).get(fldname);
        RWIndexScan idx = (ii == null) ? null : ii.open();

        RWRecordScan s = (RWRecordScan) p.open();
        int count = 0;
        while (s.next()) {
            // first, update the record
            d_Constant newval = data.newValue().evaluate(s);
            d_Constant oldval = s.getVal(fldname);
            s.setVal(data.targetField(), newval);

            // then update the appropriate index, if it exists
            if (idx != null) {
                RecordKey recordKey = s.getRid();
                idx.delete(oldval, recordKey);
                idx.insert(newval, recordKey);
            }
            count++;
        }
        if (idx != null) idx.close();
        s.close();
        return count;
    }





}

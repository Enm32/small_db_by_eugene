package com.tinydb.server.query_engine.impl;

import java.util.ArrayList;
import java.util.List;
import java.io.File;
import com.tinydb.server.query_engine.IQueryEngine;
import com.tinydb.server.query_engine.common.catalog.MetadataMgr;
import com.tinydb.server.query_engine.common.dto.TableDto;
import com.tinydb.server.query_engine.impl.query_optimizer.BasicPlanner;
import com.tinydb.server.query_engine.impl.query_optimizer.plan.Plan;
import com.tinydb.server.query_engine.impl.query_optimizer.planner.QueryPlanner;
import com.tinydb.server.query_engine.impl.query_optimizer.planner.UpdatePlanner;
import com.tinydb.server.query_engine.impl.query_optimizer.planner.b_rule_base.b_rule_query_planner;
import com.tinydb.server.query_engine.impl.query_optimizer.planner.b_rule_base.b_rule_update_planner;
import com.tinydb.server.storage_engine.RORecordScan;
import com.tinydb.server.storage_engine.common.file.FileMgr;
import com.tinydb.server.storage_engine.common.transaction.Transaction;

public class BasicQueryEngine implements IQueryEngine {
    public static int BLOCK_SIZE = 512;

    private final FileMgr fm;
    private final BasicPlanner planner;

    public BasicQueryEngine(String dirname) {
        File dbDirectory = new File(dirname);
        fm = new FileMgr(dbDirectory, BLOCK_SIZE);
        Transaction tx = newTx();

        MetadataMgr mdm = new MetadataMgr(fm.isNew(), tx);
        QueryPlanner qp = new b_rule_query_planner(mdm);
        UpdatePlanner up = new b_rule_update_planner(mdm);
        planner = new BasicPlanner(qp, up);

        tx.commit();
    }


    public TableDto doQuery(String sql) {
        Transaction tx = newTx();
        Plan p = planner.createQueryPlan(sql, tx);
        RORecordScan s = p.open();

        List<String> columnNames = p.schema().fields();

        List<List<String>> rows = new ArrayList<>();
        while (s.next()) {
            List<String> row = new ArrayList<>();
            for (String field : columnNames) row.add(s.getVal(field).toString());
            rows.add(row);
        }

        s.close();
        tx.commit();

        return new TableDto(columnNames, rows);
    }


    public TableDto doUpdate(String sql) {
        Transaction tx = newTx();
        int updatedRows = planner.executeUpdate(sql, tx);
        tx.commit();

        String message = updatedRows + " " + (updatedRows == 1 ? "row" : "rows") + " updated.";
        return new TableDto(message);
    }

    public void close() {
        System.out.println("Shutting down");
    }

    private Transaction newTx() {
        return new Transaction(fm);
    }

    public FileMgr fileMgr() {
        return fm;
    }
}

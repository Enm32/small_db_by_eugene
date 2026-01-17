package com.tinydb.server.query_engine.common.catalog;

import java.util.Map;

import com.tinydb.server.query_engine.common.catalog.index.IndexInfo;
import com.tinydb.server.query_engine.common.catalog.index.IndexMgr;
import com.tinydb.server.query_engine.common.catalog.stats.StatInfo;
import com.tinydb.server.query_engine.common.catalog.stats.StatMgr;
import com.tinydb.server.query_engine.common.catalog.table.TableDefinition;
import com.tinydb.server.query_engine.common.catalog.table.TableMgr;
import com.tinydb.server.query_engine.common.catalog.table.TablePhysicalLayout;
import com.tinydb.server.storage_engine.common.transaction.Transaction;



public class MetadataMgr {
      private static TableMgr tblmgr;
    private static IndexMgr idxmgr;
    private static StatMgr statmgr;

    public MetadataMgr(boolean isnew, Transaction tx) {
        tblmgr = new TableMgr(isnew, tx);
        statmgr = new StatMgr(tblmgr, tx);
        idxmgr = new IndexMgr(isnew, tblmgr, statmgr, tx);
    }

    public void createTable(String tblname, TableDefinition sch, Transaction tx) {
        tblmgr.createTable(tblname, sch, tx);
    }

    public TablePhysicalLayout getLayout(String tblname, Transaction tx) {
        return tblmgr.getLayout(tblname, tx);
    }


    public void createIndex(String idxname, String tblname, String fldname, Transaction tx) {
        idxmgr.createIndex(idxname, tblname, fldname, tx);
    }

    public Map<String, IndexInfo> getIndexInfo(String tblname, Transaction tx) {
        return idxmgr.getIndexInfo(tblname, tx);
    }

    public StatInfo getStatInfo(String tblname, TablePhysicalLayout layout, Transaction tx) {
        return statmgr.getStatInfo(tblname, layout, tx);
    }
}

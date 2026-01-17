package com.tinydb.server.query_engine.common.catalog.stats;

import java.util.HashMap;
import java.util.Map;
import com.tinydb.server.query_engine.common.catalog.stats.StatInfo;
import com.tinydb.server.query_engine.common.catalog.table.TableMgr;
import com.tinydb.server.query_engine.common.catalog.table.TablePhysicalLayout;
import com.tinydb.server.storage_engine.common.transaction.Transaction;


public class StatMgr {
       private int numcalls;
    private Map<String, StatInfo> tablestats;
    private TableMgr tblMgr;

    public StatMgr(TableMgr tblMgr, Transaction tx) {
        this.tblMgr = tblMgr;
        refreshStatistics(tx);
    }

    public synchronized StatInfo getStatInfo(String tblname, TablePhysicalLayout layout, Transaction tx) {
        numcalls++;
        if (numcalls > 100) refreshStatistics(tx);
        StatInfo si = tablestats.get(tblname);
        if (si == null) {
            si = calcTableStats(tblname, layout, tx);
            tablestats.put(tblname, si);
        }
        return si;
    }

    private synchronized void refreshStatistics(Transaction tx) {
        tablestats = new HashMap<String, StatInfo>();
    }

    private synchronized StatInfo calcTableStats(String tblname, TablePhysicalLayout layout, Transaction tx) {
        return null;
    }
}

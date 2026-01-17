package com.tinydb.server.query_engine.common.catalog.index;

import java.util.HashMap;
import java.util.Map;

import com.tinydb.server.query_engine.common.catalog.stats.StatInfo;
import com.tinydb.server.query_engine.common.catalog.stats.StatMgr;
import com.tinydb.server.query_engine.common.catalog.table.TableDefinition;
import com.tinydb.server.query_engine.common.catalog.table.TableMgr;
import com.tinydb.server.query_engine.common.catalog.table.TablePhysicalLayout;
import com.tinydb.server.storage_engine.common.transaction.Transaction;
import com.tinydb.server.storage_engine.impl.HeapRWRecordScan;


public class IndexMgr {
  private TablePhysicalLayout recordValueLayout;
  private TableMgr tblmgr;
  private StatMgr statmgr;

  public IndexMgr(boolean isnew, TableMgr tblmgr, StatMgr statmgr, Transaction tx) {
    if (isnew) {
      TableDefinition sch = new TableDefinition();
      sch.addStringField("indexname", TableMgr.MAX_NAME);
      sch.addStringField("tablename", TableMgr.MAX_NAME);
      sch.addStringField("fieldname", TableMgr.MAX_NAME);

      tblmgr.createTable("idxcat", sch, tx);
    }
    this.tblmgr = tblmgr;
    this.statmgr = statmgr;
    recordValueLayout = tblmgr.getLayout("idxcat", tx);
  }


  public void createIndex(String idxname, String tblname, String fldname, Transaction tx) {
    HeapRWRecordScan ts = new HeapRWRecordScan(tx, "idxcat", recordValueLayout);
    ts.seekToInsertStart();
    ts.setString("indexname", idxname);
    ts.setString("tablename", tblname);
    ts.setString("fieldname", fldname);
    ts.close();
  }

 
  public Map<String, IndexInfo> getIndexInfo(String tblname, Transaction tx) {
    Map<String, IndexInfo> result = new HashMap<String, IndexInfo>();
    HeapRWRecordScan ts = new HeapRWRecordScan(tx, "idxcat", recordValueLayout);
    while (ts.next()) {
      if (ts.getString("tablename").equals(tblname)) {
        String idxname = ts.getString("indexname");
        String fldname = ts.getString("fieldname");
        TablePhysicalLayout tblRecordValueLayout = tblmgr.getLayout(tblname, tx);
        StatInfo tblsi = statmgr.getStatInfo(tblname, tblRecordValueLayout, tx);

        IndexInfo ii = new IndexInfo(idxname, fldname, tblRecordValueLayout.schema(), tx, tblsi);
        result.put(fldname, ii);
      }
    }
    ts.close();
    return result;
  }

}

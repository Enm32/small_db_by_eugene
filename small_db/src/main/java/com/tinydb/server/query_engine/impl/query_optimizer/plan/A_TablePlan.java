package com.tinydb.server.query_engine.impl.query_optimizer.plan;

import com.tinydb.server.query_engine.impl.query_optimizer.plan.Plan;
import com.tinydb.server.query_engine.common.catalog.MetadataMgr;
import com.tinydb.server.query_engine.common.catalog.stats.StatInfo;
import com.tinydb.server.query_engine.common.catalog.table.TableDefinition;
import com.tinydb.server.query_engine.common.catalog.table.TablePhysicalLayout;
import com.tinydb.server.storage_engine.RORecordScan;
import com.tinydb.server.storage_engine.common.transaction.Transaction;
import com.tinydb.server.storage_engine.impl.HeapRWRecordScan;

public class A_TablePlan implements Plan  {
  private String tblname;
  private Transaction tx;
  private TablePhysicalLayout recordValueLayout;
  private StatInfo si;

  public A_TablePlan(Transaction tx, String tblname, MetadataMgr md) {
    this.tblname = tblname;
    this.tx = tx;
    recordValueLayout = md.getLayout(tblname, tx);
  }


  public RORecordScan open() {

    // NOTE: The Place where Query Engine interacts with StorageEngine
    return new HeapRWRecordScan(tx, tblname, recordValueLayout);
  }


  public TableDefinition schema() {
    return recordValueLayout.schema();
  }

  public int blocksAccessed() {
    return si.blocksAccessed();
  }
}

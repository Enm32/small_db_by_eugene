package com.tinydb.server.query_engine.common.catalog.index;
import static java.sql.Types.INTEGER;
import com.tinydb.server.query_engine.common.catalog.stats.StatInfo;
import com.tinydb.server.query_engine.common.catalog.table.TableDefinition;
import com.tinydb.server.query_engine.common.catalog.table.TablePhysicalLayout;
import com.tinydb.server.storage_engine.RWIndexScan;
import com.tinydb.server.storage_engine.common.transaction.Transaction;
import com.tinydb.server.storage_engine.impl.index.bplustree.BPlusTreeIndex;

public class IndexInfo {
  private String idxname, fldname;
  private Transaction tx;
  private TableDefinition tblTableDefinition;
  private TablePhysicalLayout idxRecordValueLayout;
  private StatInfo si;


  public IndexInfo(String idxname, String fldname, TableDefinition tblTableDefinition,
      Transaction tx, StatInfo si) {
    this.idxname = idxname;
    this.fldname = fldname;
    this.tx = tx;
    this.tblTableDefinition = tblTableDefinition;
    this.idxRecordValueLayout = createIdxLayout();
    this.si = si;
  }


  public RWIndexScan open() {
    return new BPlusTreeIndex(tx, idxname, idxRecordValueLayout);
  }


  private TablePhysicalLayout createIdxLayout() {
    // Schema contains Block, Id, DataValue
    TableDefinition sch = new TableDefinition();
    sch.addIntField("block");
    sch.addIntField("id");
    if (tblTableDefinition.type(fldname) == INTEGER) {
      sch.addIntField("dataval");
    } else {
      int fldlen = tblTableDefinition.length(fldname);
      sch.addStringField("dataval", fldlen);
    }
    return new TablePhysicalLayout(sch);
  }

  public int blocksAccessed() {
    int rpb = tx.blockSize() / idxRecordValueLayout.slotSize();
    int numblocks = si.recordsOutput() / rpb;
    return BPlusTreeIndex.searchCost(numblocks, rpb);
  }

  public int recordsOutput() {
    return si.recordsOutput() / si.distinctValues(fldname);
  }


}

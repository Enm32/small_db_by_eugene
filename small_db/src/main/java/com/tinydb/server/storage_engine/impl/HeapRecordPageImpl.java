package com.tinydb.server.storage_engine.impl;


import static java.sql.Types.INTEGER;
import com.tinydb.server.storage_engine.common.transaction.Transaction;
import com.tinydb.server.storage_engine.common.file.BlockId;
import com.tinydb.server.query_engine.common.catalog.table.TablePhysicalLayout;
import com.tinydb.server.query_engine.common.catalog.table.TableDefinition;






public class HeapRecordPageImpl {

  public static final int EMPTY = 0, USED = 1;
  private Transaction tx;
  private BlockId blockId;
  private TablePhysicalLayout recordValueLayout;

  public HeapRecordPageImpl(Transaction tx, BlockId blockId, TablePhysicalLayout recordValueLayout) {
    this.tx = tx;
    this.blockId = blockId;
    this.recordValueLayout = recordValueLayout;
  }

 
  public int getInt(int slot, String fldname) {
    int fldpos = offset(slot) + recordValueLayout.offset(fldname);
    return tx.getInt(blockId, fldpos);
  }

  public String getString(int slot, String fldname) {
    int fldpos = offset(slot) + recordValueLayout.offset(fldname);
    return tx.getString(blockId, fldpos);
  }


  public void setInt(int slot, String fldname, int val) {
    int fldpos = offset(slot) + recordValueLayout.offset(fldname);
    tx.setInt(blockId, fldpos, val);
  }


  public void setString(int slot, String fldname, String val) {
    int fldpos = offset(slot) + recordValueLayout.offset(fldname);
    tx.setString(blockId, fldpos, val);
  }

  public void delete(int slot) {
    setFlag(slot, EMPTY);
  }

 
  
  public void format() {
    int slot = 0;
    while (isValidSlot(slot)) {
      tx.setInt(blockId, offset(slot), EMPTY);
      TableDefinition sch = recordValueLayout.schema();
      for (String fldname : sch.fields()) {
        int fldpos = offset(slot) + recordValueLayout.offset(fldname);
        if (sch.type(fldname) == INTEGER) {
          tx.setInt(blockId, fldpos, 0);
        } else {
          tx.setString(blockId, fldpos, "");
        }
      }
      slot++;
    }
  }

  public int findSlotAfter(int slot) {
    return searchAfter(slot, USED);
  }

  public int insertAfter(int slot) {
    int newslot = searchAfter(slot, EMPTY);
    if (newslot >= 0) {
      setFlag(newslot, USED);
    }
    return newslot;
  }

  public BlockId getBlockId() {
    return blockId;
  }

 
  private void setFlag(int slot, int flag) {
    tx.setInt(blockId, offset(slot), flag);
  }

  private int searchAfter(int slot, int flag) {
    slot++;
    while (isValidSlot(slot)) {
      if (tx.getInt(blockId, offset(slot)) == flag) {
        return slot;
      }
      slot++;
    }
    return -1;
  }

  private boolean isValidSlot(int slot) {
    return offset(slot + 1) <= tx.blockSize();
  }

  private int offset(int slot) {
    return slot * recordValueLayout.slotSize();
  }

}

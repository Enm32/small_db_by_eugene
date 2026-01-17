package com.tinydb.server.storage_engine.impl;



import static java.sql.Types.INTEGER;

import com.tinydb.server.frontend.common.domain.query_clause.d_Constant;
import com.tinydb.server.storage_engine.common.file.BlockId;
import  com.tinydb.server.storage_engine.impl.RecordKey;
import com.tinydb.server.storage_engine.RWRecordScan;
import com.tinydb.server.storage_engine.common.transaction.Transaction;
import  com.tinydb.server.storage_engine.impl.HeapRecordPageImpl;
import com.tinydb.server.query_engine.common.catalog.table.TablePhysicalLayout;






public class HeapRWRecordScan implements RWRecordScan {
  private final Transaction tx;
  private final TablePhysicalLayout recordValueLayout;
  private HeapRecordPageImpl rp;
  private final String filename;
  private int currentSlot;

  public HeapRWRecordScan(Transaction tx, String tblname, TablePhysicalLayout recordValueLayout) {
    this.tx = tx;
    this.recordValueLayout = recordValueLayout;
    filename = tblname + ".tbl";
    if (tx.blockCount(filename) == 0) {
      createAndMoveToNewBlock();
    } else {
      moveToBlock(0);
    }
  }

  // Methods that implement Scan

  public void seekToQueryStart() {
    moveToBlock(0);
  }

  public boolean next() {
    currentSlot = rp.findSlotAfter(currentSlot);
    while (currentSlot < 0) {
      if (atLastBlock()) {
        return false;
      }
      moveToBlock(rp.getBlockId().getBlockNumber() + 1);
      currentSlot = rp.findSlotAfter(currentSlot);
    }
    return true;
  }

  public int getInt(String fldname) {
    return rp.getInt(currentSlot, fldname);
  }

  public String getString(String fldname) {
    return rp.getString(currentSlot, fldname);
  }

  public d_Constant getVal(String fldname) {
    if (recordValueLayout.schema().type(fldname) == INTEGER) {
      return new d_Constant(getInt(fldname));
    } else {
      return new d_Constant(getString(fldname));
    }
  }

  public boolean hasField(String fldname) {
    return recordValueLayout.schema().hasField(fldname);
  }

  public void close() {
    if (rp != null) {
      tx.unpin(rp.getBlockId());
    }
  }

  // Methods that implement UpdateScan

  public void setInt(String fldname, int val) {
    rp.setInt(currentSlot, fldname, val);
  }

  public void setString(String fldname, String val) {
    rp.setString(currentSlot, fldname, val);
  }

  public void setVal(String fldname, d_Constant val) {
    if (recordValueLayout.schema().type(fldname) == INTEGER) {
      setInt(fldname, val.asInt());
    } else {
      setString(fldname, val.asString());
    }
  }

  public void seekToInsertStart() {
    currentSlot = rp.insertAfter(currentSlot);
    while (currentSlot < 0) {
      if (atLastBlock()) {
        createAndMoveToNewBlock();
      } else {
        moveToBlock(rp.getBlockId().getBlockNumber() + 1);
      }
      currentSlot = rp.insertAfter(currentSlot);
    }
  }

  public void delete() {
    rp.delete(currentSlot);
  }

  public void seekTo(RecordKey recordKey) {
    close();
    BlockId blk = new BlockId(filename, recordKey.getBlockNumber());
    rp = new HeapRecordPageImpl(tx, blk, recordValueLayout);
    currentSlot = recordKey.getSlotNumber();
  }

  public RecordKey getRid() {
    return new RecordKey(rp.getBlockId().getBlockNumber(), currentSlot);
  }

  // Private auxiliary methods

  private void moveToBlock(int blockNumber) {
    close();
    BlockId blk = new BlockId(filename, blockNumber);
    rp = new HeapRecordPageImpl(tx, blk, recordValueLayout);
    currentSlot = -1;
  }

  private void createAndMoveToNewBlock() {
    close();
    BlockId blk = tx.append(filename);
    rp = new HeapRecordPageImpl(tx, blk, recordValueLayout);
    rp.format();
    currentSlot = -1;
  }

  private boolean atLastBlock() {
    return rp.getBlockId().getBlockNumber() == tx.blockCount(filename) - 1;
  }



}

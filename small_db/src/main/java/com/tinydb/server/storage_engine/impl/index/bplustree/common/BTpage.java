package com.tinydb.server.storage_engine.impl.index.bplustree.common;

import static java.sql.Types.INTEGER;
import com.tinydb.server.frontend.common.domain.query_clause.d_Constant;
import com.tinydb.server.query_engine.common.catalog.table.TableDefinition;
import com.tinydb.server.query_engine.common.catalog.table.TablePhysicalLayout;
import com.tinydb.server.storage_engine.common.file.BlockId;
import com.tinydb.server.storage_engine.common.transaction.Transaction;
import com.tinydb.server.storage_engine.impl.RecordKey;


public class BTpage {

    private Transaction tx;
    private BlockId currentblk;
    private TablePhysicalLayout recordValueLayout;

  
    public BTpage(Transaction tx, BlockId currentblk, TablePhysicalLayout recordValueLayout) {
        this.tx = tx;
        this.currentblk = currentblk;
        this.recordValueLayout = recordValueLayout;
        tx.pin(currentblk);
    }

   
    public int findSlotBefore(d_Constant searchkey) {
        int slot = 0;
        while (slot < getNumRecs() && getDataVal(slot).compareTo(searchkey) < 0)
            slot++;
        return slot - 1;
    }

   
    public void close() {
        if (currentblk != null)
            tx.unpin(currentblk);
        currentblk = null;
    }

   
    public boolean isFull() {
        return slotpos(getNumRecs() + 1) >= tx.blockSize();
    }

   
    public BlockId split(int splitpos, int flag) {
        BlockId newblk = appendNew(flag);
        BTpage newpage = new BTpage(tx, newblk, recordValueLayout);
        transferRecs(splitpos, newpage);
        newpage.setFlag(flag);
        newpage.close();
        return newblk;
    }

   
    public d_Constant getDataVal(int slot) {
        return getVal(slot, "dataval");
    }

    public int getFlag() {
        return tx.getInt(currentblk, 0);
    }

    public void setFlag(int val) {
        tx.setInt(currentblk, 0, val);
    }

    
    public BlockId appendNew(int flag) {
        BlockId blk = tx.append(currentblk.getFileName());
        tx.pin(blk);
        format(blk, flag);
        return blk;
    }

    public void format(BlockId blk, int flag) {
        tx.setInt(blk, 0, flag);
        tx.setInt(blk, Integer.BYTES, 0);  // #records = 0
        int recsize = recordValueLayout.slotSize();
        for (int pos = 2 * Integer.BYTES; pos + recsize <= tx.blockSize(); pos += recsize)
            makeDefaultRecord(blk, pos);
    }

    private void makeDefaultRecord(BlockId blk, int pos) {
        for (String fldname : recordValueLayout.schema().fields()) {
            int offset = recordValueLayout.offset(fldname);
            if (recordValueLayout.schema().type(fldname) == INTEGER)
                tx.setInt(blk, pos + offset, 0);
            else
                tx.setString(blk, pos + offset, "");
        }
    }
  
    public int getChildNum(int slot) {
        return getInt(slot, "block");
    }

    public void insertDir(int slot, d_Constant val, int blknum) {
        insert(slot);
        setVal(slot, "dataval", val);
        setInt(slot, "block", blknum);
    }

   
    public RecordKey getDataRid(int slot) {
        return new RecordKey(getInt(slot, "block"), getInt(slot, "id"));
    }

   
    public void insertLeaf(int slot, d_Constant val, RecordKey recordKey) {
        insert(slot);
        setVal(slot, "dataval", val);
        setInt(slot, "block", recordKey.getBlockNumber());
        setInt(slot, "id", recordKey.getSlotNumber());
    }

    
    public void delete(int slot) {
        for (int i = slot + 1; i < getNumRecs(); i++)
            copyRecord(i, i - 1);
        setNumRecs(getNumRecs() - 1);
        return;
    }

  
    public int getNumRecs() {
        return tx.getInt(currentblk, Integer.BYTES);
    }

    

    private void setNumRecs(int n) {
        tx.setInt(currentblk, Integer.BYTES, n);
    }

    private int getInt(int slot, String fldname) {
        int pos = fldpos(slot, fldname);
        return tx.getInt(currentblk, pos);
    }

    private String getString(int slot, String fldname) {
        int pos = fldpos(slot, fldname);
        return tx.getString(currentblk, pos);
    }

    private d_Constant getVal(int slot, String fldname) {
        int type = recordValueLayout.schema().type(fldname);
        if (type == INTEGER)
            return new d_Constant(getInt(slot, fldname));
        else
            return new d_Constant(getString(slot, fldname));
    }

    private void setInt(int slot, String fldname, int val) {
        int pos = fldpos(slot, fldname);
        tx.setInt(currentblk, pos, val);
    }

    private void setString(int slot, String fldname, String val) {
        int pos = fldpos(slot, fldname);
        tx.setString(currentblk, pos, val);
    }

    private void setVal(int slot, String fldname, d_Constant val) {
        int type = recordValueLayout.schema().type(fldname);
        if (type == INTEGER)
            setInt(slot, fldname, val.asInt());
        else
            setString(slot, fldname, val.asString());
    }

    private void insert(int slot) {
        for (int i = getNumRecs(); i > slot; i--)
            copyRecord(i - 1, i);
        setNumRecs(getNumRecs() + 1);
    }

    private void copyRecord(int from, int to) {
        TableDefinition sch = recordValueLayout.schema();
        for (String fldname : sch.fields())
            setVal(to, fldname, getVal(from, fldname));
    }

    private void transferRecs(int slot, BTpage dest) {
        int destslot = 0;
        while (slot < getNumRecs()) {
            dest.insert(destslot);
            TableDefinition sch = recordValueLayout.schema();
            for (String fldname : sch.fields())
                dest.setVal(destslot, fldname, getVal(slot, fldname));
            delete(slot);
            destslot++;
        }
    }

    private int fldpos(int slot, String fldname) {
        int offset = recordValueLayout.offset(fldname);
        return slotpos(slot) + offset;
    }

    private int slotpos(int slot) {
        int slotsize = recordValueLayout.slotSize();
        return Integer.BYTES + Integer.BYTES + (slot * slotsize);
    }

}

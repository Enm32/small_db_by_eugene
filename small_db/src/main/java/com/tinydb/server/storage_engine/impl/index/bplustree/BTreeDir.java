package com.tinydb.server.storage_engine.impl.index.bplustree;

import com.tinydb.server.frontend.common.domain.query_clause.d_Constant;
import com.tinydb.server.query_engine.common.catalog.table.TablePhysicalLayout;
import com.tinydb.server.storage_engine.common.file.BlockId;
import com.tinydb.server.storage_engine.common.transaction.Transaction;
import com.tinydb.server.storage_engine.impl.index.bplustree.common.DirEntry;
import com.tinydb.server.storage_engine.impl.index.bplustree.common.BTpage;

public class BTreeDir {
    private Transaction tx;
    private TablePhysicalLayout recordValueLayout;
    private BTpage contents;
    private String filename;

    public BTreeDir(Transaction tx, BlockId blk, TablePhysicalLayout recordValueLayout) {
        this.tx = tx;
        this.recordValueLayout = recordValueLayout;
        contents = new BTpage(tx, blk, recordValueLayout);
        filename = blk.getFileName();
    }

   
    public void close() {
        contents.close();
    }

   
    public int search(d_Constant searchkey) {
        BlockId childblk = findChildBlock(searchkey);
        while (contents.getFlag() > 0) {
            contents.close();
            contents = new BTpage(tx, childblk, recordValueLayout);
            childblk = findChildBlock(searchkey);
        }
        return childblk.getBlockNumber();
    }

    public void makeNewRoot(DirEntry e) {
        d_Constant firstval = contents.getDataVal(0);
        int level = contents.getFlag();
        BlockId newblk = contents.split(0, level); 
        DirEntry oldroot = new DirEntry(firstval, newblk.getBlockNumber());
        insertEntry(oldroot);
        insertEntry(e);
        contents.setFlag(level + 1);
    }


    public DirEntry insert(DirEntry e) {
        if (contents.getFlag() == 0) return insertEntry(e);
        BlockId childblk = findChildBlock(e.dataVal());
        BTreeDir child = new BTreeDir(tx, childblk, recordValueLayout);
        DirEntry myentry = child.insert(e);
        child.close();
        return (myentry != null) ? insertEntry(myentry) : null;
    }

    private DirEntry insertEntry(DirEntry e) {
        int newslot = 1 + contents.findSlotBefore(e.dataVal());
        contents.insertDir(newslot, e.dataVal(), e.blockNumber());
        if (!contents.isFull()) return null;
       
        int level = contents.getFlag();
        int splitpos = contents.getNumRecs() / 2;
        d_Constant splitval = contents.getDataVal(splitpos);
        BlockId newblk = contents.split(splitpos, level);
        return new DirEntry(splitval, newblk.getBlockNumber());
    }

    private BlockId findChildBlock(d_Constant searchkey) {
        int slot = contents.findSlotBefore(searchkey);
        if (contents.getDataVal(slot + 1).equals(searchkey)) slot++;
        int blknum = contents.getChildNum(slot);
        return new BlockId(filename, blknum);
    }
}

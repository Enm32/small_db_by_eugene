package com.tinydb.server.storage_engine.impl.index.bplustree;

import com.tinydb.server.frontend.common.domain.query_clause.d_Constant;
import com.tinydb.server.query_engine.common.catalog.table.TablePhysicalLayout;
import com.tinydb.server.storage_engine.common.file.BlockId;
import com.tinydb.server.storage_engine.common.transaction.Transaction;
import com.tinydb.server.storage_engine.impl.RecordKey;
import com.tinydb.server.storage_engine.impl.index.bplustree.common.BTpage;
import com.tinydb.server.storage_engine.impl.index.bplustree.common.DirEntry;

public class BTreeLeaf {
    private Transaction tx;
    private TablePhysicalLayout recordValueLayout;
    private d_Constant searchkey;
    private BTpage contents;
    private int currentslot;
    private String filename;

  
    public BTreeLeaf(Transaction tx, BlockId blk, TablePhysicalLayout recordValueLayout, d_Constant searchkey) {
        this.tx = tx;
        this.recordValueLayout = recordValueLayout;
        this.searchkey = searchkey;
        contents = new BTpage(tx, blk, recordValueLayout);
        currentslot = contents.findSlotBefore(searchkey);
        filename = blk.getFileName();
    }

   
    public void close() {
        contents.close();
    }

 
    public boolean next() {
        currentslot++;
        if (currentslot >= contents.getNumRecs())
            return tryOverflow();
        else if (contents.getDataVal(currentslot).equals(searchkey))
            return true;
        else
            return tryOverflow();
    }

   
    public RecordKey getDataRid() {
        return contents.getDataRid(currentslot);
    }

   
    public void delete(RecordKey value) {
        while (next())
            if (getDataRid().equals(value)) {
                contents.delete(currentslot);
                return;
            }
    }

    public DirEntry insert(RecordKey value) {
        if (contents.getFlag() >= 0 && contents.getDataVal(0).compareTo(searchkey) > 0) {
         d_Constant firstval = contents.getDataVal(0);
            BlockId newblk = contents.split(0, contents.getFlag());
            currentslot = 0;
            contents.setFlag(-1);
            contents.insertLeaf(currentslot, searchkey, value);
            return new DirEntry(firstval, newblk.getBlockNumber());
        }

        currentslot++;
        contents.insertLeaf(currentslot, searchkey, value);
        if (!contents.isFull())
            return null;
        
     d_Constant firstkey = contents.getDataVal(0);
     d_Constant lastkey = contents.getDataVal(contents.getNumRecs() - 1);
        if (lastkey.equals(firstkey)) {
          
            BlockId newblk = contents.split(1, contents.getFlag());
            contents.setFlag(newblk.getBlockNumber());
            return null;
        } else {
            int splitpos = contents.getNumRecs() / 2;
         d_Constant splitkey = contents.getDataVal(splitpos);
            if (splitkey.equals(firstkey)) {
                
                while (contents.getDataVal(splitpos).equals(splitkey))
                    splitpos++;
                splitkey = contents.getDataVal(splitpos);
            } else {
                
                while (contents.getDataVal(splitpos - 1).equals(splitkey))
                    splitpos--;
            }
            BlockId newblk = contents.split(splitpos, -1);
            return new DirEntry(splitkey, newblk.getBlockNumber());
        }
    }

    private boolean tryOverflow() {
     d_Constant firstkey = contents.getDataVal(0);
        int flag = contents.getFlag();
        if (!searchkey.equals(firstkey) || flag < 0)
            return false;
        contents.close();
        BlockId nextblk = new BlockId(filename, flag);
        contents = new BTpage(tx, nextblk, recordValueLayout);
        currentslot = 0;
        return true;
    }
}

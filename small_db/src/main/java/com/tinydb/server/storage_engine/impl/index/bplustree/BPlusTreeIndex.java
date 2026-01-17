package com.tinydb.server.storage_engine.impl.index.bplustree;

import static java.sql.Types.INTEGER;
import com.tinydb.server.frontend.common.domain.query_clause.d_Constant;
import com.tinydb.server.storage_engine.impl.RecordKey;
import com.tinydb.server.storage_engine.impl.index.bplustree.common.BTpage;
import com.tinydb.server.query_engine.common.catalog.table.TableDefinition;
import com.tinydb.server.query_engine.common.catalog.table.TablePhysicalLayout;
import com.tinydb.server.storage_engine.RWIndexScan;
import com.tinydb.server.storage_engine.common.file.BlockId;
import com.tinydb.server.storage_engine.common.transaction.Transaction;
import com.tinydb.server.storage_engine.impl.index.bplustree.common.DirEntry;

public class BPlusTreeIndex  implements RWIndexScan {
    private Transaction tx;
    private TablePhysicalLayout dirRecordValueLayout, leafRecordValueLayout;
    private String leaftbl;
    private BTreeLeaf leaf = null;
    private BlockId rootblk;


    public BPlusTreeIndex(Transaction tx, String idxname, TablePhysicalLayout leafRecordValueLayout) {
        this.tx = tx;
      
        leaftbl = idxname + "leaf";
        this.leafRecordValueLayout = leafRecordValueLayout;
        if (tx.blockCount(leaftbl) == 0) {
            BlockId blk = tx.append(leaftbl);
            BTpage node = new BTpage(tx, blk, leafRecordValueLayout);
            node.format(blk, -1);
        }

       
        TableDefinition dirsch = new TableDefinition();
        dirsch.add("block", leafRecordValueLayout.schema());
        dirsch.add("dataval", leafRecordValueLayout.schema());
        String dirtbl = idxname + "dir";
        dirRecordValueLayout = new TablePhysicalLayout(dirsch);
        rootblk = new BlockId(dirtbl, 0);
        if (tx.blockCount(dirtbl) == 0) {
            
            tx.append(dirtbl);
            BTpage node = new BTpage(tx, rootblk, dirRecordValueLayout);
            node.format(rootblk, 0);
  
            int fldtype = dirsch.type("dataval");
            d_Constant minval = (fldtype == INTEGER) ? new d_Constant(Integer.MIN_VALUE) : new d_Constant("");
            node.insertDir(0, minval, 0);
            node.close();
        }
    }

  
    public static int searchCost(int numblocks, int rpb) {
        return 1 + (int) (Math.log(numblocks) / Math.log(rpb));
    }

    public void seek(d_Constant key) {
        close();
        BTreeDir root = new BTreeDir(tx, rootblk, dirRecordValueLayout);
        int blknum = root.search(key);
        root.close();
        BlockId leafblk = new BlockId(leaftbl, blknum);
        leaf = new BTreeLeaf(tx, leafblk, leafRecordValueLayout, key);
    }

 
    public boolean hasNext() {
        return leaf.next();
    }

  
    public RecordKey next() {
        return leaf.getDataRid();
    }

    public void insert(d_Constant key, RecordKey value) {
        seek(key);
        DirEntry e = leaf.insert(value);
        leaf.close();
        if (e == null) return;

        BTreeDir root = new BTreeDir(tx, rootblk, dirRecordValueLayout);
        DirEntry e2 = root.insert(e);
        if (e2 != null) root.makeNewRoot(e2);
        root.close();
    }

   
    public void delete(d_Constant key, RecordKey value) {
        seek(key);
        leaf.delete(value);
        leaf.close();
    }

   
    public void close() {
        if (leaf != null) leaf.close();
    }
}

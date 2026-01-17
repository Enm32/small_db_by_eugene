package com.tinydb.server.storage_engine.impl.index.bplustree.common;


import com.tinydb.server.frontend.common.domain.query_clause.d_Constant;



public class DirEntry {
    private d_Constant dataval;
    private int blocknum;

   
    public DirEntry(d_Constant dataval, int blocknum) {
        this.dataval = dataval;
        this.blocknum = blocknum;
    }

    
    public d_Constant dataVal() {
        return dataval;
    }

   
    public int blockNumber() {
        return blocknum;
    }

}

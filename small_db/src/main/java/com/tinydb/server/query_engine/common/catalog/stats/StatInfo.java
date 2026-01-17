package com.tinydb.server.query_engine.common.catalog.stats;


public class StatInfo {
 private int numBlocks;
    private int numRecs;

  
    public StatInfo(int numblocks, int numrecs) {
        this.numBlocks = numblocks;
        this.numRecs = numrecs;
    }

   
    public int blocksAccessed() {
        return numBlocks;
    }

   
    public int recordsOutput() {
        return numRecs;
    }

   
    public int distinctValues(String fldname) {
        return 1 + (numRecs / 3);
    }
}

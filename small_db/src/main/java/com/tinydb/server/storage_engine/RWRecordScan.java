package com.tinydb.server.storage_engine;

import com.tinydb.server.frontend.common.domain.query_clause.d_Constant;
import com.tinydb.server.storage_engine.impl.RecordKey;


public interface RWRecordScan extends RORecordScan  {

  
    
    public void setVal(String fldname, d_Constant val);

   
    
    public void setInt(String fldname, int val);

  
    public void setString(String fldname, String val);

  
    public void seekToInsertStart();

   
    public void delete();

    public RecordKey getRid();

   
    public void seekTo(RecordKey recordKey);


}

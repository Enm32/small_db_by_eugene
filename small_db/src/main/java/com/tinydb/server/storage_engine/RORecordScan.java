package com.tinydb.server.storage_engine;

import com.tinydb.server.frontend.common.domain.query_clause.d_Constant;


public interface RORecordScan {


   
    public void seekToQueryStart();

   
    public boolean next();

  
    public int getInt(String fldname);

  
    public String getString(String fldname);

    public d_Constant getVal(String fldname);

   
    public boolean hasField(String fldname);

    public void close();



}

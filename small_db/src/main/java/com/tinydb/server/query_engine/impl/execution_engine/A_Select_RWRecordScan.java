package com.tinydb.server.query_engine.impl.execution_engine;

import com.tinydb.server.frontend.common.domain.query_commands.ModifyData;
import com.tinydb.server.storage_engine.RORecordScan;
import com.tinydb.server.storage_engine.RWRecordScan;
import com.tinydb.server.storage_engine.common.transaction.Transaction;
import com.tinydb.server.storage_engine.impl.RecordKey;
import com.tinydb.server.frontend.common.domain.query_clause.a_Predicate;
import com.tinydb.server.frontend.common.domain.query_clause.d_Constant;

public class A_Select_RWRecordScan implements RWRecordScan  {
     
  private RORecordScan s;
  private a_Predicate pred;

 
  public A_Select_RWRecordScan(RORecordScan s, a_Predicate pred) {
    this.s = s;
    this.pred = pred;
  }


  public void seekToQueryStart() {
    s.seekToQueryStart();
  }

  public boolean next() {
    while (s.next()) {
      if (pred.isSatisfied(s)) {
        return true;
      }
    }
    return false;
  }

  public int getInt(String fldname) {
    return s.getInt(fldname);
  }

  public String getString(String fldname) {
    return s.getString(fldname);
  }

  public d_Constant getVal(String fldname) {
    return s.getVal(fldname);
  }

  public boolean hasField(String fldname) {
    return s.hasField(fldname);
  }

  public void close() {
    s.close();
  }



  public void setInt(String fldname, int val) {
    RWRecordScan us = (RWRecordScan) s;
    us.setInt(fldname, val);
  }

  public void setString(String fldname, String val) {
    RWRecordScan us = (RWRecordScan) s;
    us.setString(fldname, val);
  }

  public void setVal(String fldname, d_Constant val) {
    RWRecordScan us = (RWRecordScan) s;
    us.setVal(fldname, val);
  }

  public void delete() {
    RWRecordScan us = (RWRecordScan) s;
    us.delete();
  }

  public void seekToInsertStart() {
    RWRecordScan us = (RWRecordScan) s;
    us.seekToInsertStart();
  }

  public RecordKey getRid() {
    RWRecordScan us = (RWRecordScan) s;
    return us.getRid();
  }

  public void seekTo(RecordKey recordKey) {
    RWRecordScan us = (RWRecordScan) s;
    us.seekTo(recordKey);
  }


}

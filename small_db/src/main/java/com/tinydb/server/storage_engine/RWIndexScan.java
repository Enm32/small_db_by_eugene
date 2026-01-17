package com.tinydb.server.storage_engine;
import com.tinydb.server.frontend.common.domain.query_clause.d_Constant;
import com.tinydb.server.storage_engine.impl.RecordKey;



public interface RWIndexScan {
    // CRUD
    public void insert(d_Constant key, RecordKey value);

    public void delete(d_Constant key, RecordKey value);

    // Iterator
    public void seek(d_Constant key);

    public boolean hasNext();

    public RecordKey next();


    public void close();

}

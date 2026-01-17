package com.tinydb.server.query_engine.impl.query_optimizer.plan;

import com.tinydb.server.query_engine.common.catalog.table.TableDefinition;
import com.tinydb.server.storage_engine.RORecordScan;

public interface Plan {
    public RORecordScan open();

    public TableDefinition schema();

    public int blocksAccessed();

}

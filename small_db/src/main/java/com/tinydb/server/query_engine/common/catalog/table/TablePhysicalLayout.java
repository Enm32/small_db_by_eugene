package com.tinydb.server.query_engine.common.catalog.table;

import java.util.HashMap;
import java.util.Map;

import com.tinydb.server.storage_engine.common.file.Page;

import static java.sql.Types.INTEGER;

public class TablePhysicalLayout {
  private TableDefinition tableDefinition;
    private Map<String, Integer> offsets;
    private int slotsize;

    public TablePhysicalLayout(TableDefinition tableDefinition) {
        this.tableDefinition = tableDefinition;
        offsets = new HashMap<>();
        int pos = Integer.BYTES; 
        for (String fldname : tableDefinition.fields()) {
            offsets.put(fldname, pos);
            pos += lengthInBytes(fldname);
        }
        slotsize = pos;
    }

   
    public TablePhysicalLayout(TableDefinition tableDefinition, Map<String, Integer> offsets, int slotsize) {
        this.tableDefinition = tableDefinition;
        this.offsets = offsets;
        this.slotsize = slotsize;
    }

    public TableDefinition schema() {
        return tableDefinition;
    }

  
    public int offset(String fldname) {
        return offsets.get(fldname);
    }

 
    public int slotSize() {
        return slotsize;
    }

    private int lengthInBytes(String fldname) {
        int fldtype = tableDefinition.type(fldname);
        if (fldtype == INTEGER)
            return Integer.BYTES; 
        else 
            return Page.maxBytesRequiredForString(tableDefinition.length(fldname));
    }
}

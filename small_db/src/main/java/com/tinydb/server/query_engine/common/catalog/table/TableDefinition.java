package com.tinydb.server.query_engine.common.catalog.table;


import lombok.ToString;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;




@ToString
public class TableDefinition {
 private final List<String> fields = new ArrayList<>();
    private final Map<String, FieldInfo> info = new HashMap<>();

  
    public void addField(String fldname, int type, int length) {
        fields.add(fldname);
        info.put(fldname, new FieldInfo(type, length));
    }

  
    public void addIntField(String fldname) {
        addField(fldname, INTEGER, 0);
    }

   
    public void addStringField(String fldname, int length) {
        addField(fldname, VARCHAR, length);
    }

   
    public void add(String fldname, TableDefinition sch) {
        int type = sch.type(fldname);
        int length = sch.length(fldname);
        addField(fldname, type, length);
    }

    public void addAll(TableDefinition sch) {
        for (String fldname : sch.fields())
            add(fldname, sch);
    }

   
    public List<String> fields() {
        return fields;
    }

    public boolean hasField(String fldname) {
        return fields.contains(fldname);
    }

 
    public int type(String fldname) {
        return info.get(fldname).type;
    }

    public int length(String fldname) {
        return info.get(fldname).length;
    }

    @ToString
    class FieldInfo {
        int type, length;

        public FieldInfo(int type, int length) {
            this.type = type;
            this.length = length;
        }
    }
}

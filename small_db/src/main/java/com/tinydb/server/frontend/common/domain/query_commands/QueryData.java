package com.tinydb.server.frontend.common.domain.query_commands;


import com.tinydb.server.frontend.common.domain.query_clause.a_Predicate;

import lombok.ToString;
import java.util.List;
@ToString
public class QueryData {

    private List<String> fields;
    private String table;
    private a_Predicate pred;

    
    public QueryData(List<String> fields, String table, a_Predicate pred) {
        this.fields = fields;
        this.table = table;
        this.pred = pred;
    }

   
    public List<String> fields() {
        return fields;
    }

  
    public String table() {
        return table;
    }

   
    public a_Predicate pred() {
        return pred;
    }


}

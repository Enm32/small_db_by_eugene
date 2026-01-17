package com.tinydb.server.query_engine;
import com.tinydb.server.query_engine.common.dto.TableDto;

public interface IQueryEngine {
   public TableDto doQuery(String sql);

    public TableDto doUpdate(String sql);

    public void close();
}

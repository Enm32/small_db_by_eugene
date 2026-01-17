package com.tinydb.server.frontend;

import com.tinydb.server.frontend.common.domain.query_commands.QueryData;

/**
 * Parser interface to parse SQL statements to QueryEngine Command Objects.
 */
public interface IParser {

    public QueryData queryCmd();

    public Object updateCmd();


}

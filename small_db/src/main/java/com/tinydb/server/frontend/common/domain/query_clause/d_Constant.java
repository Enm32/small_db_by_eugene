package com.tinydb.server.frontend.common.domain.query_clause;

import java.io.Serializable;

public class d_Constant implements Comparable<d_Constant>, Serializable  {
 private static final long serialVersionUID = 1L;

    private Integer ival = null;
    private String sval = null;

    public d_Constant(Integer ival) {
        this.ival = ival;
    }

    public d_Constant(String sval) {
        this.sval = sval;
    }

    public Integer asInt() {
        return ival;
    }

    public String asString() {
        return sval;
    }

    public boolean equals(Object obj) {
        d_Constant c = (d_Constant) obj;
        return (ival != null) ? ival.equals(c.ival) : sval.equals(c.sval);
    }

    public int compareTo(d_Constant c) {
        return (ival != null) ? ival.compareTo(c.ival) : sval.compareTo(c.sval);
    }

    public int hashCode() {
        return (ival != null) ? ival.hashCode() : sval.hashCode();
    }

    public String toString() {
        return (ival != null) ? ival.toString() : sval.toString();
    }
}

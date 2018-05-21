package com.renseki.plugin;

public class SqlQuery {
    private final String query;
    private final Object[] params;

    public SqlQuery(String query, Object[] params) {
        this.query = query;
        this.params = params;
    }

    public String getQuery() {
        return query;
    }

    public Object[] getParams() {
        return params;
    }
}

package com.sunnysuperman.repository.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.springframework.jdbc.core.PreparedStatementCreator;

public class CreateWithGeneratedKeyStatement implements PreparedStatementCreator {
    private String sql;
    private Object[] params;

    public CreateWithGeneratedKeyStatement(String sql, Object[] params) {
        super();
        this.sql = sql;
        this.params = params;
    }

    public String getSql() {
        return sql;
    }

    public Object[] getParams() {
        return params;
    }

    @Override
    public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
        PreparedStatement statement = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        if (params != null) {
            for (int i = 0; i < params.length; i++) {
                statement.setObject(i + 1, params[i]);
            }
        }
        return statement;
    }

}

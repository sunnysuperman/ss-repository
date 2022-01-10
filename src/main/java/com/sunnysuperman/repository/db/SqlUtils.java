package com.sunnysuperman.repository.db;

import java.util.Collection;

public class SqlUtils {

    public static StringBuilder appendInClause(StringBuilder sql, Collection<?> items) {
        sql.append('(');
        for (int i = 0; i < items.size(); i++) {
            if (i > 0) {
                sql.append(",?");
            } else {
                sql.append('?');
            }
        }
        sql.append(')');
        return sql;
    }

    public static String replaceInClause(String sql, int index, Collection<?> items) {
        StringBuilder replacement = new StringBuilder(items.size() * 2 - 1);
        for (int i = 0; i < items.size(); i++) {
            if (i > 0) {
                replacement.append(",?");
            } else {
                replacement.append('?');
            }
        }
        return sql.replace("#" + index, replacement.toString());
    }

    public static String getCountDialect(String sql) {
        int index1 = sql.indexOf(" from ");
        if (index1 <= 0) {
            throw new RuntimeException("Bad sql: " + sql);
        }
        int index2 = sql.indexOf(" order by", index1);
        if (index2 < 0) {
            index2 = sql.length();
        }
        StringBuilder buf = new StringBuilder("select count(*)").append(sql.substring(index1, index2));
        return buf.toString();
    }

}

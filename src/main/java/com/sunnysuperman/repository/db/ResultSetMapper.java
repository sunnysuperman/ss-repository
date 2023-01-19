package com.sunnysuperman.repository.db;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class ResultSetMapper {

	public static Map<String, Object> map(ResultSet rs) throws SQLException {
		ResultSetMetaData meta = rs.getMetaData();
		int columnsCount = meta.getColumnCount();
		Map<String, Object> result = new HashMap<>(columnsCount, 1f);
		for (int i = 1; i <= columnsCount; i++) {
			String name = meta.getColumnName(i);
			result.put(name, rs.getObject(i));
		}
		return result;
	}

}

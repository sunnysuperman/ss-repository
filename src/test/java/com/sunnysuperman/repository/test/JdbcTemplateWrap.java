package com.sunnysuperman.repository.test;

import java.io.IOException;
import java.util.Properties;

import org.springframework.jdbc.core.JdbcTemplate;

import com.sunnysuperman.repository.db.LogAwareJdbcTemplate;
import com.zaxxer.hikari.HikariDataSource;

public class JdbcTemplateWrap {
	private static JdbcTemplate jdbcTemplate;

	public static JdbcTemplate get() throws IOException {
		if (jdbcTemplate == null) {
			Properties props = new Properties();
			props.load(JdbcTemplateWrap.class.getResourceAsStream("test.properties"));

			HikariDataSource ds = new HikariDataSource();
			ds.setDriverClassName("com.mysql.cj.jdbc.Driver");
			ds.setJdbcUrl(props.getProperty("jdbcUrl"));
			ds.setUsername(props.getProperty("username"));
			ds.setPassword(props.getProperty("password"));
			jdbcTemplate = new LogAwareJdbcTemplate(ds);
		}

		return jdbcTemplate;
	}

}

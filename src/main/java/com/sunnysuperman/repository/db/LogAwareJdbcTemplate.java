package com.sunnysuperman.repository.db;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.KeyHolder;

public class LogAwareJdbcTemplate extends JdbcTemplate {
    private static final Logger LOG = LoggerFactory.getLogger(LogAwareJdbcTemplate.class);

    public LogAwareJdbcTemplate(DataSource dataSource, boolean lazyInit) {
        super(dataSource, lazyInit);
    }

    @Override
    public List<Map<String, Object>> queryForList(String sql, Object... args) throws DataAccessException {
        long t1 = System.nanoTime();
        try {
            return super.queryForList(sql, args);
        } finally {
            long take = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t1);
            LOG.info("[JdbcTemplate] {}, take {} ms", sql, take);
        }
    }

    @Override
    public int update(PreparedStatementCreator psc, KeyHolder generatedKeyHolder) throws DataAccessException {
        long t1 = System.nanoTime();
        try {
            return super.update(psc, generatedKeyHolder);
        } finally {
            long take = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t1);
            if (psc instanceof CreateWithGeneratedKeyStatement) {
                LOG.info("[JdbcTemplate] {}, take {} ms", ((CreateWithGeneratedKeyStatement) psc).getSql(), take);
            } else {
                LOG.info("[JdbcTemplate] {}, take {} ms", psc.toString(), take);
            }
        }
    }

    @Override
    public int update(String sql, Object... args) throws DataAccessException {
        long t1 = System.nanoTime();
        try {
            return super.update(sql, args);
        } finally {
            long take = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t1);
            LOG.info("[JdbcTemplate] {}, take {} ms", sql, take);
        }
    }
}
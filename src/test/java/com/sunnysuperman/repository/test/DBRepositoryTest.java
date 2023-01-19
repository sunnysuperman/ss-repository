package com.sunnysuperman.repository.test;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.sunnysuperman.commons.util.StringUtil;
import com.sunnysuperman.repository.annotation.Column;
import com.sunnysuperman.repository.annotation.Entity;
import com.sunnysuperman.repository.annotation.Id;
import com.sunnysuperman.repository.annotation.IdStrategy;
import com.sunnysuperman.repository.annotation.Table;
import com.sunnysuperman.repository.db.DBCRUDRepository;
import com.sunnysuperman.repository.db.DBRepository;
import com.sunnysuperman.repository.db.mapper.LongDBMapper;

public class DBRepositoryTest {
	private static DBRepository dbRepository;

	@Entity
	@Table(name = "test_insert_generate_key")
	public static class AutoIncrementIdAwareEntity {
		@Id(strategy = IdStrategy.INCREMENT)
		@Column
		private Long id;

		@Column
		private String val;

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getVal() {
			return val;
		}

		public void setVal(String val) {
			this.val = val;
		}

	}

	public static DBRepository get() throws IOException {
		if (dbRepository == null) {
			dbRepository = new DBRepository() {

				@Override
				protected JdbcTemplate getJdbcTemplate() {
					try {
						return TestJdbcTemplate.get();
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}

			};

		}
		return dbRepository;
	}

	public static <T, ID> DBCRUDRepository<T, ID> getCRUDRepository(Class<T> type, Class<ID> idType)
			throws IOException {
		return new DBCRUDRepository<T, ID>() {

			@Override
			protected Class<T> getEntityClass() {
				return type;
			}

			@Override
			protected JdbcTemplate getJdbcTemplate() {
				try {
					return TestJdbcTemplate.get();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}

		};
	}

	@Test
	public void test_insertDoc() throws Exception {
		DBRepository repo = get();

		{
			long currentId = getCurrentAutoIncrementId();

			boolean ok = repo.insertDoc("test_insert_generate_key", Collections.singletonMap("val", makeValue()));
			assertTrue(ok);

			Long newId = repo.insertDoc("test_insert_generate_key", Collections.singletonMap("val", makeValue()),
					Long.class);
			assertTrue(newId == currentId + 2);
		}

		{
			int count = repo.count("select count(*) from test_insert", null);

			Map<String, Object> doc = new HashMap<>();
			doc.put("id", System.currentTimeMillis());
			doc.put("val", makeValue());
			repo.insertDoc("test_insert", doc);

			int count2 = repo.count("select count(*) from test_insert", null);
			assertTrue(count2 == count + 1);
		}
	}

	@Test
	public void test_insertDocs() throws Exception {
		DBRepository repo = get();
		long currentId = getCurrentAutoIncrementId();

		{
			List<Map<String, Object>> docs = new ArrayList<>();
			for (int i = 0; i < 3; i++) {
				docs.add(Collections.singletonMap("val", makeValue()));
			}
			repo.insertDocs("test_insert_generate_key", docs);

			long currentId2 = getCurrentAutoIncrementId();

			assertTrue(currentId2 == currentId + 3);
			// 重置自增id
			currentId = currentId2;
		}
		{
			List<Map<String, Object>> docs = new ArrayList<>();
			for (int i = 0; i < 5; i++) {
				docs.add(Collections.singletonMap("val", makeValue()));
			}
			List<Long> ids = repo.insertDocs("test_insert_generate_key", docs, Long.class);

			assertTrue(ids.size() == docs.size());
			for (int i = 0; i < ids.size(); i++) {
				assertTrue(ids.get(i).longValue() == currentId + i + 1);
			}
		}
	}

	@Test
	public void test_insertEntity() throws Exception {
		DBCRUDRepository<AutoIncrementIdAwareEntity, Long> repo = getCRUDRepository(AutoIncrementIdAwareEntity.class,
				Long.class);
		long currentId = getCurrentAutoIncrementId();

		AutoIncrementIdAwareEntity entity = new AutoIncrementIdAwareEntity();
		entity.setVal(makeValue());
		repo.insert(entity);

		assertTrue(entity.getId() == currentId + 1);
		long currentId2 = getCurrentAutoIncrementId();
		assertTrue(currentId2 == currentId + 1);
	}

	@Test
	public void test_insertEntityBatch() throws Exception {
		DBCRUDRepository<AutoIncrementIdAwareEntity, Long> repo = getCRUDRepository(AutoIncrementIdAwareEntity.class,
				Long.class);
		long currentId = getCurrentAutoIncrementId();

		List<AutoIncrementIdAwareEntity> entityList = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			AutoIncrementIdAwareEntity entity = new AutoIncrementIdAwareEntity();
			entity.setVal(makeValue());
			entityList.add(entity);
		}
		repo.insertBatch(entityList);

		for (int i = 0; i < entityList.size(); i++) {
			assertTrue(entityList.get(i).getId().longValue() == currentId + i + 1);
		}
		long currentId2 = getCurrentAutoIncrementId();
		assertTrue(currentId2 == currentId + entityList.size());
	}

	private String makeValue() {
		return StringUtil.randomString("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", 20);
	}

	private long getCurrentAutoIncrementId() throws Exception {
		Long currentId = get().find("select max(id) from test_insert_generate_key", null, LongDBMapper.getInstance());
		return currentId != null ? currentId : 0;
	}

}

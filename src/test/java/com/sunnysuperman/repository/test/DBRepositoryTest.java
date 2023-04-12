package com.sunnysuperman.repository.test;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.sunnysuperman.commons.util.StringUtil;
import com.sunnysuperman.repository.RepositoryException;
import com.sunnysuperman.repository.annotation.Column;
import com.sunnysuperman.repository.annotation.Entity;
import com.sunnysuperman.repository.annotation.Id;
import com.sunnysuperman.repository.annotation.IdStrategy;
import com.sunnysuperman.repository.annotation.Table;
import com.sunnysuperman.repository.annotation.VersionControl;
import com.sunnysuperman.repository.db.DBCRUDRepository;
import com.sunnysuperman.repository.db.DBRepository;
import com.sunnysuperman.repository.db.mapper.LongDBMapper;
import com.sunnysuperman.repository.exception.StaleEntityRepositoryException;

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

	@Entity
	@Table(name = "test_insert_update")
	public static class InsertUpdateAwareEntity {
		@Id(strategy = IdStrategy.PROVIDED)
		@Column
		private Long id;

		@Column
		private String v1;

		@Column(insertable = false)
		private String v2;

		@Column(updatable = false)
		private String v3;

		@Column(insertable = false, updatable = false)
		private String v4;

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getV1() {
			return v1;
		}

		public void setV1(String v1) {
			this.v1 = v1;
		}

		public String getV2() {
			return v2;
		}

		public void setV2(String v2) {
			this.v2 = v2;
		}

		public String getV3() {
			return v3;
		}

		public void setV3(String v3) {
			this.v3 = v3;
		}

		public String getV4() {
			return v4;
		}

		public void setV4(String v4) {
			this.v4 = v4;
		}

	}

	@Entity
	@Table(name = "test_versioning_int")
	public static class IntVerionAwareEntity {
		@Id(strategy = IdStrategy.INCREMENT)
		@Column
		private Long id;

		@VersionControl
		@Column
		private Integer version;

		@Column
		private String val;

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public Integer getVersion() {
			return version;
		}

		public void setVersion(Integer version) {
			this.version = version;
		}

		public String getVal() {
			return val;
		}

		public void setVal(String val) {
			this.val = val;
		}

	}

	@Entity
	@Table(name = "test_versioning_long")
	public static class LongVerionAwareEntity {
		@Id(strategy = IdStrategy.PROVIDED)
		@Column
		private Long id;

		@VersionControl
		@Column
		private Long version;

		@Column
		private String val;

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public Long getVersion() {
			return version;
		}

		public void setVersion(Long version) {
			this.version = version;
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
						return JdbcTemplateWrap.get();
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
					return JdbcTemplateWrap.get();
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

	@Test
	public void test_insertAndUpdate() throws Exception {
		DBCRUDRepository<InsertUpdateAwareEntity, Long> repo = getCRUDRepository(InsertUpdateAwareEntity.class,
				Long.class);

		Long id = 100L;
		repo.deleteById(id);
		{
			InsertUpdateAwareEntity e = new InsertUpdateAwareEntity();
			e.setId(id);
			e.setV1("xx1");
			e.setV2("xx2");
			e.setV3("xx3");
			e.setV4("xx4");
			repo.insert(e);
		}
		{
			InsertUpdateAwareEntity e = repo.findById(id);
			assertTrue(e.getV1().equals("xx1"));
			assertTrue(e.getV2() == null);
			assertTrue(e.getV3().equals("xx3"));
			assertTrue(e.getV4() == null);
		}
		{
			InsertUpdateAwareEntity e = repo.findById(id);
			e.setV1("zz1");
			e.setV2("zz2");
			e.setV3("zz3");
			e.setV4("zz4");
			assertTrue(repo.update(e));
		}
		{
			InsertUpdateAwareEntity e = repo.findById(id);
			assertTrue(e.getV1().equals("zz1"));
			assertTrue(e.getV2().equals("zz2"));
			assertTrue(e.getV3().equals("xx3"));
			assertTrue(e.getV4() == null);
		}
		{
			InsertUpdateAwareEntity e = repo.findById(id);
			e.setV3("zz3");
			e.setV4("zz4");
			assertTrue(repo.update(e, new HashSet<>(Arrays.asList("v3", "v4"))));
		}
		{
			InsertUpdateAwareEntity e = repo.findById(id);
			assertTrue(e.getV1().equals("zz1"));
			assertTrue(e.getV2().equals("zz2"));
			assertTrue(e.getV3().equals("zz3"));
			assertTrue(e.getV4().equals("zz4"));
		}
	}

	@Test
	public void test_versioning1() throws Exception {
		DBCRUDRepository<IntVerionAwareEntity, Long> repo = getCRUDRepository(IntVerionAwareEntity.class, Long.class);

		Long id;
		// 不指定版本号插入
		{
			IntVerionAwareEntity entity = new IntVerionAwareEntity();
			entity.setVal(makeValue());
			repo.insert(entity);
			id = entity.getId();
			assertTrue(entity.getVersion() == 1);
		}
		IntVerionAwareEntity last;
		{
			IntVerionAwareEntity entity = repo.findById(id);
			assertTrue(entity.getVersion() == 1);

			// 更新
			entity.setVal(makeValue());
			repo.update(entity);
			assertTrue(entity.getId().equals(id));
			assertTrue(entity.getVersion() == 2);

			// 连续更新
			entity.setVal(makeValue());
			repo.update(entity);
			assertTrue(entity.getId().equals(id));
			assertTrue(entity.getVersion() == 3);
			last = entity;
		}
		{
			IntVerionAwareEntity entity = repo.findById(id);
			assertTrue(entity.getVersion() == 3);
			assertTrue(entity.getVal().equals(last.getVal()));
		}
		// 指定版本号插入
		{
			IntVerionAwareEntity entity = new IntVerionAwareEntity();
			entity.setVersion(2);
			entity.setVal(makeValue());
			repo.insert(entity);
			assertTrue(entity.getVersion() == 2);

			IntVerionAwareEntity entity2 = repo.findById(entity.getId());
			assertTrue(entity2.getVersion() == 2);
			assertTrue(entity2.getVal().equals(entity.getVal()));
		}
		// 仅更新版本号
		{
			IntVerionAwareEntity entity = new IntVerionAwareEntity();
			entity.setVal(makeValue());
			repo.insert(entity);

			IntVerionAwareEntity entity2 = repo.findById(entity.getId());
			assertTrue(entity2.getVersion() == 1);
			repo.compareAndUpdateVersion(entity2);
			assertTrue(entity2.getVersion() == 2);

			IntVerionAwareEntity entity3 = repo.findById(entity.getId());
			assertTrue(entity3.getVersion() == 2);
			repo.update(entity3, Collections.singleton("version"));
			assertTrue(entity3.getVersion() == 3);

			IntVerionAwareEntity entity4 = repo.findById(entity.getId());
			assertTrue(entity4.getVersion() == 3);
		}
	}

	@Test
	public void test_versioning2() throws Exception {
		DBCRUDRepository<LongVerionAwareEntity, Long> repo = getCRUDRepository(LongVerionAwareEntity.class, Long.class);

		Long id = 100L;
		repo.deleteById(id);

		// 如果非自增ID，save需要指定版本号插入
		try {
			LongVerionAwareEntity entity = new LongVerionAwareEntity();
			entity.setId(id);
			entity.setVal(makeValue());
			repo.save(entity);
			assertTrue(false);
		} catch (RepositoryException e) {
			assertTrue(true);
		}
		{
			LongVerionAwareEntity entity = new LongVerionAwareEntity();
			entity.setId(id);
			entity.setVersion(10L);
			entity.setVal(makeValue());
			repo.save(entity);
			assertTrue(entity.getId().equals(id));
			assertTrue(entity.getVersion() == 10L);
		}
		LongVerionAwareEntity last;
		{
			LongVerionAwareEntity entity = repo.findById(id);
			assertTrue(entity.getVersion() == 10L);

			// 更新
			entity.setVal(makeValue());
			repo.update(entity);
			assertTrue(entity.getId().equals(id));
			assertTrue(entity.getVersion() == 11L);

			// 连续更新
			entity.setVal(makeValue());
			repo.update(entity);
			assertTrue(entity.getId().equals(id));
			assertTrue(entity.getVersion() == 12L);
			last = entity;
		}
		{
			LongVerionAwareEntity entity = repo.findById(id);
			assertTrue(entity.getVersion() == 12L);
			assertTrue(entity.getVal().equals(last.getVal()));
		}
		// 指定版本号插入
		{
			Long id2 = 101L;
			repo.deleteById(id2);

			LongVerionAwareEntity entity = new LongVerionAwareEntity();
			entity.setId(id2);
			entity.setVersion(20L);
			entity.setVal(makeValue());
			repo.insert(entity);
			assertTrue(entity.getVersion() == 20L);

			LongVerionAwareEntity entity2 = repo.findById(entity.getId());
			assertTrue(entity2.getVersion() == 20L);
			assertTrue(entity2.getVal().equals(entity.getVal()));
		}
	}

	@Test
	public void test_concurrentUpdate() throws Exception {
		DBCRUDRepository<IntVerionAwareEntity, Long> repo = getCRUDRepository(IntVerionAwareEntity.class, Long.class);

		Long id = 100L;
		repo.deleteById(id);

		{
			IntVerionAwareEntity entity = new IntVerionAwareEntity();
			entity.setId(id);
			entity.setVersion(1);
			entity.setVal(makeValue());
			repo.insert(entity);
		}

		IntVerionAwareEntity entity = repo.findById(id);
		IntVerionAwareEntity entity2 = repo.findById(id);

		// ok
		entity.setVal(makeValue());
		repo.update(entity);

		// concurrent update
		try {
			entity2.setVal(makeValue());
			repo.update(entity2);
			assertTrue(false);
		} catch (StaleEntityRepositoryException e) {
			e.printStackTrace();
			assertTrue(true);
		}

		IntVerionAwareEntity entity3 = repo.findById(id);
		entity3.setVal(entity2.getVal());
		repo.update(entity3);
	}

	private String makeValue() {
		return StringUtil.randomString("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", 20);
	}

	private long getCurrentAutoIncrementId() throws Exception {
		Long currentId = get().find("select max(id) from test_insert_generate_key", null, LongDBMapper.getInstance());
		return currentId != null ? currentId : 0;
	}

}

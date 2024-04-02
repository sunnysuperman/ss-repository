package com.sunnysuperman.repository.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
	void insertDoc() throws Exception {
		DBRepository repo = get();

		{
			long currentId = getCurrentAutoIncrementId();

			boolean ok = repo.insertDoc("test_insert_generate_key", Collections.singletonMap("val", makeValue()));
			assertTrue(ok);

			Long newId = repo.insertDoc("test_insert_generate_key", Collections.singletonMap("val", makeValue()),
					Long.class);
			assertEquals(currentId + 2, newId);
		}

		{
			int count = repo.count("select count(*) from test_insert", null);

			Map<String, Object> doc = new HashMap<>();
			doc.put("id", System.currentTimeMillis());
			doc.put("val", makeValue());
			repo.insertDoc("test_insert", doc);

			int count2 = repo.count("select count(*) from test_insert", null);
			assertEquals(count + 1, count2);
		}
	}

	@Test
	void insertDocs() throws Exception {
		DBRepository repo = get();
		long currentId = getCurrentAutoIncrementId();

		{
			List<Map<String, Object>> docs = new ArrayList<>();
			for (int i = 0; i < 3; i++) {
				docs.add(Collections.singletonMap("val", makeValue()));
			}
			repo.insertDocs("test_insert_generate_key", docs);

			long currentId2 = getCurrentAutoIncrementId();

			assertEquals(currentId + 3, currentId2);
			// 重置自增id
			currentId = currentId2;
		}
		{
			List<Map<String, Object>> docs = new ArrayList<>();
			for (int i = 0; i < 5; i++) {
				docs.add(Collections.singletonMap("val", makeValue()));
			}
			List<Long> ids = repo.insertDocs("test_insert_generate_key", docs, Long.class);
			assertEquals(docs.size(), ids.size());
			for (int i = 0; i < ids.size(); i++) {
				assertEquals(currentId + i + 1, ids.get(i).longValue());
			}
		}
	}

	@Test
	void insertEntity() throws Exception {
		DBCRUDRepository<AutoIncrementIdAwareEntity, Long> repo = getCRUDRepository(AutoIncrementIdAwareEntity.class,
				Long.class);
		long currentId = getCurrentAutoIncrementId();

		AutoIncrementIdAwareEntity entity = new AutoIncrementIdAwareEntity();
		entity.setVal(makeValue());
		repo.insert(entity);

		assertEquals(currentId + 1, entity.getId());
		long currentId2 = getCurrentAutoIncrementId();
		assertEquals(currentId + 1, currentId2);
	}

	@Test
	void insertEntityBatch() throws Exception {
		DBCRUDRepository<AutoIncrementIdAwareEntity, Long> repo = getCRUDRepository(AutoIncrementIdAwareEntity.class,
				Long.class);
		{
			AutoIncrementIdAwareEntity e = new AutoIncrementIdAwareEntity();
			e.setVal("xx");
			repo.insert(e);
		}
		long currentId = getCurrentAutoIncrementId();

		List<AutoIncrementIdAwareEntity> entityList = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			AutoIncrementIdAwareEntity entity = new AutoIncrementIdAwareEntity();
			entity.setVal(makeValue());
			entityList.add(entity);
		}
		repo.insertBatch(entityList);

		for (int i = 0; i < entityList.size(); i++) {
			assertEquals(currentId + i + 1, entityList.get(i).getId().longValue());
		}
		long currentId2 = getCurrentAutoIncrementId();
		assertEquals(currentId + entityList.size(), currentId2);
	}

	@Test
	void insertAndUpdate() throws Exception {
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
			assertEquals("xx1", e.getV1());
			assertNull(e.getV2());
			assertEquals("xx3", e.getV3());
			assertNull(e.getV4());
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
			assertEquals("zz1", e.getV1());
			assertEquals("zz2", e.getV2());
			assertEquals("xx3", e.getV3());
			assertNull(e.getV4());
		}
		{
			InsertUpdateAwareEntity e = repo.findById(id);
			e.setV3("zz3");
			e.setV4("zz4");
			assertTrue(repo.update(e, new HashSet<>(Arrays.asList("v3", "v4"))));
		}
		{
			InsertUpdateAwareEntity e = repo.findById(id);
			assertEquals("zz1", e.getV1());
			assertEquals("zz2", e.getV2());
			assertEquals("zz3", e.getV3());
			assertEquals("zz4", e.getV4());
		}
	}

	@Test
	void versioning1() throws Exception {
		DBCRUDRepository<IntVerionAwareEntity, Long> repo = getCRUDRepository(IntVerionAwareEntity.class, Long.class);

		Long id;
		// 不指定版本号插入
		{
			IntVerionAwareEntity entity = new IntVerionAwareEntity();
			entity.setVal(makeValue());
			repo.insert(entity);
			id = entity.getId();
			assertEquals(1, entity.getVersion());
		}
		IntVerionAwareEntity last;
		{
			IntVerionAwareEntity entity = repo.findById(id);
			assertEquals(1, entity.getVersion());

			// 更新
			entity.setVal(makeValue());
			repo.update(entity);
			assertEquals(id, entity.getId());
			assertEquals(2, entity.getVersion());

			// 连续更新
			entity.setVal(makeValue());
			repo.update(entity);
			assertEquals(id, entity.getId());
			assertEquals(3, entity.getVersion());
			last = entity;
		}
		{
			IntVerionAwareEntity entity = repo.findById(id);
			assertEquals(3, entity.getVersion());
			assertEquals(last.getVal(), entity.getVal());
		}
		// 指定版本号插入
		{
			IntVerionAwareEntity entity = new IntVerionAwareEntity();
			entity.setVersion(2);
			entity.setVal(makeValue());
			repo.insert(entity);
			assertEquals(2, entity.getVersion());

			IntVerionAwareEntity entity2 = repo.findById(entity.getId());
			assertEquals(2, entity2.getVersion());
			assertEquals(entity.getVal(), entity2.getVal());
		}
		// 仅更新版本号
		{
			IntVerionAwareEntity entity = new IntVerionAwareEntity();
			entity.setVal(makeValue());
			repo.insert(entity);

			IntVerionAwareEntity entity2 = repo.findById(entity.getId());
			assertEquals(1, entity2.getVersion());
			repo.compareAndUpdateVersion(entity2);
			assertEquals(2, entity2.getVersion());

			IntVerionAwareEntity entity3 = repo.findById(entity.getId());
			assertEquals(2, entity3.getVersion());
			repo.update(entity3, Collections.singleton("version"));
			assertEquals(3, entity3.getVersion());

			IntVerionAwareEntity entity4 = repo.findById(entity.getId());
			assertEquals(3, entity4.getVersion());
		}
	}

	@Test
	void versioning2() throws Exception {
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
			e.printStackTrace();
			assertTrue(true);
		}
		{
			LongVerionAwareEntity entity = new LongVerionAwareEntity();
			entity.setId(id);
			entity.setVersion(10L);
			entity.setVal(makeValue());
			repo.save(entity);
			assertEquals(id, entity.getId());
			assertEquals(10L, entity.getVersion());
		}
		LongVerionAwareEntity last;
		{
			LongVerionAwareEntity entity = repo.findById(id);
			assertEquals(10L, entity.getVersion());

			// 更新
			entity.setVal(makeValue());
			repo.update(entity);
			assertEquals(id, entity.getId());
			assertEquals(11L, entity.getVersion());

			// 连续更新
			entity.setVal(makeValue());
			repo.update(entity);
			assertEquals(id, entity.getId());
			assertEquals(12L, entity.getVersion());
			last = entity;
		}
		{
			LongVerionAwareEntity entity = repo.findById(id);
			assertEquals(12L, entity.getVersion());
			assertEquals(last.getVal(), entity.getVal());
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
			assertEquals(20L, entity.getVersion());

			LongVerionAwareEntity entity2 = repo.findById(entity.getId());
			assertEquals(20L, entity2.getVersion());
			assertEquals(entity.getVal(), entity2.getVal());
		}
	}

	@Test
	void versioning3() throws Exception {
		DBCRUDRepository<IntVerionAwareEntity, Long> repo = getCRUDRepository(IntVerionAwareEntity.class, Long.class);

		IntVerionAwareEntity a = new IntVerionAwareEntity();
		a.setVal("a1");
		repo.insert(a);
		assertNotNull(a.getId());

		IntVerionAwareEntity a2 = repo.findById(a.getId());
		a2.setVal("a2");
		repo.save(a2);
		assertEquals(a2.getVersion().intValue(), a.getVersion().intValue() + 1);

		try {
			a.setVal("a3");
			repo.save(a);
			assertTrue(false);
		} catch (StaleEntityRepositoryException e) {
			e.printStackTrace();
		}
	}

	@Test
	void concurrentUpdate() throws Exception {
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

	@Test
	void delete() throws Exception {
		DBCRUDRepository<AutoIncrementIdAwareEntity, Long> repo = getCRUDRepository(AutoIncrementIdAwareEntity.class,
				Long.class);

		AutoIncrementIdAwareEntity a1 = new AutoIncrementIdAwareEntity();
		a1.setVal("a1");
		repo.insert(a1);
		assertNotNull(a1.getId());

		AutoIncrementIdAwareEntity a2 = repo.findById(a1.getId());
		a2.setVal("a2");
		repo.update(a2);
		assertEquals(a2.getId(), a1.getId());

		assertTrue(repo.delete(a1));
		assertTrue(!repo.delete(a1));
		assertTrue(!repo.delete(a2));
	}

	@Test
	void deleteVersionAwareEntity() throws Exception {
		DBCRUDRepository<IntVerionAwareEntity, Long> repo = getCRUDRepository(IntVerionAwareEntity.class, Long.class);

		IntVerionAwareEntity a1 = new IntVerionAwareEntity();
		a1.setVal("a1");
		repo.insert(a1);
		assertNotNull(a1.getId());

		IntVerionAwareEntity a2 = repo.findById(a1.getId());
		a2.setVal("a2");
		repo.update(a2);
		assertEquals(a2.getId(), a1.getId());

		try {
			repo.delete(a1);
			assertTrue(false);
		} catch (StaleEntityRepositoryException e) {
			e.printStackTrace();
		}

		assertTrue(repo.delete(a2));
	}

	@Test
	void updateBatch() throws Exception {
		DBCRUDRepository<IntVerionAwareEntity, Long> repo = getCRUDRepository(IntVerionAwareEntity.class, Long.class);

		IntVerionAwareEntity e1 = new IntVerionAwareEntity();
		e1.setVal("xx");
		repo.insert(e1);
		e1.setVal("xx2");

		IntVerionAwareEntity e2 = new IntVerionAwareEntity();
		e2.setVal("yy");
		repo.insert(e2);
		e2.setVal("zz");

		assertTrue(repo.updateBatch(Arrays.asList(e1, e2)));

		{
			IntVerionAwareEntity e = repo.getById(e1.getId());
			assertEquals(2, e.getVersion());
			assertEquals("xx2", e.getVal());
		}
		{
			e2 = repo.getById(e2.getId());
			assertEquals(2, e2.getVersion());
			assertEquals("zz", e2.getVal());
		}
		{
			IntVerionAwareEntity e = repo.getById(e1.getId());
			e.setVersion(1); // 版本号不对
			e.setVal("xx3");

			e2.setVal("zz...");

			try {
				repo.updateBatch(Arrays.asList(e, e2));
				assertTrue(false);
			} catch (StaleEntityRepositoryException ex) {
				ex.printStackTrace();
				assertTrue(ex.getMessage().contains(e.getId().toString()));
				assertFalse(ex.getMessage().contains(e2.getId().toString()));
			}
		}
		// 批量提交部分失败
		{
			e1 = repo.getById(e1.getId());
			assertEquals(2, e1.getVersion());
			assertEquals("xx2", e1.getVal());
		}
		{
			e2 = repo.getById(e2.getId());
			assertEquals(3, e2.getVersion());
			assertEquals("zz...", e2.getVal());
		}

		// 版本正确，再次提交批量修改
		e1.setVal("xx5");
		e2.setVal("zz6");
		assertTrue(repo.updateBatch(Arrays.asList(e1, e2)));

		{
			e1 = repo.getById(e1.getId());
			assertEquals(3, e1.getVersion());
			assertEquals("xx5", e1.getVal());
		}
		{
			e2 = repo.getById(e2.getId());
			assertEquals(4, e2.getVersion());
			assertEquals("zz6", e2.getVal());
		}
	}

	private String makeValue() {
		return StringUtil.randomString("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", 20);
	}

	private long getCurrentAutoIncrementId() throws Exception {
		Long currentId = get().find("select max(id) from test_insert_generate_key", null, LongDBMapper.getInstance());
		return currentId != null ? currentId : 0;
	}

}

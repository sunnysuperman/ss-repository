package com.sunnysuperman.repository.test;

import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.sunnysuperman.commons.config.Config;
import com.sunnysuperman.commons.config.PropertiesConfig;
import com.sunnysuperman.repository.SaveResult;
import com.sunnysuperman.repository.db.DBRepository;
import com.sunnysuperman.repository.db.LogAwareJdbcTemplate;
import com.sunnysuperman.repository.db.SerializeWrapper;
import com.sunnysuperman.repository.db.mapper.BeanDBMapper;
import com.sunnysuperman.repository.serialize.IdGenerator;
import com.sunnysuperman.repository.serialize.SerializeBean;
import com.sunnysuperman.repository.serialize.SerializeId;
import com.sunnysuperman.repository.serialize.SerializeProperty;
import com.sunnysuperman.repository.serialize.Serializer;

import junit.framework.TestCase;

public class DBRepositoryTest extends TestCase {

    @SerializeBean(value = "test_device")
    public static class Device {
        @SerializeId(generator = IdGenerator.PROVIDE)
        @SerializeProperty
        private String id;

        @SerializeProperty(updatable = false)
        private Long createdAt;

        @SerializeProperty
        private String name;

        @SerializeProperty
        private String notes;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public Long getCreatedAt() {
            return createdAt;
        }

        public void setCreatedAt(Long createdAt) {
            this.createdAt = createdAt;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getNotes() {
            return notes;
        }

        public void setNotes(String notes) {
            this.notes = notes;
        }

    }

    private static class DefaultDBRepository extends DBRepository {
        private JdbcTemplate jdbcTemplate;

        public DefaultDBRepository(JdbcTemplate jdbcTemplate) {
            super();
            this.jdbcTemplate = jdbcTemplate;
        }

        @Override
        protected JdbcTemplate getJdbcTemplate() {
            return jdbcTemplate;
        }
    }

    private static DBRepository repository;
    static {
        try {
            Serializer.scan(DBRepositoryTest.class.getPackage().getName());

            Config config = new PropertiesConfig(DBRepositoryTest.class.getResourceAsStream("db.properties"));

            ComboPooledDataSource ds = new com.mchange.v2.c3p0.ComboPooledDataSource();
            ds.setDriverClass("com.mysql.jdbc.Driver");
            ds.setJdbcUrl(config.getString("db.jdbcUrl"));
            ds.setUser(config.getString("db.user"));
            ds.setPassword(config.getString("db.password"));
            ds.setInitialPoolSize(config.getInt("db.initialPoolSize"));
            ds.setMinPoolSize(config.getInt("db.minPoolSize"));
            ds.setMaxPoolSize(config.getInt("db.maxPoolSize"));
            ds.setAcquireIncrement(config.getInt("db.acquireIncrement"));
            ds.setAcquireRetryAttempts(config.getInt("db.acquireRetryAttempts"));
            ds.setMaxIdleTime(config.getInt("db.maxIdleTime"));

            JdbcTemplate jdbcTemplate = new LogAwareJdbcTemplate(ds, false);
            repository = new DefaultDBRepository(jdbcTemplate);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void test_insert_update() {
        String id = "1000";
        repository.removeByKey("test_device", "id", id);
        BeanDBMapper<Device> mapper = new BeanDBMapper<>(Device.class);

        // insert 1
        {
            String name = "Device name on insert";
            Device device = new Device();
            device.setId(id);
            device.setName(name);
            device.setCreatedAt(System.currentTimeMillis());
            assertTrue(repository.insert(device) == null);

            Device found = repository.findByKey("test_device", null, "id", id, mapper);
            assertTrue(found.getName().equals(name));
        }

        repository.removeByKey("test_device", "id", id);

        // insert 2
        {
            String name = "Device name on insert";
            final String notes = "notes on insert";
            Device device = new Device();
            device.setId(id);
            device.setName(name);
            device.setCreatedAt(System.currentTimeMillis());
            assertTrue(repository.insert(device, new SerializeWrapper<Device>() {

                @Override
                public Map<String, Object> wrap(Map<String, Object> doc, Device bean) {
                    doc.put("notes", notes);
                    return doc;
                }

            }) == null);

            Device found = repository.findByKey("test_device", null, "id", id, mapper);
            assertTrue(found.getName().equals(name));
            assertTrue(found.getNotes().equals(notes));
        }

        // update 1
        {
            String name = "Device name on update1";
            String notes = "notes on update 1";
            Device device = new Device();
            device.setId(id);
            device.setName(name);
            device.setNotes(notes);
            assertTrue(repository.update(device));

            Device found = repository.findByKey("test_device", null, "id", id, mapper);
            assertTrue(found.getName().equals(name));
            assertTrue(found.getNotes().equals(notes));
        }

        // update 2
        {
            final String name = "Device name on update2";
            Device device = new Device();
            device.setId(id);
            device.setName(name);
            assertTrue(repository.update(device, new SerializeWrapper<Device>() {

                @Override
                public Map<String, Object> wrap(Map<String, Object> doc, Device bean) {
                    doc.put("name", name);
                    return doc;
                }

            }));

            Device found = repository.findByKey("test_device", null, "id", id, mapper);
            assertTrue(found.getName().equals(name));
            assertTrue(found.getNotes() == null);
        }
    }

    public void test_save() {
        String id = "1000";
        Long createdAt = 123L;
        repository.removeByKey("test_device", "id", id);
        BeanDBMapper<Device> mapper = new BeanDBMapper<>(Device.class);

        {
            String name = "name to save";
            String notes = "notes to save";
            Device device = new Device();
            device.setId(id);
            device.setName(name);
            device.setNotes(notes);
            device.setCreatedAt(createdAt);
            SaveResult result = repository.save(device);
            assertTrue(result.isInserted());

            Device found = repository.findByKey("test_device", null, "id", id, mapper);
            assertTrue(found.getName().equals(name));
            assertTrue(found.getNotes().equals(notes));
            assertTrue(found.getCreatedAt().equals(createdAt));
        }

        {
            String name = "name to update";
            Device device = new Device();
            device.setId(id);
            device.setName(name);
            device.setNotes(null);
            SaveResult result = repository.save(device);
            assertTrue(result.isUpdated());

            Device found = repository.findByKey("test_device", null, "id", id, mapper);
            assertTrue(found.getName().equals(name));
            assertTrue(found.getNotes() == null);
            assertTrue(found.getCreatedAt().equals(createdAt));
        }

        {
            String name = "name to update2";
            final String notes = "abc";
            Device device = new Device();
            device.setId(id);
            device.setName(name);
            device.setNotes(null);
            SaveResult result = repository.save(device, new SerializeWrapper<Device>() {

                @Override
                public Map<String, Object> wrap(Map<String, Object> doc, Device bean) {
                    doc.put("notes", notes);
                    return doc;
                }

            });
            assertTrue(result.isUpdated());

            Device found = repository.findByKey("test_device", null, "id", id, mapper);
            assertTrue(found.getName().equals(name));
            assertTrue(found.getNotes().equals(notes));
            assertTrue(found.getCreatedAt().equals(createdAt));
        }
    }
}

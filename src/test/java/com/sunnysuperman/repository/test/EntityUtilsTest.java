package com.sunnysuperman.repository.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.Date;

import org.junit.jupiter.api.Test;

import com.sunnysuperman.commons.util.FormatUtil;
import com.sunnysuperman.repository.EntityUtils;
import com.sunnysuperman.repository.annotation.Column;
import com.sunnysuperman.repository.annotation.Entity;
import com.sunnysuperman.repository.annotation.Id;
import com.sunnysuperman.repository.annotation.IdStrategy;
import com.sunnysuperman.repository.annotation.Table;
import com.sunnysuperman.repository.annotation.VersionControl;

class EntityUtilsTest {

	@Entity
	@Table(name = "rule")
	public static class Rule {
		@Id(strategy = IdStrategy.INCREMENT)
		@Column
		private Long id;

		@VersionControl
		@Column
		private Long version;

		@Column(updatable = false, nullable = false)
		private Date createdDate;

		@Column(nullable = false)
		private Date updatedDate;

		@Column(comment = "创建者", updatable = false)
		private String createdBy;

		@Column(comment = "更新者")
		private String updatedBy;

		@Column(comment = "规则名称", nullable = false)
		private String name;

		@Column(comment = "是否启用", nullable = false)
		private Boolean enabled;

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

		public Date getCreatedDate() {
			return createdDate;
		}

		public void setCreatedDate(Date createdDate) {
			this.createdDate = createdDate;
		}

		public Date getUpdatedDate() {
			return updatedDate;
		}

		public void setUpdatedDate(Date updatedDate) {
			this.updatedDate = updatedDate;
		}

		public String getCreatedBy() {
			return createdBy;
		}

		public void setCreatedBy(String createdBy) {
			this.createdBy = createdBy;
		}

		public String getUpdatedBy() {
			return updatedBy;
		}

		public void setUpdatedBy(String updatedBy) {
			this.updatedBy = updatedBy;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Boolean getEnabled() {
			return enabled;
		}

		public void setEnabled(Boolean enabled) {
			this.enabled = enabled;
		}

	}

	@Test
	void testCopyNotUpdatableFields() {
		Rule rule1 = new Rule();
		rule1.setId(1000L);
		rule1.setVersion(2L);
		rule1.setCreatedDate(FormatUtil.parseISO8601Date("2024-04-16T18:00:00.000Z", null));
		rule1.setUpdatedDate(rule1.getCreatedDate());
		rule1.setCreatedBy("张三");
		rule1.setUpdatedBy(rule1.getCreatedBy());
		rule1.setName("默认规则");
		rule1.setEnabled(Boolean.TRUE);

		Rule rule2 = new Rule();
		rule2.setId(1000L);
		rule2.setName("修改过的默认规则");
		rule2.setEnabled(Boolean.FALSE);

		rule2.setUpdatedBy("李四");
		rule2.setUpdatedDate(FormatUtil.parseISO8601Date("2024-04-16T18:05:00.000Z", null));

		EntityUtils.copyNotUpdatableFields(rule1, rule2);
		assertEquals(2L, rule2.getVersion());
		assertSame(rule1.getCreatedDate(), rule2.getCreatedDate());
		assertEquals(FormatUtil.parseISO8601Date("2024-04-16T18:05:00.000Z", null), rule2.getUpdatedDate());
		assertSame(rule1.getCreatedBy(), rule2.getCreatedBy());
		assertEquals("李四", rule2.getUpdatedBy());
		assertEquals("修改过的默认规则", rule2.getName());
		assertEquals(Boolean.FALSE, rule2.getEnabled());
	}

}

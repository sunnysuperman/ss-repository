package com.sunnysuperman.repository.test;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.junit.jupiter.api.Test;

import com.sunnysuperman.commons.util.StringUtil;
import com.sunnysuperman.repository.ClassFinder;
import com.sunnysuperman.repository.ClassFinder.ClassFilter;
import com.sunnysuperman.repository.annotation.Entity;

public class ClassFinderTest {

	@Test
	public void test() throws Exception {
		ClassFilter filter = new ClassFilter() {

			@Override
			public boolean filter(Class<?> clazz) {
				return clazz.isAnnotationPresent(Entity.class);
			}

		};
		assertTrue(findClasses(null, filter).size() == 6);
		assertTrue(findClasses("", filter).size() == 6);
		assertTrue(findClasses("com", filter).size() == 5);
		assertTrue(findClasses("com.sunnysuperman", filter).size() == 4);
		assertTrue(findClasses("com.sunnysuperman.repository", filter).size() == 3);
		assertTrue(findClasses("com.sunnysuperman.repository.test", filter).size() == 2);
		assertTrue(findClasses("com.sunnysuperman.repository.test.entity", filter).size() == 2);
		assertTrue(findClasses("com.sunnysuperman.repository.test.entity.xx", filter).size() == 0);
		assertTrue(findClasses("com.notexists", filter).size() == 0);
	}

	private Set<Class<?>> findClasses(String packageName, ClassFilter filter) throws Exception {
		Set<Class<?>> classes = ClassFinder.find(packageName, filter);
		System.out.println("found classes: " + StringUtil.join(classes));
		return classes;
	}

}

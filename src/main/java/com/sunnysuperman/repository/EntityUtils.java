package com.sunnysuperman.repository;

import com.sunnysuperman.repository.db.EntityManager;

public class EntityUtils extends EntityManager {

	private EntityUtils() {
	}

	public static <T> void copyNotUpdatableFields(T src, T dest) {
		EntityManager.copyNotUpdatableFields(src, dest);
	}

}

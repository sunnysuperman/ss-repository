package com.sunnysuperman.repository;

import com.sunnysuperman.repository.db.EntityManager;

public class EntityUtils extends EntityManager {

	private EntityUtils() {
	}

	public static <T> void update(T latest, T original) {
		EntityManager.update(latest, original);
	}

}

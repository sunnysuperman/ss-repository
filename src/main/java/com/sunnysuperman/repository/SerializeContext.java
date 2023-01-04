package com.sunnysuperman.repository;

import java.util.Set;

import com.sunnysuperman.repository.db.DefaultFieldConverter;

public interface SerializeContext {

	Object getEntity();

	Set<String> getFields();

	InsertUpdate getInsertUpdate();

	DefaultFieldConverter getDefaultFieldConverter();

}

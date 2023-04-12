package com.sunnysuperman.repository;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface CRUDRepository<T, ID> {

	SaveResult save(T entity) throws RepositoryException;

	void insert(T entity) throws RepositoryException;

	void insertBatch(List<T> entityList) throws RepositoryException;

	boolean update(T entity) throws RepositoryException;

	boolean update(T entity, Set<String> fields) throws RepositoryException;

	void compareAndUpdateVersion(T entity) throws RepositoryException;

	boolean deleteById(ID id) throws RepositoryException;

	int deleteByIds(Collection<ID> ids) throws RepositoryException;

	boolean delete(T entity) throws RepositoryException;

	boolean existsById(ID id) throws RepositoryException;

	T findById(ID id) throws RepositoryException;

	List<T> findByIds(Collection<ID> ids) throws RepositoryException;

	List<T> findByIdsInOrder(Collection<ID> ids) throws RepositoryException;

	Map<ID, T> findByIdsAsMap(Collection<ID> ids) throws RepositoryException;

	List<T> findAll() throws RepositoryException;
}

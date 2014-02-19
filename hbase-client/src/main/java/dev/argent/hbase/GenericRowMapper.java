/*
 * @(#)GenericRowMapper.java $version 2014. 2. 19.
 * dev.argent.hbase.GenericRowMapper.java
 */
package dev.argent.hbase;

import java.lang.reflect.Constructor;

import org.apache.hadoop.hbase.client.Result;
import org.springframework.data.hadoop.hbase.HbaseUtils;
import org.springframework.data.hadoop.hbase.RowMapper;

/**
 * @author ddkkinf@naver.com
 */
public class GenericRowMapper<T extends KeyValueType> implements RowMapper<T> {
	private Class<T> clazz;

	public GenericRowMapper(Class<T> clazz) {
		this.clazz = clazz;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public T mapRow(Result result, int rowNum) throws Exception {
		try {
			Constructor<T> constructor = clazz.getConstructor();
			T newInstance = constructor.newInstance();
			newInstance.setKeyValueList(result.listCells());
			return newInstance;
		} catch (Exception e) {
			throw HbaseUtils.convertHbaseException(e);
		}
	}
}
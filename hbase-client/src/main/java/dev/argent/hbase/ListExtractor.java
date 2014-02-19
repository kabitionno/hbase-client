/*
 * @(#)ListExtractor.java $version 2014. 2. 19.
 * dev.argent.hbase.ListExtractor.java
 */
package dev.argent.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.springframework.data.hadoop.hbase.ResultsExtractor;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.util.Assert;

/**
 * @author ddkkinf@naver.com
 */
public class ListExtractor<T> implements ResultsExtractor<List<T>> {
	private RowMapper<T> rowMapper;
	
	public ListExtractor(RowMapper<T> rowMapper) {
		Assert.notNull(rowMapper, "RowMapper is required");
		this.rowMapper = rowMapper;
	}

	@Override
	public List<T> extractData(ResultScanner results) throws Exception {
		List<T> rs = new ArrayList<T>();
		int rowNum = 0;
		for (Result result : results) {
			rs.add(rowMapper.mapRow(result, rowNum++));
		}
		return rs;
	}
}
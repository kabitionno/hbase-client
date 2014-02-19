/*
 * @(#)HbaseTemplate.java $version 2014. 2. 19.
 * dev.argent.hbase.HbaseTemplate.java
 */
package dev.argent.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.data.hadoop.hbase.HbaseAccessor;
import org.springframework.data.hadoop.hbase.HbaseUtils;
import org.springframework.data.hadoop.hbase.ResultsExtractor;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.data.hadoop.hbase.TableCallback;
import org.springframework.util.Assert;

/**
 * @author ddkkinf@naver.com
 */
public class HbaseTemplate extends HbaseAccessor {
	private boolean autoFlush = false;

	public <T> T execute(String tableName, TableCallback<T> action) {
		Assert.notNull(action, "Callback object must not be null");
		Assert.notNull(tableName, "No table specified");

		HTableInterface table = getTable(tableName);
		try {
			boolean previousFlushSetting = applyFlushSetting(table);
			T result = action.doInTable(table);
			flushIfNecessary(table, previousFlushSetting);
			return result;
		} catch (Throwable th) {
			if (th instanceof Error) {
				throw ((Error) th);
			}
			if (th instanceof RuntimeException) {
				throw ((RuntimeException) th);
			}
			throw HbaseUtils.convertHbaseException((Exception) th);
		} finally {
			releaseTable(tableName, table);
		}
	}
	
	private HTableInterface getTable(String tableName) {
//		return hTablePool.getTable(tableName);
		return HbaseUtils.getHTable(tableName, getConfiguration(), getCharset(), getTableFactory());
	}

	private void releaseTable(String tableName, HTableInterface table) {
//		try {
//			table.close();
//		} catch (IOException e) {
//			log.warn(e.getMessage(), e);
//		}
		HbaseUtils.releaseTable(tableName, table, getTableFactory());
	}
	
	private boolean applyFlushSetting(HTableInterface table) {
		if (table instanceof HTable) {
			((HTable)table).setAutoFlushTo(this.autoFlush);
		}
		return autoFlush;
	}

	private void restoreFlushSettings(HTableInterface table, boolean oldFlush) {
		if (table instanceof HTable) {
			if (table.isAutoFlush() != oldFlush) {
				((HTable) table).setAutoFlushTo(oldFlush);
			}
		}
	}

	private void flushIfNecessary(HTableInterface table, boolean oldFlush) throws IOException {
		// TODO: check whether we can consider or not a table scope
		table.flushCommits();
		restoreFlushSettings(table, oldFlush);
	}
	
	public <T> List<T> scan(String tableName, RowMapper<T> action, Filter... filters) {
		return scan(tableName, null, null, action, filters);
	}
	public <T> List<T> scan(String tableName, byte[] family, RowMapper<T> action, Filter... filters) {
		return scan(tableName, family, null, action, filters);
	}
	public <T> List<T> scan(String tableName, HBaseColumn hbaseColumn, RowMapper<T> action, Filter... filters) {
		return scan(tableName, hbaseColumn.getFamily(), hbaseColumn.getQualifier(), action, filters);
	}
	public <T> List<T> scan(String tableName, List<HBaseColumn> hbaseColumns, RowMapper<T> action, Filter... filters) {
		Scan scan = new Scan();
		for (HBaseColumn hBaseColumn : hbaseColumns) {
			scan.addColumn(hBaseColumn.getFamily(), hBaseColumn.getQualifier());
		}
		return scan(tableName, scan, new ListExtractor<T>(action), filters);
	}
	public <T> List<T> scan(String tableName, byte[] family, byte[] qualifier, RowMapper<T> action, Filter... filters) {
		Scan scan = new Scan();
		if (family != null) {
			if (qualifier != null) {
				scan.addColumn(family, qualifier);
			} else {
				scan.addFamily(family);
			}
		}
		return scan(tableName, scan, new ListExtractor<T>(action), filters);
	}
	public <T> T scan(String tableName, final Scan scan, final ResultsExtractor<T> action, Filter... filters) {
		FilterList filterList = new FilterList();
		if (ArrayUtils.isNotEmpty(filters)) {
			for (Filter filter : filters) {
				if (filter instanceof PrefixFilter) {
					scan.setStartRow(((PrefixFilter)filter).getPrefix());
				}
				filterList.addFilter(filter);
			}
		}
		scan.setFilter(filterList);
		scan.setBatch(1000);
		scan.setCacheBlocks(true);
		scan.setCaching(1000);
		return execute(tableName, new TableCallback<T>() {
			@Override
			public T doInTable(HTableInterface htable) throws Throwable {
				ResultScanner scanner = htable.getScanner(scan);
				try {
					return action.extractData(scanner);
				} finally {
					scanner.close();
				}
			}
		});
	}
	
	public <T> T get(String tableName, final String rowName, final RowMapper<T> action) {
		return get(tableName, rowName, null, null, action);
	}
	public <T> T get(String tableName, final String rowName, final byte[] family, final RowMapper<T> action) {
		return get(tableName, rowName, family, null, action);
	}
	public <T> T get(String tableName, final String rowName, HBaseColumn hbaseColumn, final RowMapper<T> action) {
		return get(tableName, rowName, hbaseColumn.getFamily(), hbaseColumn.getQualifier(), action);
	}
	public <T> T get(String tableName, final String rowName, List<HBaseColumn> hbaseColumns, final RowMapper<T> action) {
		Get get = new Get(Bytes.toBytes(rowName));
		for (HBaseColumn hBaseColumn : hbaseColumns) {
			get.addColumn(hBaseColumn.getFamily(), hBaseColumn.getQualifier());
		}
		return get(tableName, get, action);
	}
	public <T> T get(String tableName, final String rowName, final byte[] family, final byte[] qualifier, final RowMapper<T> action) {
		Get get = new Get(Bytes.toBytes(rowName));
		if (family != null) {
			if (qualifier != null) {
				get.addColumn(family, qualifier);
			} else {
				get.addFamily(family);
			}
		}
		return get(tableName, get, action);
	}
	public <T> T get(String tableName, final Get get, final RowMapper<T> action) {
		return execute(tableName, new TableCallback<T>() {
			@Override
			public T doInTable(HTableInterface htable) throws Throwable {
				return action.mapRow(htable.get(get), 0);
			}
		});
	}
	
	public int put(KeyValueType keyValueType) {
		try {
			return batch((String)keyValueType.getClass().getMethod("getTableName").invoke(null), keyValueType.getKeyValueList(), Put.class);
		} catch (Exception e) {
			throw HbaseUtils.convertHbaseException(e);
		}
	}
	public int put(String tableName, KeyValueType keyValueType) {
		return batch(tableName, keyValueType.getKeyValueList(), Put.class);
	}
	public int put(String tableName, final List<KeyValueType> keyValueTypeList) {
		List<KeyValue> kvList = new ArrayList<KeyValue>();
		for (KeyValueType keyValueType : keyValueTypeList) {
			kvList.addAll(keyValueType.getKeyValueList());
		}
		return batch(tableName, kvList, Put.class);
	}
	public int batch(String tableName, final List<KeyValue> keyValueList, Class<? extends Row> clazz) {
		return execute(tableName, new BatchTableCallback(keyValueList, new RowCallback() {
			@Override
			public void doInIteration(List<Row> rowList, KeyValue keyValue) throws IOException {
				Put put = new Put(keyValue.getRowArray());
				put.add(keyValue);
				rowList.add(put);
			}
		}));
	}
	
	public void delete(String tableName, final String rowName) {
		delete(tableName, rowName, null, null);
	}
	public void delete(String tableName, final String rowName, final byte[] family) {
		delete(tableName, rowName, family, null);
	}
	public void delete(String tableName, final String rowName, final byte[] family, final byte[] qualifier) {
		execute(tableName, new TableCallback<Integer>() {
			@Override
			public Integer doInTable(HTableInterface table) throws Throwable {
				Delete delete = new Delete(Bytes.toBytes(rowName));
				if (family != null) {
					if (qualifier != null) {
						delete.deleteColumn(family, qualifier);
					} else {
						delete.deleteFamily(family);
					}
				}
				table.delete(delete);
				return 0;
			}
		});
	}
	
	public class BatchTableCallback implements TableCallback<Integer> {
		private final List<KeyValue> keyValueList;
		private final RowCallback action;

		/**
		 * @param keyValueList
		 */
		public BatchTableCallback(List<KeyValue> keyValueList, RowCallback action) {
			this.keyValueList = keyValueList;
			this.action = action;
		}

		@Override
		public Integer doInTable(HTableInterface table) throws Throwable {
			List<Row> rowList = new ArrayList<Row>();
			for (KeyValue keyValue : keyValueList) {
				action.doInIteration(rowList, keyValue);
			}
			table.batch(rowList);
			return rowList.size();
		}
	}
	
	public interface RowCallback {
		public void doInIteration(List<Row> rowList, KeyValue keyValue) throws IOException;
	}
}

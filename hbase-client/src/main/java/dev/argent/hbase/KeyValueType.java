/*
 * @(#)KeyValueType.java $version 2014. 2. 19.
 * dev.argent.hbase.KeyValueType.java
 */
package dev.argent.hbase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author ddkkinf@naver.com
 */
public abstract class KeyValueType {
	protected static String TABLE_NAME = null;
	
	public KeyValueType() {
		super();
	}
	
	public static String getTableName() {
		throw new UnsupportedOperationException("TableName Not Specified.");
	}
	
	abstract protected byte[] getRow();
	abstract protected byte[] getValue(byte[] family, byte[] qualifier);
	abstract protected long getTimeStamp();
	abstract protected void setRow(byte[] row);
	abstract protected void setValue(byte[] family, byte[] qualifier, byte[] value);
	abstract protected void setTimeStamp(long timeStamp);
	abstract protected HBaseColumn[] getColumns();
	
	public List<KeyValue> getKeyValueList(HBaseColumn... hbaseColumns) {
		List<KeyValue> kvList = new ArrayList<KeyValue>();
		for (HBaseColumn hBaseColumn : hbaseColumns) {
			kvList.add(new KeyValue(getRow(), hBaseColumn.getFamily(), hBaseColumn.getQualifier(), getTimeStamp(), getValue(hBaseColumn.getFamily(), hBaseColumn.getQualifier())));
		}
		return kvList;
	}
	public List<KeyValue> getKeyValueList() {
		return getKeyValueList(getColumns());
	}
	protected void setKeyValueList(List<Cell> kvList) {
		boolean rowRequired = true;
		for (Cell keyValue : kvList) {
			if (rowRequired) {
				setRow(keyValue.getRowArray());
				rowRequired = false;
			}
			setValue(keyValue.getFamilyArray(), keyValue.getQualifierArray(), keyValue.getValueArray());
			setTimeStamp(keyValue.getTimestamp());
		}
	}
	
	protected <E extends Enum<E> & HBaseColumn> E getHBaseColumn(Class<E> clazz, byte[] family, byte[] qualifier) {
		for (E hbaseColumn : (EnumSet<E>)EnumSet.allOf(clazz)) {
			if (Arrays.equals(family, hbaseColumn.getFamily()) &&
				Arrays.equals(qualifier, hbaseColumn.getQualifier())) {
				return hbaseColumn;
			}
		}
		throw new IllegalArgumentException("No enum const " + HBaseColumn.class + "." + Bytes.toString(family) + Bytes.toString(qualifier));
	}
}

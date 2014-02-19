/*
 * @(#)HbaseColumn.java $version 2014. 2. 19.
 * dev.argent.hbase.HbaseColumn.java
 */
package dev.argent.hbase;

/**
 * @author ddkkinf@naver.com
 */
public interface HBaseColumn {
	public byte[] getFamily();
	public byte[] getQualifier();
}

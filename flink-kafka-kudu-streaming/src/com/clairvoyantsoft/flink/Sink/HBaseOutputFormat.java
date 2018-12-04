package com.clairvoyantsoft.flink.Sink;

import java.io.IOException;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseOutputFormat implements OutputFormat<String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3294495525029550656L;
	
	private org.apache.hadoop.conf.Configuration conf = null;
	private HTable table = null;
	private String taskNumber = null;
	private int rowNumber = 0;
	private String tableName = null;
	private String[] columns;
	private String dbUrl = null;

	public HBaseOutputFormat(String dbUrl, String tableName, String[] columns) {
		this.tableName = tableName;
		this.columns = columns;
		this.dbUrl = dbUrl;
	}

	@Override
	public void configure(Configuration parameters) {
		conf = HBaseConfiguration.create();
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		table = new HTable(conf, tableName);
		this.taskNumber = String.valueOf(taskNumber);
	}

	@Override
	public void writeRecord(String record) throws IOException {
		Put put = new Put(Bytes.toBytes(taskNumber + rowNumber));
		System.out.println("row " + taskNumber + rowNumber);
		System.out.println("record " + record);
		if (record.contains("|")) {
			Integer i = 0;
			for (String s : record.split("\\|")) {
				System.out.println("s " + s);
				if (i == 0)
					put.add(Bytes.toBytes("id"), Bytes.toBytes(columns[i]), Bytes.toBytes(s));
				if (i == 1)
					put.add(Bytes.toBytes("time"), Bytes.toBytes(columns[i]), Bytes.toBytes(new Long(s)));
				if (i == 2)
					put.add(Bytes.toBytes("data"), Bytes.toBytes(columns[i]), Bytes.toBytes(new Double(s)));
				if (i == 3)
					put.add(Bytes.toBytes("data"), Bytes.toBytes(columns[i]), Bytes.toBytes(new Double(s)));
				i++;
			}
		}

		rowNumber++;
		table.put(put);
	}

	@Override
	public void close() throws IOException {
		table.flushCommits();
		table.close();
	}

}

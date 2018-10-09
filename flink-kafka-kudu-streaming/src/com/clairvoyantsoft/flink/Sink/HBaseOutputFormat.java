package com.clairvoyantsoft.flink.Sink;

import java.io.IOException;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseOutputFormat implements OutputFormat<String> {

	private org.apache.hadoop.conf.Configuration conf = null;
	private HTable table = null;
	private String taskNumber = null;
	private int rowNumber = 0;

	private static final long serialVersionUID = 1L;

	@Override
	public void configure(Configuration parameters) {
		conf = HBaseConfiguration.create();
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		table = new HTable(conf, "temp_humidity");
		this.taskNumber = String.valueOf(taskNumber);
	}

	@Override
	public void writeRecord(String record) throws IOException {
		Put put = new Put(Bytes.toBytes(taskNumber + rowNumber));

		System.out.println("record " + record);
		if (record.contains("|")) {
			Integer i = 0;
			for (String s : record.split("\\|")) {
				System.out.println("s " + s);
				if (i == 0)
					put.add(Bytes.toBytes("deviceId"), Bytes.toBytes("macId"), Bytes.toBytes(s));
				if (i == 1)
					put.add(Bytes.toBytes("time"), Bytes.toBytes("long"), Bytes.toBytes(new Long(s)));
				if (i == 2)
					put.add(Bytes.toBytes("temperature"), Bytes.toBytes("temp_c"), Bytes.toBytes(new Double(s)));
				if (i == 3)
					put.add(Bytes.toBytes("humidity"), Bytes.toBytes("humidity"), Bytes.toBytes(new Double(s)));
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

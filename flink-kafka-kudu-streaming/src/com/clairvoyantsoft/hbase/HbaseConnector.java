package com.clairvoyantsoft.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ServiceException;

public class HbaseConnector {

	Configuration config = HBaseConfiguration.create();

	public static void main(String[] args) throws ServiceException {
		String tablename = "temp_humidity";
		String[] familys = { "deviceId", "time", "temperature", "humidity" };
		HbaseConnector hbc = new HbaseConnector();
		try {
			// hbc.createHbaseTable(tablename, familys);
			//hbc.insertData(tablename, familys);
			hbc.readData(tablename, familys);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void createHbaseTable(String name, String[] colfamily)
			throws MasterNotRunningException, ZooKeeperConnectionException, IOException, ServiceException {
		HBaseAdmin admin = new HBaseAdmin(config);
		admin.checkHBaseAvailable(config);

		HTableDescriptor des = new HTableDescriptor(Bytes.toBytes(name));
		for (int i = 0; i < colfamily.length; i++) {
			des.addFamily(new HColumnDescriptor(colfamily[i]));
		}
		if (admin.tableExists(name)) {
			System.out.println("Table already exist");
		} else {
			admin.createTable(des);
			System.out.println("Table: " + name + " Sucessfully created");
		}

	}

	public void insertData(String name, String[] colfamily)
			throws MasterNotRunningException, ZooKeeperConnectionException, IOException, ServiceException {
		HTable table = new HTable(config, name);
		// create the put object
		Put put = new Put(Bytes.toBytes("row-1"));
		// Add the column into the column family Emp_name with qualifier name
		put.add(Bytes.toBytes("deviceId"), Bytes.toBytes("macId"), Bytes.toBytes("Kiran"));
		// Add the column into the column family sal with qualifier name
		put.add(Bytes.toBytes("time"), Bytes.toBytes("long"), Bytes.toBytes("100000"));
		put.add(Bytes.toBytes("temperature"), Bytes.toBytes("temp_c"), Bytes.toBytes("100000"));
		put.add(Bytes.toBytes("humidity"), Bytes.toBytes("humidity"), Bytes.toBytes("100000"));
		// insert the put instance to table
		table.put(put);
		System.out.println("Values inserted : "); 
		table.close();

	}

	public void readData(String name, String[] colfamily)
			throws MasterNotRunningException, ZooKeeperConnectionException, IOException, ServiceException {
		HTable table = new HTable(config, name);
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("deviceId"), Bytes.toBytes("macId"));
		scan.addColumn(Bytes.toBytes("time"), Bytes.toBytes("long"));
		scan.addColumn(Bytes.toBytes("temperature"), Bytes.toBytes("temp_c"));
		scan.addColumn(Bytes.toBytes("humidity"), Bytes.toBytes("humidity"));
		//scan.setStartRow(Bytes.toBytes("row-1"));
		// scan.setStartRow(Bytes.toBytes("row-4"));
		ResultScanner result = table.getScanner(scan);
		for (Result res : result) {
			byte[] val = res.getValue(Bytes.toBytes("deviceId"), Bytes.toBytes("macId"));
			System.out.println("Row-value:" + Bytes.toString(val));
		}
		table.close();
	}

}

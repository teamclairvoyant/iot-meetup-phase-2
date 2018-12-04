package com.clairvoyantsoft.hbase;

import java.io.IOException;
import java.util.Date;

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
		String[] familys = { "id", "time", "data" };
		HbaseConnector hbc = new HbaseConnector();
		try {
			//hbc.createHbaseTable(tablename, familys);
			hbc.insertData(tablename, familys);
			hbc.readData(tablename, familys);
			//hbc.deleteTable(tablename);
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
		long date =  1539847058982l;
		for (int i = 0; i < 10000; i++) {
		// create the put object
		Put put = new Put(Bytes.toBytes("row_"+i));
		// Add the column into the column family Emp_name with qualifier name
		put.add(Bytes.toBytes("id"), Bytes.toBytes("deviceId"), Bytes.toBytes("device_1"));
		// Add the column into the column family sal with qualifier name
		date =  date+60000l;
		 System.out.println("date "+date);
		put.add(Bytes.toBytes("time"), Bytes.toBytes("time"), Bytes.toBytes(date));
		put.add(Bytes.toBytes("data"), Bytes.toBytes("temperature"), Bytes.toBytes(new Double("10").doubleValue()));
		put.add(Bytes.toBytes("data"), Bytes.toBytes("humidity"), Bytes.toBytes(new Double("20").doubleValue()));
		// insert the put instance to table
		table.put(put);
		}
		System.out.println("Values inserted : "); 
		table.close();

	}

	public void readData(String name, String[] colfamily)
			throws MasterNotRunningException, ZooKeeperConnectionException, IOException, ServiceException {
		HTable table = new HTable(config, name);
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("id"), Bytes.toBytes("deviceId"));
		scan.addColumn(Bytes.toBytes("time"), Bytes.toBytes("time"));
		scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("temperature"));
		scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("humidity"));
		//scan.setStartRow(Bytes.toBytes("row-1"));
		// scan.setStartRow(Bytes.toBytes("row-4"));
		ResultScanner result = table.getScanner(scan);
		for (Result res : result) {
			byte[] val1 = res.getValue(Bytes.toBytes("id"), Bytes.toBytes("deviceId"));
			byte[] val2 = res.getValue(Bytes.toBytes("data"), Bytes.toBytes("temperature"));
			System.out.println("Row-value:" + Bytes.toString(val1)+ " "+Bytes.toDouble(val2));
		}
		table.close();
	}
	
	public void deleteTable(String name) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		HBaseAdmin admin = new HBaseAdmin(config);

	      // disabling table named emp
	      admin.disableTable(name);

	      // Deleting emp
	      admin.deleteTable(name);
	      System.out.println("Table deleted");
	}

}

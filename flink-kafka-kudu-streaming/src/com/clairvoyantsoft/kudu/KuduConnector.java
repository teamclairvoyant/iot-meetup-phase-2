package com.clairvoyantsoft.kudu;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

public class KuduConnector {

	/*
	 * private static final String KUDU_MASTER = System.getProperty( "kuduMaster",
	 * "quickstart.cloudera");
	 */

	public static void main(String[] args) {
		System.out.println("-----------------------------------------------");
		System.out.println("Will try to connect to Kudu master at " + "quickstart.cloudera");
		System.out.println("Run with -DkuduMaster=myHost:port to override.");
		System.out.println("-----------------------------------------------");
		String tableName = "temp_humidity";
		KuduClient client = new KuduClient.KuduClientBuilder("quickstart.cloudera").build();

		try {

			
/*
			List<ColumnSchema> columns = new ArrayList(4);
			columns.add(
					new ColumnSchema.ColumnSchemaBuilder("deviceId", Type.STRING).key(true).nullable(false).build());
			columns.add(new ColumnSchema.ColumnSchemaBuilder("time", Type.INT64).key(true).nullable(false)
					.build());
			columns.add(new ColumnSchema.ColumnSchemaBuilder("temperature", Type.DOUBLE).build());
			columns.add(new ColumnSchema.ColumnSchemaBuilder("humidity", Type.DOUBLE).build());

			List<String> rangeKeys = new ArrayList<>();
			rangeKeys.add("time");

			Schema schema = new Schema(columns);

			client.createTable(tableName, schema,
					new CreateTableOptions().setRangePartitionColumns(rangeKeys).setNumReplicas(1));

			System.out.println("tables "+
					 client.getTablesList().getTablesList().get(0).toString());*/
			
			KuduTable table = client.openTable(tableName);

			KuduSession session = client.newSession();
			long date =  1539722092000l;
			for (int i = 0; i < 3000; i++) {
				Insert insert = table.newInsert();
				PartialRow row = insert.getRow();
				row.addString(0, "device_1");
				 date =  date+60000l;
				 System.out.println("date "+date);
				row.addLong(1, date);
				row.addDouble(2, 10);
				row.addDouble(3, 20);
				session.apply(insert);
			}

			List<String> projectColumns = new ArrayList<>(1);
			projectColumns.add("deviceId");
			projectColumns.add("time");
			projectColumns.add("temperature");
			projectColumns.add("humidity");
			KuduScanner scanner = client.newScannerBuilder(table).setProjectedColumnNames(projectColumns).build();
			while (scanner.hasMoreRows()) {
				RowResultIterator results = scanner.nextRows();
				while (results.hasNext()) {
					RowResult result = results.next();
					System.out.println(result.getString(0) + " " + new Date(result.getLong(1))+" "+result.getDouble(2)+" "+result.getDouble(3));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				//client.deleteTable(tableName);
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					client.shutdown();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
}

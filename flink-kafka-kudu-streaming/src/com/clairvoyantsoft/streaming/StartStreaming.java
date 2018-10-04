package com.clairvoyantsoft.streaming;

import com.clairvoyantsoft.propertyloader.PropertyLoader;

public class StartStreaming {

	public static void main(String[] args) throws Exception {

		String checkPointPath = PropertyLoader.getInstance().getPropValues("checkPointPath");
		String zookeeperConnect = PropertyLoader.getInstance().getPropValues("zookeeperConnect");
		String bootstrapServers = PropertyLoader.getInstance().getPropValues("bootstrapServers");
		String groupId = PropertyLoader.getInstance().getPropValues("groupId");
		String kafkaTopic = PropertyLoader.getInstance().getPropValues("kafkaTopic");
		String dbURL = PropertyLoader.getInstance().getPropValues("dbUrl");
		String table = PropertyLoader.getInstance().getPropValues("table");
		String[] columns = PropertyLoader.getInstance().getPropValues("columns").split("\\,");

		StreamCreator.getInstance(checkPointPath, zookeeperConnect, bootstrapServers, groupId, kafkaTopic, dbURL, table,
				columns).startStreaming();

	}

}

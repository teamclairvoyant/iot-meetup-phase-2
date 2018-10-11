package com.clairvoyantsoft.stream;

public interface Stream {

	public void createStream(String checkPointPath, String zookeepeConnect, String bootstrapServers, String groupId,
			String kafkaTopic, String dbURL, String table, String[] cloumns);

	public void startStreaming();
}

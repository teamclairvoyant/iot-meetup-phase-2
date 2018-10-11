package com.clairvoyantsoft.stream.impl;

import java.util.Properties;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import com.clairvoyantsoft.flink.Sink.HBaseOutputFormat;
import com.clairvoyantsoft.stream.Stream;

public class HbaseStream implements Stream {

	private final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

	@Override
	public void createStream(String checkPointPath, String zookeepeConnect, String bootstrapServers, String groupId,
			String kafkaTopic, String dbURL, String table, String[] cloumns) {
		try {

			System.out.println("Setting properties for Hbase Stream");

			// use filesystem based state management
			env.setStateBackend(new FsStateBackend(checkPointPath));
			// checkpoint works fine if Flink is crashing but does not seem to work if job
			// is restarted?
			env.enableCheckpointing(1000);

			Properties props = new Properties();
			props.setProperty("zookeeper.connect", zookeepeConnect);
			props.setProperty("bootstrap.servers", bootstrapServers);
			// not to be shared with another job consuming the same topic
			props.setProperty("group.id", groupId);
			// props.setProperty("auto.offset.reset", "earliest");

			FlinkKafkaConsumer011<String> kafkaConsumer = new FlinkKafkaConsumer011<>(kafkaTopic,
					// true means we include metadata like topic name not
					// necessarily useful for this very example
					new SimpleStringSchema(), props);

			DataStream<String> stream = env.addSource(kafkaConsumer);

			stream.writeUsingOutputFormat(new HBaseOutputFormat(dbURL, table, cloumns));

		} catch (Exception e) {
			e.printStackTrace();
		}

		/*
		 * stream.map(new MapFunction<String, String>() {
		 * 
		 *//**
			* 
			*//*
				 * private static final long serialVersionUID = 4113133525228027896L;
				 * 
				 * @Override public String map(String data) throws Exception {
				 * System.out.println("data " + data); return data.toLowerCase(); } });
				 */

		/*
		 * FlinkKafkaProducer011 kafkaProducer = new
		 * FlinkKafkaProducer011<>("localhost:9092", "output", new
		 * SimpleStringSchema());
		 * 
		 * stream.addSink(kafkaProducer);
		 */

	}

	@Override
	public void startStreaming() {
		try {
			System.out.println("Starting Hbase Stream");
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}

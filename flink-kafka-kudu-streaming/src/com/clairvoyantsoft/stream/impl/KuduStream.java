package com.clairvoyantsoft.stream.impl;

import java.util.Date;
import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import com.clairvoyantsoft.flink.Sink.KuduSink;
import com.clairvoyantsoft.flink.Utils.RowSerializable;
import com.clairvoyantsoft.flink.Utils.Exceptions.KuduClientException;
import com.clairvoyantsoft.stream.Stream;

public class KuduStream implements Stream {

	private final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

	@Override
	public void createStream(String checkPointPath, String zookeepeConnect, String bootstrapServers, String groupId,
			String kafkaTopic, String dbURL, String table, String[] cloumns) {
		try {

			System.out.println("Setting properties for Kudu Stream");

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

			DataStream<RowSerializable> stream2 = stream.map(new MyMapFunction());
			stream2.addSink(new KuduSink(dbURL, table, cloumns));

		} catch (KuduClientException e) {
			// TODO Auto-generated catch block
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
			System.out.println("Starting Kudu Stream");
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private static class MyMapFunction implements MapFunction<String, RowSerializable> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 5977935345003384044L;

		@Override
		public RowSerializable map(String input) throws Exception {

			System.out.println("input " + input);
			RowSerializable res = new RowSerializable(4);
			if (input.contains("|")) {
				Integer i = 0;
				for (String s : input.split("\\|")) {
					System.out.println("s " + s);
					if (i == 0)
						res.setField(0, s);
					if (i == 1)
						res.setField(1, new Long(s));
					if (i == 2)
						res.setField(2, new Double(s));
					if (i == 3)
						res.setField(3, new Double(s));
					i++;
				}
			} else {
				res.setField(0, "001");
				res.setField(1, new Date().getTime());
				res.setField(2, new Double("12"));
				res.setField(3, new Double("67"));
			}

			return res;
		}
	}

}

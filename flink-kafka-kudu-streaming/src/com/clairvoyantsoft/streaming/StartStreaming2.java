package com.clairvoyantsoft.streaming;

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

public class StartStreaming2 {

	public static void main(String[] args) throws Exception {

		System.out.println("Starting application");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// use filesystem based state management
		env.setStateBackend(new FsStateBackend("file:///tmp/flink/checkpoints"));
		// checkpoint works fine if Flink is crashing but does not seem to work if job
		// is restarted?
		env.enableCheckpointing(1000);

		Properties props = new Properties();
		props.setProperty("zookeeper.connect", "localhost:2181");
		props.setProperty("bootstrap.servers", "localhost:9092");
		// not to be shared with another job consuming the same topic
		props.setProperty("group.id", "flink-group");
		// props.setProperty("auto.offset.reset", "earliest");

		FlinkKafkaConsumer011<String> kafkaConsumer = new FlinkKafkaConsumer011<>("test",
				// true means we include metadata like topic name not
				// necessarily useful for this very example
				new SimpleStringSchema(), props);

		DataStream<String> stream = env.addSource(kafkaConsumer);

		// stream.writeAsText("file:///D:/tmp/out.txt");
		// stream.print();

		DataStream<RowSerializable> stream2 = stream.map(new MyMapFunction());

		String[] cloumns = { "deviceId", "time", "temperature", "humidity" };

		stream2.addSink(new KuduSink("127.0.0.1", "temp_humidity", cloumns));

		// to send data back to Kafka
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

		env.execute();
	}

	private static class MyMapFunction implements MapFunction<String, RowSerializable> {

		@Override
		public RowSerializable map(String input) throws Exception {

			System.out.println("input " + input);
			RowSerializable res = new RowSerializable(4);
			if (input.contains("|")) {
				Integer i = 0;
				for (String s : input.split("\\|")) {
					System.out.println("s "+s);
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

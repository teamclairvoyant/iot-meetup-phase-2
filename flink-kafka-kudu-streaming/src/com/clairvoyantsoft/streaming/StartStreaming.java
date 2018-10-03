package com.clairvoyantsoft.streaming;

import java.util.Date;
import java.util.Properties;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import com.clairvoyantsoft.flink.Sink.KuduSink;
import com.clairvoyantsoft.flink.Utils.RowSerializable;

public class StartStreaming {

	public static class AverageAccumulator {
		String key;
		long count;
		long sum;
	}

	// how can I clean the aggregate state if I know I won't receive any more events
	// for it
	public static class Average implements AggregateFunction<ObjectNode, AverageAccumulator, Tuple2> {
		@Override
		public AverageAccumulator createAccumulator() {
			return new AverageAccumulator();
		}

		@Override
		public AverageAccumulator add(ObjectNode value, AverageAccumulator accumulator) {
			// a little bit weird but we need to keep the key as part of the accumulator to
			// be able
			// to serialize it back in the end
			System.out.println("key " + value.get("key") + " value " + value.get("value"));

			accumulator.key = "key";
			accumulator.sum += 1l;
			accumulator.count++;
			return accumulator;
		}

		@Override
		public Tuple2<String, Double> getResult(AverageAccumulator accumulator) {
			return new Tuple2<>(accumulator.key, accumulator.sum / (double) accumulator.count);
		}

		@Override
		public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
			a.count += b.count;
			a.sum += b.sum;
			return a;
		}
	}

	public static class AverageSerializer implements KeyedSerializationSchema<Tuple2> {
		@Override
		public byte[] serializeKey(Tuple2 element) {
			return ("\"" + element.getField(0).toString() + "\"").getBytes();
		}

		@Override
		public byte[] serializeValue(Tuple2 element) {
			String value = "{\"avg\": " + element.getField(1).toString() + "}";
			return value.getBytes();
		}

		@Override
		public String getTargetTopic(Tuple2 element) {
			// use always the default topic
			return null;
		}
	}

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

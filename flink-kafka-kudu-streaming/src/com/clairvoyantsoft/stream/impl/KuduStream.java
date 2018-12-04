package com.clairvoyantsoft.stream.impl;

import java.util.Date;
import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import com.clairvoyantsoft.flink.Sink.KuduSink;
import com.clairvoyantsoft.flink.Utils.RowSerializable;
import com.clairvoyantsoft.flink.Utils.Exceptions.KuduClientException;
import com.clairvoyantsoft.stream.Stream;

public class KuduStream implements Stream {

	private final StreamExecutionEnvironment env = StreamExecutionEnvironment
			.getExecutionEnvironment();

	@Override
	public void createStream(String checkPointPath, String zookeepeConnect,
			String bootstrapServers, String groupId, String kafkaTopic,
			String dbURL, String table, String[] cloumns) {
		try {

			System.out.println("Setting properties for Kudu Stream");

			// use filesystem based state management
			env.setStateBackend(new FsStateBackend(checkPointPath));
			// checkpoint works fine if Flink is crashing but does not seem to
			// work if job
			// is restarted?
			env.enableCheckpointing(1000);

			Properties props = new Properties();
			props.setProperty("zookeeper.connect", zookeepeConnect);
			props.setProperty("bootstrap.servers", bootstrapServers);
			// not to be shared with another job consuming the same topic
			props.setProperty("group.id", groupId);
			// props.setProperty("auto.offset.reset", "earliest");

			FlinkKafkaConsumer011<String> kafkaConsumer = new FlinkKafkaConsumer011<>(
					kafkaTopic,
					// true means we include metadata like topic name not
					// necessarily useful for this very example
					new SimpleStringSchema(), props);

			DataStream<String> stream = env.addSource(kafkaConsumer);

			// stream.timeWindowAll(Time.minutes(2));
			stream.map(new MyMapFunction())
					.timeWindowAll(Time.minutes(2))
					.apply(new AllWindowFunction<Tuple4<String, Long, Double, Double>, RowSerializable, TimeWindow>() {

						/**
						 * 
						 */
						private static final long serialVersionUID = -3471258823198359335L;

						@Override
						public void apply(
								TimeWindow window,
								Iterable<Tuple4<String, Long, Double, Double>> inIterator,
								Collector<RowSerializable> out)
								throws Exception {
							
							for (Tuple4<String, Long, Double, Double> in : inIterator) {
								
								System.out
								.println(" ##################################### "+in.toString());
							
								RowSerializable res = new RowSerializable(4);

								res.setField(0, in.f0);
								res.setField(1, in.f1);
								res.setField(2, in.f2);
								res.setField(3, in.f3);
								out.collect(res);
							}
						}
					}).addSink(new KuduSink(dbURL, table, cloumns));

			/*
			 * DataStream<RowSerializable> stream2 = stream.map(new
			 * MyMapFunction()); //stream2.timeWindowAll(Time.minutes(1));
			 * stream2.addSink(new KuduSink(dbURL, table, cloumns));
			 */

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/*
		 * stream.map(new MapFunction<String, String>() {
		 *//**
			* 
			*/
		/*
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

	private static class MyMapFunction implements
			MapFunction<String, Tuple4<String, Long, Double, Double>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 5977935345003384044L;

		@Override
		public Tuple4<String, Long, Double, Double> map(String input)
				throws Exception {

			//System.out.println("input " + input);
			Tuple4<String, Long, Double, Double> in = new Tuple4<String, Long, Double, Double>();
			if (input.contains("|")) {
				Integer i = 0;
				for (String s : input.split("\\|")) {
					//System.out.println("s " + s);
					if (i == 0)
						in.setField(s, i);
					if (i == 1)
						in.setField(new Long(s), i);
					if (i == 2)
						in.setField(new Double(s), i);
					if (i == 3)
						in.setField(new Double(s), i);
					i++;
				}
			}

			return in;
		}
	}

}

package com.clairvoyantsoft.kafka;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerTest {

	public static void main(String[] args) throws InterruptedException {
		final Producer<Long, String> producer = ProducerCreator.createProducer();
	    long time = System.currentTimeMillis();
	    final CountDownLatch countDownLatch = new CountDownLatch(10);

	    try {
	        for (long index = time; index < time + 10; index++) {
	            final ProducerRecord<Long, String> record =
	                    new ProducerRecord<>("test", index, "device_001|" + new Date().getTime() + "|" + 33.4 + "|"
	        					+ 88.0);
	            producer.send(record, (metadata, exception) -> {
	                long elapsedTime = System.currentTimeMillis() - time;
	                if (metadata != null) {
	                    System.out.printf("sent record(key=%s value=%s) " +
	                                    "meta(partition=%d, offset=%d) time=%d\n",
	                            record.key(), record.value(), metadata.partition(),
	                            metadata.offset(), elapsedTime);
	                } else {
	                    exception.printStackTrace();
	                }
	                countDownLatch.countDown();
	            });
	        }
	        countDownLatch.await(25, TimeUnit.SECONDS);
	    }finally {
	        producer.flush();
	        producer.close();
	    }
	}
	
}

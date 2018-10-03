package com.clairvoyantsoft.bridge;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.kura.core.cloud.CloudPayloadProtoBufDecoderImpl;
import org.eclipse.kura.message.KuraPayload;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.clairvoyantsoft.kafka.ProducerCreator;

public class MessageReceiver implements MqttCallback {

	final Producer<Long, String> producer = ProducerCreator.createProducer();

	@Override
	public void connectionLost(Throwable cause) {
		// TODO Auto-generated method stub

	}

	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		byte[] payload = message.getPayload();

		try {

			final CloudPayloadProtoBufDecoderImpl decoder = new CloudPayloadProtoBufDecoderImpl(payload);

			KuraPayload kuraPayload = decoder.buildFromByteArray();

			Object humidityReading = kuraPayload.getMetric("HumidityReading");
			Object temperatureReading = kuraPayload.getMetric("TemperatureReading");
			String humidity = String.valueOf(humidityReading);
			String temperature = String.valueOf(temperatureReading);

			String recordString = "device_001|" + kuraPayload.getTimestamp().getTime() + "|" + temperature + "|"
					+ humidity;

			System.out.println("recordString " + recordString);

			final ProducerRecord<Long, String> record = new ProducerRecord<>("test", recordString);
			producer.send(record, (metadata, exception) -> {
				long elapsedTime = System.currentTimeMillis() - kuraPayload.getTimestamp().getTime();
				if (metadata != null) {
					System.out.printf("sent record(value=%s) " + "meta(partition=%d, offset=%d) time=%d\n",
							record.value(), metadata.partition(), metadata.offset(), elapsedTime);
				} else {
					exception.printStackTrace();
				}
			});

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		// TODO Auto-generated method stub

	}

}

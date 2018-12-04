package com.clairvoyantsoft.bridge;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.clairvoyantsoft.mqtt.MessageReceiver;

public class MqttConsumer {

	public static void main(String[] args) {

		String topic = "kapua-sys/Meetup_Kapua/DHT11Sensor/DHTMeetup22";
		String content = "Message from MqttPublishSample";
		int qos = 2;
		String broker = "tcp://192.168.32.190:1883";
		String clientId = "JavaSample";
		MemoryPersistence persistence = new MemoryPersistence();

		try {
			MqttClient sampleClient = new MqttClient(broker, clientId, persistence);
			MqttConnectOptions connOpts = new MqttConnectOptions();
			connOpts.setUserName("kapua-broker");
			connOpts.setPassword("kapua-password".toCharArray());
			connOpts.setCleanSession(true);
			System.out.println("Connecting to broker: " + broker);
			sampleClient.setCallback(new MessageReceiver());
			sampleClient.connect(connOpts);
			System.out.println("Connected");
			/*
			 * System.out.println("Publishing message: "+content); MqttMessage message = new
			 * MqttMessage(content.getBytes()); message.setQos(qos);
			 * sampleClient.publish(topic, message);
			 * System.out.println("Message published");
			 */
			// sampleClient.disconnect();
			sampleClient.subscribe(topic);
			// System.out.println("Disconnected");
			// System.exit(0);
		} catch (MqttException me) {
			System.out.println("reason " + me.getReasonCode());
			System.out.println("msg " + me.getMessage());
			System.out.println("loc " + me.getLocalizedMessage());
			System.out.println("cause " + me.getCause());
			System.out.println("excep " + me);
			me.printStackTrace();
		}
	}
}
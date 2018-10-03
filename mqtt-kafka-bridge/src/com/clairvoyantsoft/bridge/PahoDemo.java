package com.clairvoyantsoft.bridge;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class PahoDemo implements MqttCallback {

	MqttClient client;

	public PahoDemo() {
	}

	public static void main(String[] args) {
		new PahoDemo().doDemo();
	}

	MemoryPersistence persistence = new MemoryPersistence();

	public void doDemo() {
		try {
			client = new MqttClient("tcp://192.168.34.124:1883", "kapua-sys/test", persistence);
			MqttConnectOptions connOpts = new MqttConnectOptions();
			connOpts.setCleanSession(true);
			connOpts.setUserName("kapua-broker");
			connOpts.setPassword("kapua-password".toCharArray());
			System.out.println("Connecting to broker: ");
			client.connect(connOpts);
			client.setCallback(this);
			client.connect();
			// client.subscribe("kapua-sys/Meetup_kapua/DHT11Sensor/DHT11TechTalk");
			// kapua-sys/Meetup_kapua/
			/*
			 * MqttMessage message = new MqttMessage();
			 * message.setPayload("A single message from my computer fff" .getBytes());
			 * client.publish("foo", message);
			 */
		} catch (MqttException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void connectionLost(Throwable cause) {
		// TODO Auto-generated method stub

	}

	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		System.out.println(message);
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		// TODO Auto-generated method stub

	}

}
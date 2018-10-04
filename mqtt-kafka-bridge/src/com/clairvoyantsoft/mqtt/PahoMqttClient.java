package com.clairvoyantsoft.mqtt;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.clairvoyantsoft.propertyloader.PropertyLoader;

public class PahoMqttClient {

	private MqttClient client;

	private final MemoryPersistence persistence = new MemoryPersistence();

	private static PahoMqttClient simpleMqttClient = null;

	private PahoMqttClient() {
		createMqttClient();
	}

	public static PahoMqttClient getInstance() {

		if (simpleMqttClient == null)
			simpleMqttClient = new PahoMqttClient();

		return simpleMqttClient;
	}

	public void createMqttClient(){
		String broker = PropertyLoader.getInstance().getPropValues("broker");
		String clientId = PropertyLoader.getInstance().getPropValues("clientId");
		String userName = PropertyLoader.getInstance().getPropValues("userName");
		String password = PropertyLoader.getInstance().getPropValues("password");

		try {
			client = new MqttClient(broker, clientId, persistence);
			MqttConnectOptions connOpts = new MqttConnectOptions();
			connOpts.setUserName(userName);
			connOpts.setPassword(password.toCharArray());
			connOpts.setCleanSession(true);
			System.out.println("Connecting to broker: " + broker);
			client.setCallback(new MessageReceiver());
			client.connect(connOpts);
			System.out.println("Connected");
		} catch (MqttException me) {
			System.out.println("reason " + me.getReasonCode());
			System.out.println("msg " + me.getMessage());
			System.out.println("loc " + me.getLocalizedMessage());
			System.out.println("cause " + me.getCause());
			System.out.println("excep " + me);
			me.printStackTrace();
		}
	}

	public MqttClient getClient() {
		if (client == null)
			createMqttClient();

		if (client.isConnected()) {
			client = null;
			createMqttClient();
		}

		return client;
	}

}

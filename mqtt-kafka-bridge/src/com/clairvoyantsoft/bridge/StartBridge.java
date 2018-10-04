package com.clairvoyantsoft.bridge;

import org.eclipse.paho.client.mqttv3.MqttException;

import com.clairvoyantsoft.mqtt.PahoMqttClient;
import com.clairvoyantsoft.propertyloader.PropertyLoader;

public class StartBridge {

	public static void main(String[] args) {
		String topicList = PropertyLoader.getInstance().getPropValues("topicList");
		
		for (String topic : topicList.split("\\,")) {
			try {
				PahoMqttClient.getInstance().getClient().subscribe(topic);
				System.out.println("subscribed to "+topic);
			} catch (MqttException e) {
				e.printStackTrace();
			}
		}
	}
	
}

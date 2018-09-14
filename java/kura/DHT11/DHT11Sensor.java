package kura.DHT11;

import java.util.Date;

import org.eclipse.kura.cloud.CloudClient;
import org.eclipse.kura.cloud.CloudClientListener;
import org.eclipse.kura.cloud.CloudService;
import org.eclipse.kura.message.KuraPayload;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.ComponentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DHT11Sensor implements CloudClientListener {

	/*
	 * private static final String topic = "DHT11MeetUp"; private static final int
	 * qos = 0; private static final boolean retain = false;
	 */
	DHT11 d;

	private CloudService m_cloudService;
	private CloudClient m_cloudClient;

	private static final Logger s_logger = LoggerFactory.getLogger(DHT11Sensor.class);
	private static final String APP_ID = "DHT11Sensor";

	public DHT11Sensor() {
		super();
	}

	public void setCloudService(CloudService cloudService) {
		this.m_cloudService = cloudService;
	}

	public void unsetCloudService(CloudService cloudService) {
		this.m_cloudService = null;
	}

	protected void activate(ComponentContext componentContext) {

		// get the mqtt client for this application
		try {

			// Acquire a Cloud Application Client for this Application
			s_logger.info("Getting CloudClient for {}...", APP_ID);

			// Don't subscribe because these are handled by the default
			// subscriptions and we don't want to get messages twice

			this.m_cloudClient = this.m_cloudService.newCloudClient(APP_ID);
			this.m_cloudClient.addCloudClientListener(this);

			d = new DHT11();
			doPublish();
		} catch (Exception e) {
			s_logger.error("Error during component activation", e);
			throw new ComponentException(e);
		}

		s_logger.info("Bundle " + APP_ID + " has started!");

	}
	public void updated() {
		s_logger.info("in update");
		doPublish();

	}
	protected void deactivate(ComponentContext componentContext) {

		// Releasing the CloudApplicationClient
		s_logger.info("Releasing CloudApplicationClient for {}...", APP_ID);
		this.m_cloudClient.release();
		s_logger.debug("Deactivating DHT11... Done.");
		s_logger.info("Bundle " + APP_ID + " has stopped!");

	}

	private void doPublish() {
		String result;
		String temp = "NA";
		String humi = "NA";
		// fetch the publishing configuration from the publishing properties
		String topic = "DHT11MeetUp";
		Integer qos = 0;
		Boolean retain = false;

		// Allocate a new payload
		KuraPayload payload = new KuraPayload();

		// Timestamp the message
		payload.setTimestamp(new Date());

		for (int i = 0; i < 30; i++) {

			result = d.updateTemperature();
			s_logger.info("result is ::- " + result);
			if (!result.equals("xxxx")) {
				String res[] = result.split(":");
				temp = res[0];
				humi = res[1];
				s_logger.info("Published TemperatureMeetup is: {} HumidityMeetup is : {}", temp, humi);
				
				// Add the temperature as a metric to the payload
				payload.addMetric("TemperatureMeetup", temp);
				payload.addMetric("HumidityMeetup", humi);
			} else {
				s_logger.info("Data not good");
			}

			try {
				
				 // Publish the message
		            this.m_cloudClient.publish(topic, payload, qos, retain);
		            s_logger.info("Published to {} message: {}", topic, payload);
		        
				Thread.sleep(4000);
			} catch (Exception e) {
				// TODO: handle exception
	            s_logger.error("Cannot publish topic: " + topic, e);

				s_logger.info("sleep exception is ::- ", e);

			}
		}
	}

	@Override
	public void onControlMessageArrived(String deviceId, String appTopic, KuraPayload msg, int qos, boolean retain) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onMessageArrived(String deviceId, String appTopic, KuraPayload msg, int qos, boolean retain) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onConnectionLost() {
		// TODO Auto-generated method stub

	}

	@Override
	public void onConnectionEstablished() {
		// TODO Auto-generated method stub

	}

	@Override
	public void onMessageConfirmed(int messageId, String appTopic) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onMessagePublished(int messageId, String appTopic) {
		// TODO Auto-generated method stub

	}

}

package org.eclipse.kura.DHT11;

import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.eclipse.kura.cloud.CloudClient;
import org.eclipse.kura.cloud.CloudClientListener;
import org.eclipse.kura.cloud.CloudService;
import org.eclipse.kura.configuration.ConfigurableComponent;
import org.eclipse.kura.message.KuraPayload;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.ComponentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DHT11Sensor implements ConfigurableComponent, CloudClientListener {

	private static final Logger s_logger = LoggerFactory.getLogger(DHT11Sensor.class);

	private CloudService m_cloudService;
	private CloudClient m_cloudClient;

	private ScheduledExecutorService m_worker;
	private ScheduledFuture<?> m_handle;

	private DHT11DataReader dht11DataReader;
	private double temperature;
	private double humidity;

	private Map<String, Object> m_properties;
	private static final String APP_ID = "DHT11Sensor";

	// Publishing Property Names on Kura Web UI
	private static final String PROP_TEMPERATURE_METRIC = "Temperature";
	private static final String PROP_HUMIDITY_METRIC = "Humidity";
	private static final String PROP_TOPIC_NAME = "Topic";

	public DHT11Sensor() {
		super();
		m_worker = Executors.newSingleThreadScheduledExecutor();
	}

	public void setCloudService(CloudService cloudService) {
		m_cloudService = cloudService;
	}

	public void unsetCloudService(CloudService cloudService) {
		m_cloudService = null;
	}

	protected void activate(ComponentContext componentContext, Map<String, Object> properties) {

		this.m_properties = properties;
		for (Entry<String, Object> property : properties.entrySet()) {
			s_logger.info("Update - {}: {}", property.getKey(), property.getValue());
		}

		// get the mqtt client for this application
		try {
			// Acquire a Cloud Application Client for this Application
			s_logger.info("Getting CloudClient for {}...", APP_ID);
			m_cloudClient = m_cloudService.newCloudClient(APP_ID);
			m_cloudClient.addCloudClientListener(this);
			
			/*
			 * Don't subscribe because these are handled by the default subscriptions and we
			 * don't want to get messages twice
			 */
			dht11DataReader = new DHT11DataReader();
			doUpdate(false);
		} catch (Exception e) {
			s_logger.error("Error during component activation", e);
			throw new ComponentException(e);
		}

		s_logger.info("Bundle " + APP_ID + " has started!");

	}

	protected void deactivate(ComponentContext componentContext) {
		m_worker.shutdown();

		// Releasing the CloudApplicationClient
		s_logger.info("Releasing CloudApplicationClient for {}...", APP_ID);
		m_cloudClient.release();

		s_logger.debug("Deactivating DHT11... Done.");
		s_logger.info("Bundle " + APP_ID + " has stopped!");

	}

	public void updated(Map<String, Object> properties) {
		s_logger.info("Updated DHT11...");

		// store the properties received
		this.m_properties = properties;
		for (Entry<String, Object> property : properties.entrySet()) {
			s_logger.info("Update - {}: {}", property.getKey(), property.getValue());
		}

		// try to kick off a new job
		dht11DataReader = new DHT11DataReader();
		doUpdate(true);
		s_logger.info("Updated DHT11... Done.");
	}

	/**
	 * Called after a new set of properties has been configured on the service
	 */
	private void doUpdate(boolean onUpdate) {
		// cancel a current worker handle if one if active
		if (m_handle != null) {
			m_handle.cancel(true);
		}

		// schedule a new worker based on the properties of the service
		int pubrate = 2;
		m_handle = m_worker.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				Thread.currentThread().setName(getClass().getSimpleName());
				doPublish();
			}
		}, 0, pubrate, TimeUnit.SECONDS);
	}

	private void doPublish() {

		// fetch the publishing configuration from the publishing properties
		String topic = (String) m_properties.get(PROP_TOPIC_NAME);
		String temperatureMetric = (String) m_properties.get(PROP_TEMPERATURE_METRIC);
		String humidityMetric = (String) m_properties.get(PROP_HUMIDITY_METRIC);
		Integer qos = 0;
		Boolean retain = false;

		// Allocate a new payload
		KuraPayload payload = new KuraPayload();
		payload.setTimestamp(new Date());// Timestamp the message

		/*
		 * calling the readTemperatureHumidity method of DHT11DataReader class to get
		 * updated temperature and humidity
		 */
		dht11DataReader.readTemperatureHumidity();
		temperature = dht11DataReader.getTemperature();
		humidity = dht11DataReader.getHumidity();

		if ((temperature != Double.MAX_VALUE) && (humidity != Double.MAX_VALUE)) {

			// Add the temperature and humidity as a metric to the payload
			payload.addMetric(temperatureMetric, temperature);
			payload.addMetric(humidityMetric, humidity);

			// Publish the message to the Cloud
			try {
				m_cloudClient.publish(topic, payload, qos, retain);
				s_logger.info("--------------------------" + "\n");
				s_logger.info("Published on topic: " + topic + "\n" + temperatureMetric + " : " + temperature + " & "
						+ humidityMetric + " : " + humidity);
				s_logger.info("--------------------------" + "\n");
				Thread.sleep(4000); // sleeping for 4 second before next reading
			} catch (Exception e) {
				s_logger.error("Cannot publish data: ", e);
			}
		}
	}

	@Override
	public void onConnectionEstablished() {
		// TODO Auto-generated method stub

	}

	@Override
	public void onConnectionLost() {
		// TODO Auto-generated method stub

	}

	@Override
	public void onControlMessageArrived(String arg0, String arg1, KuraPayload arg2, int arg3, boolean arg4) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onMessageArrived(String arg0, String arg1, KuraPayload arg2, int arg3, boolean arg4) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onMessageConfirmed(int arg0, String arg1) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onMessagePublished(int arg0, String arg1) {
		// TODO Auto-generated method stub

	}
}

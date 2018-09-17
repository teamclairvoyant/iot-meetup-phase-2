package org.eclipse.kura.DHT11;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pi4j.wiringpi.Gpio;
import com.pi4j.wiringpi.GpioUtil;
/*
 * Class for getting data from DHT11 Sensor connected to Raspberry Pi through 
 * wiringPi and Py4j libraries.
 * http://wiringpi.com/download-and-install/
 * 
 */
public class DHT11DataReader {

	// GPIO Stuff
	private final int[] dht11_dat = { 0, 0, 0, 0, 0 };
	
	// change the pin number accordingly as this is your DHT11 sensor's DATA pin connected to Raspberry Pi 3.
	private static int pin = 7;
	private static final int MAXTIMINGS = 85;

	private static final Logger s_logger = LoggerFactory.getLogger(DHT11DataReader.class);


	// Measurement Values
	private double temperature;
	private double humidity;

	// Error Handling
	private int errorCount = 0;

	/**
	 * Creates a DHT11 object.
	 */
	public DHT11DataReader() {
		if (Gpio.wiringPiSetup() == -1) {
			s_logger.error("GPIO setup failed. An error occured when creating a DHT11DataReader object.");
			s_logger.error("GPIO setup failed. Please check your wiring Pi configuraion or GPIO Setup");
			return;
		}

		GpioUtil.export(3, GpioUtil.DIRECTION_OUT);
	}

	/**
	 * Reads the temperature and humidity data from the sensor.
	 * 
	 */
	public void readTemperatureHumidity() {
		int laststate = Gpio.HIGH;
		int j = 0;
		dht11_dat[0] = dht11_dat[1] = dht11_dat[2] = dht11_dat[3] = dht11_dat[4] = 0;

		/* pull pin down for 18 milliseconds */
		Gpio.pinMode(pin, Gpio.OUTPUT);
		Gpio.digitalWrite(pin, Gpio.LOW);
		Gpio.delay(18);
		
		/* prepare to read the pin */
		Gpio.digitalWrite(pin, Gpio.HIGH);
		Gpio.pinMode(pin, Gpio.INPUT);
		
		/* detect change and read data */
		for (int i = 0; i < MAXTIMINGS; i++) {
			int counter = 0;

			while (Gpio.digitalRead(pin) == laststate) {
				counter++;
				Gpio.delayMicroseconds(1);
				if (counter == 255) {
					break;
				}
			}

			laststate = Gpio.digitalRead(pin);

			if (counter == 255) {
				break;
			}

			try {
				// Ignore first 3 transitions.
				if (i >= 4 && i % 2 == 0) {
					dht11_dat[j / 8] <<= 1; // Shove each bit into the storage bytes.

					if (counter > 30)
						dht11_dat[j / 8] |= 1;

					j++;
				}
			} catch (Exception e) {
				if (this.errorCount == 0)
					s_logger.error(
							"DHT11 Exception. An error occured when getting data from the DHT11 tempetemperature sensor. ",
							e);

				this.errorCount++;

				if (this.errorCount > 3)
					break;

				this.readTemperatureHumidity();
			}
		}

		/* check we read 40 bits (8bit x 5 ) + verify checksum in the last byte.
		print it out if data is good
		*/
		if (j >= 40 && checkParity()) {
			float h = (float) ((dht11_dat[0] << 8) + dht11_dat[1]) / 10;

			if (h > 100) {
				h = dht11_dat[0]; // for DHT11
			}

			float c = (float) (((dht11_dat[2] & 0x7F) << 8) + dht11_dat[3]) / 10;

			if (c > 125) {
				c = dht11_dat[2]; // for DHT11
			}

			if ((dht11_dat[2] & 0x80) != 0) {
				c = -c;
			}

			//final float f = c * 1.8f + 32;
			this.temperature = c;
			this.humidity = h;
		} else {
			this.temperature = Double.MAX_VALUE;
			this.humidity = Double.MAX_VALUE;
		}

	}

	/**
	 * Returns the temperature from the sensor.
	 * 
	 * @return Double - temperature as a double.
	 */
	public double getTemperature() {
		return this.temperature;
	}

	/**
	 * Returns the humidity from the sensor.
	 * 
	 * @return Double - humidity as a double.
	 */
	public double getHumidity() {
		return this.humidity;
	}

	/** 
	 * @return Boolean - True of false depending of temperature data array.
	 */
	private boolean checkParity() {
		return dht11_dat[4] == (dht11_dat[0] + dht11_dat[1] + dht11_dat[2] + dht11_dat[3] & 0xFF);
	}
}

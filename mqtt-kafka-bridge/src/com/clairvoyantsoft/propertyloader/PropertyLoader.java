package com.clairvoyantsoft.propertyloader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

	public class PropertyLoader {

	private static PropertyLoader propertyLoader = null;

	private InputStream inputStream;

	private PropertyLoader() {

	}

	public static PropertyLoader getInstance() {
		if (propertyLoader == null)
			propertyLoader = new PropertyLoader();
		return propertyLoader;
	}

	public String getPropValues(String key){
		String value = null;
		try {
			Properties prop = new Properties();
			String propFileName = "config.properties";

			inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

			if (inputStream != null) {
				prop.load(inputStream);
			} else {
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			}

			// get the property value and print it out
			value = prop.getProperty(key);

		} catch (Exception e) {
			System.out.println("Exception: " + e);
		} finally {
			// inputStream.close();
		}
		return value;
	}
}

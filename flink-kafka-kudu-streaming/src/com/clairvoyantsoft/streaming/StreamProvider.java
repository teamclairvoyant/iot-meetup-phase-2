package com.clairvoyantsoft.streaming;

import com.clairvoyantsoft.stream.Stream;
import com.clairvoyantsoft.stream.impl.HbaseStream;
import com.clairvoyantsoft.stream.impl.KuduStream;

public class StreamProvider {

	public static Stream getStream(String db) {
		if (db.equalsIgnoreCase("hbase"))
			return new HbaseStream();
		if (db.equalsIgnoreCase("kudu"))
			return new KuduStream();
		return null;
	}

}

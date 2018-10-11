package com.clairvoyantsoft.flink.Sink;

import java.io.IOException;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class HbaseSink extends RichSinkFunction<String>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 343097117567204070L;

	@Override
    public void invoke(String row) throws IOException {

        //write code to create Hbase Sink option for HbaseOutput format
    }

}

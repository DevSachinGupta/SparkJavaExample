package me.sachingupta.sparkexamples.services.streaming.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.sachingupta.sparkexamples.services.rdd.Fortune500Service;

public class LogStreamAnalysis {
	
	private JavaStreamingContext sc;
	private String filePath;
	private static final Logger log = LoggerFactory.getLogger(Fortune500Service.class);
	
	public void setFilePath(String appName, String masterUrl, String socketUrl, String port, String filePath) {
		this.filePath = filePath;
		log.info("File path" + filePath);
		
		SparkConf conf = new SparkConf();
		sc = new JavaStreamingContext(conf, Durations.minutes(1));
	}

	public void extract() {
		JavaDStream<String> stream = sc.socketTextStream("", 1111);
	}
	
	
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		if(sc != null) {
			sc.close();
		}
	}
		
}

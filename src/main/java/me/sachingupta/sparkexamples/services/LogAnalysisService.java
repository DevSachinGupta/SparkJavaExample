package me.sachingupta.sparkexamples.services;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;

public class LogAnalysisService implements Serializable {

	private static final long serialVersionUID = 1L;
	
	JavaPairRDD<String, String> files;
	
	
	public LogAnalysisService(String appName, String masterUrl, String filePath) {
		
	}
	public void extract() {
		
	}
	
	protected void finalize() {
		
	}
}

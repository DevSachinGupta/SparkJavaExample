package me.sachingupta.sparkexamples.services;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

public class WordCountService {
	
	private SparkConf conf;
	private JavaSparkContext sc;
	private JavaRDD<String> lines;
	
	public WordCountService(String appName, String masterUrl, String filePath) {
		conf = new SparkConf().setAppName(appName).setMaster(masterUrl);
		sc = new JavaSparkContext(conf);
		lines = sc.textFile(filePath);
	}
	
	public void create() {
			
	}
	
}

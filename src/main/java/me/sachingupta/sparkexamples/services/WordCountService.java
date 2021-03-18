package me.sachingupta.sparkexamples.services;

import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

import org.apache.spark.SparkConf;

public class WordCountService implements Serializable {
	
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(WordCountService.class);
	
	private SparkConf conf;
	private JavaSparkContext sc;
	private JavaRDD<String> lines;
	
	public WordCountService(String appName, String masterUrl, String filePath) {
		conf = new SparkConf().setAppName(appName).setMaster(masterUrl);
		sc = new JavaSparkContext(conf);
		lines = sc.textFile(filePath);
		
		logger.info("Lines count {}", lines.count());
	}
	
	public void count() {
		JavaRDD<String> brokenWords;
		JavaPairRDD<String, Long> wordsCount;
		
		//brokenWords = lines.flatMap(f - > f._1().split(""));
	}
	
}

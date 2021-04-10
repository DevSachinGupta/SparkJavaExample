package me.sachingupta.sparkexamples.services.rdd;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.sachingupta.sparkexamples.utils.functions.BreakWords;
import scala.Tuple2;

public class WordCountService implements Serializable {
	
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(WordCountService.class);
	
	private JavaSparkContext sc;
	private JavaRDD<String> lines;
	private String filePath;
	
	public WordCountService(String appName, String masterUrl, String filePath) {
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(masterUrl);
		sc = new JavaSparkContext(conf);
		lines = sc.textFile(filePath);
		this.filePath = filePath;
		logger.info("Lines count {}", lines.count());
	}
	
	public void count() {
		JavaRDD<String> brokenWords;
		JavaPairRDD<String, Long> wordsCount;
		
		/** 
		 *  the following code is also an example of an API called as Fluent API in Java Spark
		 *  This should be used while we are trying to form a single task
		 *  but  if required must use the different RDD's after performing  
		 *  either transformation or action operation for storing the result.
		*/
		brokenWords = lines.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
												.filter(sentence -> sentence.trim().length() > 0)
												.flatMap(new BreakWords())
												.filter(word -> word.trim().length() > 0);
		
		wordsCount = brokenWords.mapToPair(t -> new Tuple2<String, Long>(t, 1L)).reduceByKey((x, y) -> (long)x + (long)y);
		
		
		
		wordsCount.saveAsTextFile(filePath + "_output");
		
	}
	
	@Override
	protected void finalize() {
		if (sc != null)
			sc.close();
	}
}

package me.sachingupta.sparkexamples.services.sql;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogDataAnalysis {

	private SparkSession sc;
	// private String filePath;
	
	private static final Logger log = LoggerFactory.getLogger(LogDataAnalysis.class);
	
	public LogDataAnalysis(String appName, String masterUrl, String filePath) {
		// this.filePath = filePath;
		log.info("Called Log Data Analysis of sql");
	}

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		if(sc != null) {
			sc.close();
		}
	}
	
	
}

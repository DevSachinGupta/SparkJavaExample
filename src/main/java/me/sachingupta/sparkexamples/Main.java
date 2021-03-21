package me.sachingupta.sparkexamples;

import org.apache.log4j.Logger;

import me.sachingupta.sparkexamples.services.rdd.Fortune500Service;
import me.sachingupta.sparkexamples.services.rdd.LogAnalysisService;
import me.sachingupta.sparkexamples.services.rdd.WordCountService;

public class Main {
	
	private static Logger logger = Logger.getLogger(Main.class);

	public Main() {
		logger.info("Inside Main constructor");
	}
	
	public static void main(String... args) {
	
		logger.info("Inside Main");
		
		try {
			
			//Path p =  
			
			switch (args[0]) {
				case "LogAnalysis":
					new LogAnalysisService(args[1], args[2], args[3]).extract();
					break;
				case "CountWords":
					new WordCountService(args[1], args[2], args[3]).count();
					break;
				case "Fortune500":
					new Fortune500Service(args[1], args[2], args[3]).extract();
					break;
				default:
					logger.info("Please provide a valid action to perform");
					break;
			}
			
		} catch (ArrayIndexOutOfBoundsException e) {
			logger.error("Exception while accessing data from the args ", e);
		} catch (Exception e) {
			logger.error("Exception while getting the file path ", e);
		}
		
	}
}

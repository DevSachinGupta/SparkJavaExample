package me.sachingupta.sparkexamples.services.rdd;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.sachingupta.sparkexamples.modals.Fortune500;
import me.sachingupta.sparkexamples.utils.PatternsUtility;
import me.sachingupta.sparkexamples.utils.functions.ConvertPairRDD2RDD;
import scala.Tuple2;
import scala.Tuple3;

public class Fortune500Service implements Serializable {
	
	private static final long serialVersionUID = -4468932282748274269L;
	
	private SparkConf conf;
	private JavaSparkContext sc;
	private JavaRDD<String> lines;
	private JavaPairRDD<String, String> linesWithFileName;
	
	private String filePath;
	
	private static final Logger log = LoggerFactory.getLogger(Fortune500Service.class);
	
	public Fortune500Service(String appName, String masterUrl, String filePath) {
		
		this.filePath = filePath;
		log.info("Application name is {}, path to file(s) is {}", appName, filePath);
		
		conf = new SparkConf().setAppName(appName).setMaster(masterUrl);
		sc = new JavaSparkContext(conf);
		// lines = sc.textFile(filePath);
		// log.info("total count of the lines is {}", lines.count());
		
		linesWithFileName = sc.wholeTextFiles(filePath);
		
		// log.info("total count of the lines is {}", linesWithFileName.count());
	}
	
	public void extract() {
		
		String firstLine = PatternsUtility.LINE.split(linesWithFileName.first()._2())[0]; // lines.first();
		log.info("First line of the data is {}", firstLine);
		
		// lines = lines.filter(new StringNotEquals(firstLine));
		// linesWithFileName = linesWithFileName.filter(new StringNotEqualsPairRDD(firstLine));
		
		lines = linesWithFileName.flatMap(new ConvertPairRDD2RDD());
		//lines = lines.filter(new StringNotEquals(firstLine));
		
		log.info("First line of the data after conversion is {}", lines.first());
		// log.info("total count of the lines after conversion is {}", lines.count());
		
		log.info("Now Converting data to Fortune500 objects");
		JavaRDD<Fortune500> data = lines.map(new Convert2Fortune500());
		
		log.info("First line of the data after 2nd conversion is {}", data.first());
		// log.info("total count of the lines after 2nd conversion is {}", data.count());
		
		JavaPairRDD<Integer, Fortune500> yearWiseData = data.mapToPair(d -> new Tuple2<Integer, Fortune500>(d.getYear(), d));
		JavaPairRDD<Long, Fortune500> rankWiseData = data.mapToPair(d -> new Tuple2<Long, Fortune500>(d.getRank(), d));
		
		
		JavaPairRDD<Integer, Fortune500> highestValueYear = yearWiseData.reduceByKey((x, y) -> x.getProfitPercent() > y.getProfitPercent() ? x : y);
		JavaPairRDD<Long, Fortune500> highestValueRank = rankWiseData.reduceByKey((x, y) -> x.getProfitPercent() > y.getProfitPercent() ? x : y);
		
		JavaPairRDD<Double, Fortune500> highestValueYear2 = highestValueYear.mapToPair(d -> new Tuple2<Double, Fortune500>(d._2.getProfitPercent(), d._2)).sortByKey();
		highestValueYear = highestValueYear2.mapToPair(d -> new Tuple2<Integer, Fortune500>(d._2.getYear(), d._2));
		
		JavaPairRDD<Double, Fortune500> highestValueRank2 = highestValueRank.mapToPair(d -> new Tuple2<Double, Fortune500>(d._2.getProfitPercent(), d._2)).sortByKey();
		highestValueRank = highestValueRank2.mapToPair(d -> new Tuple2<Long, Fortune500>(d._2.getRank(), d._2));
		
		log.info("Maximum earner of the year");
		highestValueYear.take(10).forEach(System.out::println);

		log.info("Maximum earned of the year");
		highestValueRank.take(10).forEach(System.out::println);
		// data is final dataframe to use for further processing...
		//data.saveAsTextFile(filePath + "_final.txt");
		
		
	}

	public static class Convert2Fortune500 implements Function<String, Fortune500> {
		private static final long serialVersionUID = 1L;
		// private Logger log = LoggerFactory.getLogger(Convert2Fortune500.class);
		
		@Override
		public Fortune500 call(String v1) throws Exception {
			String[] str = PatternsUtility.CSV.split(v1);
			
			if(str[0] == null || str[0].equalsIgnoreCase("n.a.") || str[0].equalsIgnoreCase("na") || str[0].equalsIgnoreCase("n.a") || str[0].equalsIgnoreCase("na.")) {
				str[0] = "0";
			}
			if(str[2] == null || str[2].equalsIgnoreCase("n.a.") || str[2].equalsIgnoreCase("na") || str[2].equalsIgnoreCase("n.a") || str[2].equalsIgnoreCase("na.")) {
				str[2] = "0";
			}
			if(str[3] == null || str[3].equalsIgnoreCase("n.a.") || str[3].equalsIgnoreCase("na") || str[3].equalsIgnoreCase("n.a") || str[3].equalsIgnoreCase("na.")) {
				str[3] = "0";
			}
			// log.info("Year string {}", str[4]);
			
			String str1 = str[4].replaceAll("\\D", " ").trim().replaceAll("\\s+", " ");
			// log.info("Cleaned String is '{}'  for rank '{}'", str1, str[0]);
			
			String year = (PatternsUtility.SPACE.split(str1))[2];
			// log.info("Year = {}", year);			
				
			return new Fortune500(
					Long.parseLong(str[0].trim()), 
					str[1].trim(), Double.parseDouble(str[2].trim()), 
					Double.parseDouble(str[3].trim()),  
					str[4].trim(),
					Integer.parseInt(year.trim()));
		}

	}
	
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		if (sc != null)
			sc.close();
	}
}

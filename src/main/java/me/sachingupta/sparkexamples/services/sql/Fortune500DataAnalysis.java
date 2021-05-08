package me.sachingupta.sparkexamples.services.sql;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.sachingupta.sparkexamples.utils.PatternsUtility;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

public class Fortune500DataAnalysis implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private SparkSession sc;
	private String filePath;
	
	private static final Logger log = LoggerFactory.getLogger(Fortune500DataAnalysis.class);
	
	public Fortune500DataAnalysis(String appName, String masterUrl, String filePath) {
		this.filePath = filePath;
		log.info("Called Log Data Analysis of sql");
		
		if(masterUrl.equalsIgnoreCase("yarn")) {
			sc = SparkSession.builder()
					.appName(appName)
					.getOrCreate();
		} else {
			sc = SparkSession.builder()
					.appName(appName)
					.master(masterUrl)
					.getOrCreate();
		}
		sc.udf().register("calculateProfitPercent", calculateProfitPercent, DataTypes.DoubleType);
		sc.udf().register("extractYearFromFileName", extractYearFromFileName, DataTypes.IntegerType);
		sc.udf().register("isAvailableInNConsesutiveYears", isAvailableInNConsesutiveYears, DataTypes.BooleanType);
	}

	public void extract() {
		// new Metadata() is resultiong in null pointer exception so using the Metadata.empty()
		StructField[] fields = new StructField[]{
				new StructField("rank", DataTypes.IntegerType, true, Metadata.empty()),
				new StructField("company", DataTypes.StringType, true, Metadata.empty()),
				new StructField("revenue ($ millions)", DataTypes.DoubleType, true, Metadata.empty()),
				new StructField("profit ($ millions)", DataTypes.DoubleType , true, Metadata.empty()),
		};
		StructType schema = new StructType(fields);
		
		Dataset<Row> data = sc.read()
							.option("header", true)
							.schema(schema)
							.csv(this.filePath);
		
		data = data.withColumnRenamed("revenue ($ millions)", "revenue")
				.withColumnRenamed("profit ($ millions)", "profit");
		
		data = data.withColumn("profitPercent", callUDF("calculateProfitPercent", col("revenue"), col("profit")));
		
		data = data.withColumn("filename", org.apache.spark.sql.functions.input_file_name());

		data = data.withColumn("year", callUDF("extractYearFromFileName", col("filename")));
		
		/*data.printSchema();
		
		data.show(false);
		*/
		data = data.drop(col("filename")).na().fill(0);
		
		Dataset<Row>  data2 = data.where("rank < 11");
		
		Dataset<Row> data3 = data.groupBy(col("company")).agg(org.apache.spark.sql.functions.collect_list(col("year")).alias("years"));
		data3 = data3.withColumn("isAvailableIn5ConsecutiveYears", callUDF("isAvailableInNConsesutiveYears", col("years"), lit(new Integer(2))));
		data3 = data3.select(col("company")).where("isAvailableIn5ConsecutiveYears == true");
		data3.show();
		
		Dataset<Row> allPivotData = data2.groupBy(col("company")).pivot(col("year")).agg(
				max(col("rank")).alias("rank"),
				max(col("revenue")).alias("revenue"),
				max(col("profit")).alias("profit"),
				max(col("profitPercent")).alias("profitPercent"));
		
		/*Dataset<Row> rankData = data2.groupBy(col("company")).pivot(col("year")).agg(max(col("rank")).alias("rank"));
		
		Dataset<Row> revenueData = data2.groupBy(col("company")).pivot(col("year")).agg(max(col("revenue")).alias("revenue"));
		
		Dataset<Row> profitData = data2.groupBy(col("company")).pivot(col("year")).agg(max(col("profit")).alias("profit"));
		
		Dataset<Row> ppData = data2.groupBy(col("company")).pivot(col("year")).agg(max(col("profitPercent")).alias("profitPercent"));
		*/
		
		List<String> colNames = Arrays.asList(allPivotData.columns());
		
		//dataAsPivot = dataAsPivot.groupBy(col("rank"), col("company"), col("revenue"), col("profit")).pivot(col("year")).agg(max(col("profitPercent")).alias("max profitted(%)") ,
        //        min(col("score").cast(DataTypes.IntegerType)).alias("min profitted(%)"));
		
		
		Dataset<Row> rankData = allPivotData.select("company", colNames.stream().filter(x -> x.endsWith("rank")).toArray(String[] :: new ));
		rankData = rankData.na().fill(0L);
		rankData.show();
		Dataset<Row> profitPercentData = allPivotData.select("company", colNames.stream().filter(x -> x.endsWith("profitPercent")).toArray(String[] :: new ));
		profitPercentData = profitPercentData.na().fill(0.0f);
		profitPercentData.show();
		
		data3.write().format("csv").save("Companies occured more than 5 times.csv");
		rankData.write().format("csv").save("Companies Rank data of top 10.csv");
		profitPercentData.write().format("csv").save("Companies Profit data of top 10.csv");
		
	}
	
	private UDF2<Double, Double, Double> calculateProfitPercent = new UDF2<Double, Double, Double>() {

		private static final long serialVersionUID = 1L;

		@Override
		public Double call(Double arg0, Double arg1) throws Exception {
			if (arg0 == null || arg1 == null)
				return (double) 0.0f;
			return (arg1 / arg0)*100;
		}
		
	};
	
	private UDF1<String, Integer> extractYearFromFileName = new UDF1<String, Integer>() {

		private static final long serialVersionUID = 1L;

		@Override
		public Integer call(String arg0) throws Exception {
			String str1 = arg0.replaceAll("\\D", " ").trim().replaceAll("\\s+", " ");
			
			int index = (PatternsUtility.SPACE.split(str1)).length -1;
			
			String year = (PatternsUtility.SPACE.split(str1))[index];
			
			return new Integer(year);
		}
		
	};
	
	private UDF2<WrappedArray<Integer>, Integer, Boolean> isAvailableInNConsesutiveYears = new UDF2<WrappedArray<Integer>, Integer, Boolean>(){
		private static final long serialVersionUID = 1L;

		@Override
		public Boolean call(WrappedArray<Integer> arg, Integer arg1) throws Exception {
			
			List<Integer> arg0 = new ArrayList<Integer>(JavaConverters.asJavaCollectionConverter(arg).asJavaCollection());
			Collections.sort(arg0);
			
			System.out.println(arg0 + " : " + arg0.size() + " : " + arg0.isEmpty());
			if(!arg0.isEmpty() && (arg0.size() >= arg1.intValue()) ) {
				// logic to find if come in five consecutive years
				int count = 1, consecutiveCount = 0;
				
				for (int i = 1; i < arg0.size(); i++) {
					if(arg0.get(i-1) == (arg0.get(i) - count)) {
						System.out.println("Inside if");
						consecutiveCount = consecutiveCount + 1;
						count = count + 1;
					} else {
						System.out.println("inside else");
						count = 1;
						consecutiveCount = 0;
					}
				}
				
				if(consecutiveCount >= (arg1.intValue() - 1)) 
					return true;
				
			}
			
			return false;
		}

	};
	
}

package me.sachingupta.sparkexamples.services.sql;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.column;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.collect_list;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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

public class Fortune500DataAnalysis implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private SparkSession sc;
	private String filePath;
	
	private static final Logger log = LoggerFactory.getLogger(Fortune500DataAnalysis.class);
	
	public Fortune500DataAnalysis(String appName, String masterUrl, String filePath) {
		this.filePath = filePath;
		log.info("Called Log Data Analysis of sql");
		
		sc = SparkSession.builder()
				.appName(appName)
				.master(masterUrl)
				.getOrCreate();
		sc.udf().register("calculateProfitPercent", calculateProfitPercent, DataTypes.DoubleType);
		sc.udf().register("extractYearFromFileName", extractYearFromFileName, DataTypes.IntegerType);
	}

	public void extract() {
		
		StructField[] fields = new StructField[]{
				new StructField("rank", DataTypes.IntegerType, true, new Metadata()),
				new StructField("company", DataTypes.StringType, true, new Metadata()),
				new StructField("revenue", DataTypes.DoubleType, true, new Metadata()),
				new StructField("profit", DataTypes.DoubleType , true, new Metadata()),
		};
		StructType schema = new StructType(fields);
		
		Dataset<Row> data = sc.read()
							.option("header", true)
							//.schema(schema)
							.csv(this.filePath);
		
		data = data.withColumnRenamed("revenue ($ millions)", "revenue")
				.withColumnRenamed("profit ($ millions)", "profit");
		
		data = data.withColumn("profitPercent", callUDF("calculateProfitPercent", col("revenue").cast(DataTypes.DoubleType), col("profit").cast(DataTypes.DoubleType)));
		
		data = data.withColumn("filename", org.apache.spark.sql.functions.input_file_name());
		
		data = data.withColumn("year", callUDF("extractYearFromFileName", col("filename")));
		
		/*data.printSchema();
		
		data.show(false);
		*/
		Dataset<Row> data2 = data.drop(col("filename")).na().fill(0);
		
		Dataset<Row> allPivotData = data2.groupBy(col("company")).pivot(col("year")).agg(
				max(col("rank").cast(DataTypes.LongType)).alias("rank"),
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
			String year = (PatternsUtility.SPACE.split(str1))[2];
			
			return new Integer(year);
		}
		
	}; 
	
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		if(sc != null) {
			sc.close();
		}
	}
	
}

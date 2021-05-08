package me.sachingupta.sparkexamples.services.sql;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.dense_rank;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.md5;
import static org.apache.spark.sql.functions.sha2;
import static org.apache.spark.sql.functions.sum;

import java.io.Serializable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;

import scala.collection.immutable.Set.Set2;

public class ECommerceDataAnalysis implements Serializable {
	private static final long serialVersionUID = 1L;

	private SparkSession sc;
	private String filePath;
	
	private static Logger log = LogManager.getLogger(ECommerceDataAnalysis.class);
	
	public ECommerceDataAnalysis(String appName, String masterUrl, String filePath) {
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
		sc.udf().register("getHashedValue", getHashedValue, DataTypes.StringType);
	}

	public void extract() {
		Dataset<Row> productsData = sc.read().parquet(filePath + "/products"); //  _parquet
		Dataset<Row> sellersData = sc.read().parquet(filePath + "/sellers"); // _parquet
		Dataset<Row> salesData = sc.read().parquet(filePath + "/sales").sample(0.3);
		
		System.out.println("No. of products available are " + productsData.count());
		System.out.println("No. of sellers  available are " + sellersData.count());
		System.out.println("No. of sold data available are " + salesData.count());

		System.out.println("No. of Distinct products sold per day");
		salesData.groupBy(col("date")).agg(countDistinct("product_id").alias("count")).orderBy(col("count").desc()).show();
		
		// Dataset<Row> data = salesData.join(productsData, salesData.col("product_id").equalTo(productsData.col("product_id")))
		// 							 .join(sellersData, salesData.col("seller_id").equalTo(sellersData.col("seller_id")));
		Dataset<Row> data = salesData.join(productsData, "product_id").join(sellersData, "seller_id");
		
		//data.groupBy(col("date")).agg(countDistinct(col("product_id")).alias("count")).show();
		
		data = data.withColumn("earnedMoney", col("num_pieces_sold").multiply(col("price")));
		
		data.agg(avg(col("earnedMoney")).alias("Average money Earned")).show();
			
		data = data.withColumn("contributionPerOrder", col("num_pieces_sold").divide(col("daily_target")));
		
		data.groupBy(col("seller_id"), col("date")).agg(avg("contributionPerOrder").cast("Decimal(10, 10)").alias("AverageContributionPerOrder")).show(false);
		
		Dataset<Row> piecesSold = data.groupBy(col("product_id"), col("seller_id")).agg(sum(col("num_pieces_sold")).alias("totalPiecesSold"));
		
		WindowSpec window_desc = Window.partitionBy(col("product_id")).orderBy(col("totalPiecesSold").desc());
		WindowSpec window_asc = Window.partitionBy(col("product_id")).orderBy(col("totalPiecesSold").asc());
		
		piecesSold = piecesSold.withColumn("rank_asc", dense_rank().over(window_asc))
								.withColumn("rank_desc", dense_rank().over(window_desc));
		
		Dataset<Row> singleSelller = piecesSold.where(col("rank_asc").equalTo(col("rank_desc")))
												.select(col("product_id"), col("seller-id"), lit("Only seller or multiple sellers with the same results").alias("type"));
		
		Dataset<Row> multipleSelller = piecesSold.where(col("rank_desc").equalTo(2)).select(col("product_id"), col("seller-id"), lit("Second top seller").alias("tyoe"));
		
		Dataset<Row> leastSelller = piecesSold.where(col("rank_asc").equalTo(1)).select(col("product_id"), col("seller-id"), lit("Second top seller").alias("tyoe"))
												.join(singleSelller, new Set2<String>("product_id", "seller_id").toSeq(), "left_anti")
												.join(multipleSelller, new Set2<String>("seller_id", "product_id").toSeq(),"left_anti");
		
		leastSelller.union(multipleSelller).union(singleSelller).show();
		
		data = data.withColumn("hashed_bill", callUDF("getHashedValue", col("order_id").cast(DataTypes.LongType), col("bill_raw_text")));
		
		data.groupBy(col("hashed_bill")).agg(count(col("hashed_bill")).alias("count")).where(col("count").gt(1)).show(); 
		
		//data.show(false);
	}
	
	private UDF2<Long, String, String> getHashedValue = new UDF2<Long, String, String>() {
		private static final long serialVersionUID = 1L;

		@Override
		public String call(Long arg0, String arg1) throws Exception {
			
			String hash = null;
			
			if(arg0 % 2  == 0) {
				// even id
				long count =  arg1.chars().filter(ch -> ch == 'A').count();
				hash = arg1;
				for(int i = 0; i < count; i++) {
					hash = md5(lit(hash)).toString();
				}
			} else {
				hash = sha2(lit(arg1), 256).toString();
			}
			
			return hash;
		}
	};
	
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		if(sc != null)
			sc.close();
	}
	
}

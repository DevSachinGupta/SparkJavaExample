package me.sachingupta.sparkexamples.services.streaming.rdd;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.sachingupta.sparkexamples.services.rdd.Fortune500Service;
import scala.Tuple2;

public class ViewingFiguresAnalysis {
	private JavaStreamingContext sc;
	// private String filePath;
	private static final Logger log = LoggerFactory.getLogger(Fortune500Service.class);
	private Collection<String> topics;
	private Map<String, Object> kafkaParams;
	
	public void setFilePath(String appName, String masterUrl, String bootstrapServers, String[] topics) {
		// this.filePath = filePath;
		// log.info("File path" + filePath);
		
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(masterUrl);
		sc = new JavaStreamingContext(conf, Durations.minutes(1));
		
		this.topics = Arrays.asList(topics);
		
		kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092" + "," + bootstrapServers);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "ViewingFiguresAnalysis");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
	}

	public void extract() {
		 JavaInputDStream<ConsumerRecord<Object, Object>> stream = KafkaUtils.createDirectStream(sc, LocationStrategies.PreferConsistent(), 
				 																	ConsumerStrategies.Subscribe(topics, kafkaParams));
		 
		 // TODO: Use stream object and do the code as used in RDD programming or just like streaming
		 
		 JavaPairDStream<Object, Object> result = stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
	}
	
	
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		if(sc != null) {
			sc.close();
		}
	}
}

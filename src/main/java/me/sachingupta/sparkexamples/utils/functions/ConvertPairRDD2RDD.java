package me.sachingupta.sparkexamples.utils.functions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.sachingupta.sparkexamples.utils.PatternsUtility;
import scala.Tuple2;

public class ConvertPairRDD2RDD implements FlatMapFunction<Tuple2<String, String>, String> {

	private static final long serialVersionUID = 632576049691345761L;
	
	private static final Logger logger = LoggerFactory.getLogger(ConvertPairRDD2RDD.class);

	@Override
	public Iterator<String> call(Tuple2<String, String> t) throws Exception {
		
		String fileName = t._1();
		String content = t._2();
		
		String[] contents = PatternsUtility.LINE.split(content);
		
		List<String> lines = new ArrayList<String>(contents.length);
		
		int index = 0;
		
		for (String line: contents) {
			if(index == 0) {
				index ++;
				continue;
			}
			// logger.info("Line content is {}", line);
			lines.add(line +  ","  + fileName);
		}
		
		return lines.iterator();
	}

}

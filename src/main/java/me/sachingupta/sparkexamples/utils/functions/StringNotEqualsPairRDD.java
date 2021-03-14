package me.sachingupta.sparkexamples.utils.functions;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class StringNotEqualsPairRDD implements Function<Tuple2<String, String>, Boolean> {

	private static final long serialVersionUID = 986552807584954994L;
	private String matchesTo;
	
	public StringNotEqualsPairRDD(String matchesTo) {
		this.matchesTo = matchesTo;
	}
	
	@Override
	public Boolean call(Tuple2<String, String> v1) throws Exception {
		String content = v1._2();
		
		return !(content.equals(matchesTo));
	}

}

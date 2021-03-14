package me.sachingupta.sparkexamples.utils.functions;

import org.apache.spark.api.java.function.Function;

public class StringLengthFunction implements Function<String, Integer> {

	private static final long serialVersionUID = -2035584451660841775L;

	@Override
	public Integer call(String v1) throws Exception {
		return v1.length();
	}

}

package me.sachingupta.sparkexamples.utils.functions;

import org.apache.spark.api.java.function.Function;

public class StringNotEquals implements Function<String, Boolean> {

	
	private static final long serialVersionUID = 2864046127708495791L;
	private String matchTo;
	
	public StringNotEquals(String matchTo) {
		this.matchTo = matchTo;
	}
	@Override
	public Boolean call(String v1) throws Exception {
		return !(v1.equals(matchTo));
	}

}

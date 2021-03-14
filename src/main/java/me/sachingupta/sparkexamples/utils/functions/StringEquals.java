package me.sachingupta.sparkexamples.utils.functions;

import org.apache.spark.api.java.function.Function;

public class StringEquals implements Function<String, Boolean> {

	private static final long serialVersionUID = 3179616194284339490L;
	private String matchTo;
	
	public StringEquals(String matchTo) {
		this.matchTo = matchTo;
	}
	
	@Override
	public Boolean call(String v1) throws Exception {
		return v1.equals(matchTo);
	}

}

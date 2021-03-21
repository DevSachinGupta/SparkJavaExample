package me.sachingupta.sparkexamples.utils.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import me.sachingupta.sparkexamples.utils.PatternsUtility;

public class BreakWords implements FlatMapFunction<String, String> {

	private static final long serialVersionUID = 1L;

	@Override
	public Iterator<String> call(String t) throws Exception {
		String[] str = PatternsUtility.SPACE.split(t);
		List<String> words = new ArrayList<>();
		
		for(String string: str) {
			words.addAll(Arrays.asList(PatternsUtility.COMMA.split(string)));
		}
		
		return words.iterator();
	}

	

}

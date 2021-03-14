package me.sachingupta.sparkexamples.utils;

import java.util.regex.Pattern;

public class PatternsUtility {
	public static Pattern SPACE = Pattern.compile(" ");
	public static Pattern SPACES = Pattern.compile("[\\s]+");
	public static Pattern COMMA = Pattern.compile(",");
	public static Pattern YEAR = Pattern.compile("[0-9]{4}");
	public static Pattern LINE = Pattern.compile("[\\r\\n]+");
	public static Pattern CSV = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
	// public static Pattern COMMAS = Pattern.compile("\\\"([^\\\\\\\"]*)\\\"|(?<=,|^)([^,]*)(?=,|$)"); // Pattern.compile("(?:(?<=\")([^\"]*)(?=\"))|(?<=,|^)([^,]*)(?=,|$)");
}

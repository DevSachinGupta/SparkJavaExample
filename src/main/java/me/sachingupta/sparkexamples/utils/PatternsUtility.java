package me.sachingupta.sparkexamples.utils;

import java.util.regex.Pattern;

public class PatternsUtility {
	public static Pattern SPACE		  = Pattern.compile(" ");
	public static Pattern SPACES 	  = Pattern.compile("[\\s]+");
	public static Pattern COMMA 	  = Pattern.compile(",");
	public static Pattern YEAR 		  = Pattern.compile("[0-9]{4}");
	public static Pattern LINE 		  = Pattern.compile("[\\r\\n]+");
	public static Pattern CSV 		  = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
	// public static Pattern COMMAS = Pattern.compile("\\\"([^\\\\\\\"]*)\\\"|(?<=,|^)([^,]*)(?=,|$)"); // Pattern.compile("(?:(?<=\")([^\"]*)(?=\"))|(?<=,|^)([^,]*)(?=,|$)");
	public static Pattern EMAIL 	  = Pattern.compile("^([a-zA-Z0-9._%-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,6})*$");
	public static Pattern PASSWORD    = Pattern.compile("^(?=.*[a-z])(?=.*[A-Z])(?=.*\\d)(?=.*[$@$!%*?&])[A-Za-z\\d$@$!%*?&]{8,}");
	public static Pattern URL 		  = Pattern.compile("^(https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6})([\\/\\w \\.-]*)*\\/?$");
	public static Pattern HTMLTAGS    = Pattern.compile("^<([a-z]+)([^<]+)*(?:>(.*)<\\/\\1>|\\s+\\/>)$");
	public static Pattern IPADDRESS	  = Pattern.compile("^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$");
	public static Pattern COMMANLOG   = Pattern.compile(" ");
	public static Pattern COMBINEDLOG = Pattern.compile(" ");
	public static Pattern EXTENDEDLOG = Pattern.compile(" ");
	// public static Pattern EMAI = Pattern.compile(" ");
}

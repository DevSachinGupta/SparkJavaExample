package me.sachingupta.sparkexamples;

import me.sachingupta.sparkexamples.services.Fortune500Service;

public class Main {

	public Main() {
		System.out.println("Inside Main constructor");
	}
	
	public static void main(String... args) {
	
		System.out.println("Inside Main");
		
		args = new String[3];
		
		args[0] = "Fortune500Test";
		args[1] = "local";
		args[2] = "file:////home/sachin/eclipse-workspace/datasets/fortune500/";
		try {
			// args[2] = Main.class.getClass().getResource("/fortune500/").getPath();
		} catch (Exception e) {
			System.out.println("Exception while getting the file path");
			e.printStackTrace();
		}
		
		Fortune500Service f = new Fortune500Service(args[0], args[1], args[2]);
		f.extract();
	}
}

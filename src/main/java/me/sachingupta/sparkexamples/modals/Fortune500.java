package me.sachingupta.sparkexamples.modals;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Fortune500 implements Serializable {

	private static final long serialVersionUID = 376216386115744790L;
	
	private long rank;
	private String companyName;
	private double revenue;
	private double profit;
	private String fileName;
	private int year;
	
}

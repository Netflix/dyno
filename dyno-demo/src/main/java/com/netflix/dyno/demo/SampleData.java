package com.netflix.dyno.demo;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static com.netflix.dyno.demo.DemoConfig.*;

public class SampleData {

	private final List<String> keys = new ArrayList<String>();
	private final List<String> values = new ArrayList<String>();
	
	private final Random kRandom = new Random();
	private final Random vRandom = new Random();
	
	private static final SampleData Instance = new SampleData();
	
	public static SampleData getInstance() {
		return Instance;
	}
	
	private SampleData() {
		
		for (int i=0; i<NumKeys.get(); i++) {
			keys.add(String.valueOf(i));
		}

		for (int i=0; i<NumValues.get(); i++) {
			values.add(constructRandomValue());
		}
	}
	
	public String getRandomValue() {
		int randomValueIndex = vRandom.nextInt(NumValues.get());
		return values.get(randomValueIndex);
	}
	
	public String getRandomKey() {
		int randomKeyIndex = kRandom.nextInt(NumKeys.get());
		return keys.get(randomKeyIndex);
	}
	
	private String constructRandomValue() {
		
		int requriredLength = DemoConfig.DataSize.get();  /// bytes ... note that each char is 2 bytes

		String s = UUID.randomUUID().toString(); 
		int sLength = s.length();
		
		StringBuilder sb = new StringBuilder();
		int lengthSoFar = 0;
		
		do {
			sb.append(s);
			lengthSoFar += sLength;
		} while (lengthSoFar < requriredLength);
		
		String ss = sb.toString();

		return ss;
	}
	
	public static void main(String args[]) {
		
		SampleData sd = new SampleData();
		System.out.println(sd.constructRandomValue().getBytes().length);
	}
}

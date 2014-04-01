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
			values.add(UUID.randomUUID().toString());
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
}

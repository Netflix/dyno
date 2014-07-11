package com.netflix.dyno.demo;

import static com.netflix.dyno.demo.DemoConfig.NumKeys;
import static com.netflix.dyno.demo.DemoConfig.NumValues;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicPropertyFactory;

public class SampleData {

	private final List<String> keys = new ArrayList<String>();
	private final List<String> values = new ArrayList<String>();
	
	private final Random kRandom = new Random();
	private final Random vRandom = new Random();
	
	public static final String value1 = "dcfa7d0973834e5c9f480b65de19d684dcfa7d097383dcfa7d0973834e5c9f480b65de19d684dcfa7d097383dcfa7d0973834e5c9f480b65de19d684dcfa7d097383dcfa7d0973834e5c9f480b65de19d684dcfa7d097383";
	public static final String StaticValue = value1 + value1 + value1 + value1 + value1;

	private static final SampleData Instance = new SampleData();
	
	private static final DynamicBooleanProperty UseStaticData = DynamicPropertyFactory.getInstance().getBooleanProperty("dyno.demo.useStaticData", false);
	
	public static SampleData getInstance() {
		return Instance;
	}
	
	private SampleData() {
		
		for (int i=0; i<NumKeys.get(); i++) {
			keys.add("T"+i);
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

		if (UseStaticData.get()) {
			return StaticValue;
		}
		
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

}

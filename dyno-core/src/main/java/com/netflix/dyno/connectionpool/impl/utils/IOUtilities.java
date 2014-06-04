package com.netflix.dyno.connectionpool.impl.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class IOUtilities {

	public static List<String> readLines(File file) {

		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String line = null;
			
			List<String> lines = new ArrayList<String>();
			while ((line = reader.readLine()) != null) {
				lines.add(line);
			}
			return lines;
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
				}
			}
		}
	}

	public static String toString(InputStream in) {

		byte[] buffer = new byte[1024];
		int numRead = -1;
		StringBuilder sb = new StringBuilder();

		try  {
			while ((numRead = in.read(buffer)) != -1) {
				sb.append(new String(buffer, 0, numRead));
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return sb.toString();
	}

}

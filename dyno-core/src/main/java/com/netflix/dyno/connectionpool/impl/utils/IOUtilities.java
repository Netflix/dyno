/*******************************************************************************
 * Copyright 2011 Netflix
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
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

        try {
            while ((numRead = in.read(buffer)) != -1) {
                sb.append(new String(buffer, 0, numRead));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return sb.toString();
    }

}

/*******************************************************************************
 * Copyright 2018 Netflix
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
package com.netflix.dyno.recipes.json;

public class JsonPath {
    private static final String PATH_DELIMITER = ".";
    /* package private */ static final JsonPath ROOT_PATH = new JsonPath(".");
    private final StringBuilder pathBuilder;

    public JsonPath() {
        pathBuilder = new StringBuilder();
    }

    public JsonPath(String path) {
        pathBuilder = new StringBuilder().append(path);
    }

    public JsonPath appendSubKey(String subKey) {
        if (!pathBuilder.equals(ROOT_PATH.pathBuilder)) {
            pathBuilder.append(PATH_DELIMITER).append(subKey);
        } else {
            pathBuilder.append(subKey);
        }
        return this;
    }

    public JsonPath atIndex(int index) {
        pathBuilder.append('[').append(index).append(']');
        return this;
    }

    public String toString() {
        return pathBuilder.toString();
    }
}
